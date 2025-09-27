#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
kfile_copy.py — копирует файл в Kafka (пока без SSL/ACL):
  - построчно отправляет файл в топик с P партициями
  - запускает C потребителей в одной группе параллельно
  - каждый потребитель записывает свои партиции в partition{N}_{filename}

Пример:
  python3 kfile_copy_nossl.py -b localhost:9092 -f example.txt -p 2 -c 2

Требует: confluent-kafka
  pip install confluent-kafka
"""

import argparse
import os
import sys
import time
import multiprocessing as mp
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


def create_topic_if_not_exists(broker, topic, num_partitions, replication_factor=1, timeout=30):
    adm = AdminClient({'bootstrap.servers': broker})
    new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
    fs = adm.create_topics([new_topic])
    try:
        f = fs.get(topic)
        if f is None:
            return
        f.result(timeout=timeout)
        print(f"Topic '{topic}' created with {num_partitions} partitions.")
    except Exception as e:
        if 'TopicAlreadyExists' in str(e) or 'already exists' in str(e):
            print(f"Topic '{topic}' already exists — используем существующий.")
        else:
            raise

def build_conf(broker, group_id=None):
    conf = {
        'bootstrap.servers': broker,
        'enable.auto.commit': False,
    }
    if group_id:
        conf['group.id'] = group_id
    return conf

def produce_file_to_topic(conf, topic, filename, num_partitions):
    p = Producer(conf)
    print(f"Producing '{filename}' -> topic '{topic}' ({num_partitions} partitions)")
    count = 0
    try:
        with open(filename, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                value = line.rstrip('\n')
                partition = count % num_partitions  
                delivered = {'ok': False, 'err': None}
                def cb(err, _msg):
                    if err is None:
                        delivered['ok'] = True
                    else:
                        delivered['err'] = err

                try:
                    p.produce(topic, value=value.encode('utf-8'), partition=partition, callback=cb)
                except BufferError:
                    p.flush()
                    p.produce(topic, value=value.encode('utf-8'), partition=partition, callback=cb)

                t0 = time.time()
                while not delivered['ok'] and delivered['err'] is None and (time.time() - t0) < 5.0:
                    p.poll(0.1)

                if delivered['err'] is not None:
                    print("Delivery error:", delivered['err'], file=sys.stderr)

                count += 1

        p.flush(10.0)
        print(f"Produced {count} messages.")
    finally:
        try:
            p.flush(5.0)
        except Exception:
            pass

def consumer_worker(broker, topic, group_id, filename_base, poll_interval=1.0):
    conf = build_conf(broker, group_id=group_id)
    conf.update({'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    c = Consumer(conf)

    file_handles = {}   # partition -> file handle
    end_offsets = {}    # partition -> hi (end watermark)
    reached = {}        # partition -> bool

    def on_assign(consumer, partitions):
        # при назначении партиций — открываем файлы и фиксируем hi (end offset)
        to_assign = []
        for tp in partitions:
            pnum = tp.partition
            try:
                lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, pnum), 10)
            except Exception:
                lo, hi = 0, 0
            end_offsets[pnum] = hi
            out_name = f"partition{pnum}_{filename_base}"
            fh = open(out_name, 'w', encoding='utf-8', errors='replace')
            file_handles[pnum] = fh
            reached[pnum] = (hi == 0)
            print(f"[pid {mp.current_process().pid}] Assigned partition {pnum}, end_offset={hi}, writing to '{out_name}'")
            to_assign.append(tp)
        # назначаем оффсеты как пришли (subscribe + on_assign требует assign)
        consumer.assign(to_assign)

    try:
        c.subscribe([topic], on_assign=on_assign)

        while True:
            if file_handles and all(reached.get(p, False) for p in file_handles.keys()):
                break

            msg = c.poll(poll_interval)
            if msg is None:
                continue
            if msg.error():
                print(f"[pid {mp.current_process().pid}] Consumer error: {msg.error()}", file=sys.stderr)
                continue

            p = msg.partition()
            offset = msg.offset()
            try:
                val = msg.value().decode('utf-8', errors='replace') if msg.value() is not None else ''
            except Exception:
                val = repr(msg.value())

            fh = file_handles.get(p)
            if fh:
                fh.write(val)
                fh.write("\n")

            hi = end_offsets.get(p, 0)
            if hi == 0:
                reached[p] = True
            elif offset + 1 >= hi:
                reached[p] = True

    except KeyboardInterrupt:
        pass
    finally:
        for fh in file_handles.values():
            try:
                fh.close()
            except Exception:
                pass
        try:
            c.close()
        except Exception:
            pass
        print(f"[pid {mp.current_process().pid}] Consumer finished.")

def main():
    ap = argparse.ArgumentParser(description="Copy file to Kafka and dump partitions to files (no SSL).")
    ap.add_argument('-b', '--broker', required=True, help='bootstrap broker (host:port)')
    ap.add_argument('-f', '--file', required=True, help='source file path')
    ap.add_argument('-p', '--partitions', required=True, type=int, help='number of partitions to create for topic')
    ap.add_argument('-c', '--consumers', required=True, type=int, help='number of consumer processes to start')
    args = ap.parse_args()

    broker = args.broker
    src_file = args.file
    num_partitions = args.partitions
    num_consumers = args.consumers

    if not os.path.isfile(src_file):
        print("Source file not found:", src_file, file=sys.stderr)
        sys.exit(2)

    base_name = os.path.basename(src_file)
    topic = "copy_" + base_name.replace('.', '_').replace(' ', '_')

    try:
        create_topic_if_not_exists(broker, topic, num_partitions, replication_factor=1)
    except Exception as e:
        print("Failed to ensure topic exists:", e, file=sys.stderr)
        sys.exit(3)

    prod_conf = build_conf(broker)
    produce_file_to_topic(prod_conf, topic, src_file, num_partitions)

    group_id = f"copygroup_{topic}_{int(time.time())}"
    print(f"Starting {num_consumers} consumer(s) in group '{group_id}'...")
    procs = []
    for i in range(num_consumers):
        p = mp.Process(target=consumer_worker, args=(broker, topic, group_id, base_name))
        p.start()
        procs.append(p)

    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        print("Interrupted — terminating consumers...")
        for p in procs:
            p.terminate()
        for p in procs:
            p.join()

    print("Done. Check files: " + ", ".join([f"partition{i}_{base_name}" for i in range(num_partitions)]))

if __name__ == '__main__':
    main()
