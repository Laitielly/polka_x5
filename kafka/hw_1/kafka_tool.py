#!/usr/bin/env python3
"""
kafka_tool.py — простой CLI для Kafka: modes produce / consume / grep.

Usage examples:
  # consume
  python kafka_tool.py -b 127.0.0.1:9092 -m consume -t myTopic

  # produce (interactive lines)
  python kafka_tool.py -b 127.0.0.1:9092 -m produce -t myTopic

  # grep (regex) from beginning until current end
  python kafka_tool.py -b 127.0.0.1:9092 -m grep -t logs -e "139\\.352\\.176\\.239.*"

SSL (optional):
  add --ssl-ca ./certs/ca.crt --ssl-cert ./certs/user.crt --ssl-key ./certs/user.key
  when using PKCS12 client keystore, provide paths to extracted PEM cert/key as above.
"""
import argparse
import re
import sys
import time
from confluent_kafka import Producer, Consumer, TopicPartition, KafkaException

def build_conf(broker, ssl_ca, ssl_cert, ssl_key, ssl_key_pass, group_id=None):
    conf = {
        'bootstrap.servers': broker,
        'enable.auto.commit': False,
    }
    if group_id:
        conf['group.id'] = group_id

    # SSL options (only set if any provided)
    if ssl_ca or ssl_cert or ssl_key:
        conf['security.protocol'] = 'ssl'
        if ssl_ca:
            conf['ssl.ca.location'] = ssl_ca
        if ssl_cert:
            conf['ssl.certificate.location'] = ssl_cert
        if ssl_key:
            conf['ssl.key.location'] = ssl_key
        if ssl_key_pass:
            conf['ssl.key.password'] = ssl_key_pass

    return conf

def mode_produce(conf, topic):
    # Producer should not have group.id
    conf = conf.copy()
    conf.pop('group.id', None)
    p = Producer(conf)
    print(f"Produce mode → topic `{topic}`. Type messages and press Enter. Ctrl-C to exit.")
    try:
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            msg = line.rstrip("\n")
            if msg == "":
                continue

            delivered = {'ok': False, 'err': None}
            def delivery_cb(err, _msg):
                if err is None:
                    delivered['ok'] = True
                else:
                    delivered['err'] = err

            try:
                p.produce(topic, value=msg.encode('utf-8'), callback=delivery_cb)
            except BufferError:
                # local queue full: flush and retry once
                p.flush()
                p.produce(topic, value=msg.encode('utf-8'), callback=delivery_cb)

            # poll to serve delivery callbacks
            t0 = time.time()
            timeout = 10.0
            while not delivered['ok'] and delivered['err'] is None and (time.time() - t0) < timeout:
                p.poll(0.1)

            # final flush
            p.flush(5.0)

            if delivered['ok']:
                print("Message delivered.")
            else:
                print("Delivery failed:", delivered['err'])
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        p.flush(5.0)

def mode_consume(conf, topic):
    conf = conf.copy()
    conf.update({
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    c = Consumer(conf)
    try:
        c.subscribe([topic])
        print(f"Consume mode → topic `{topic}`. Waiting for messages... Ctrl-C to stop.")
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # librdkafka error object
                print("Consumer error:", msg.error(), file=sys.stderr)
                continue

            try:
                value = msg.value().decode('utf-8') if msg.value() is not None else None
            except Exception:
                value = repr(msg.value())
            try:
                key = msg.key().decode('utf-8') if msg.key() is not None else None
            except Exception:
                key = repr(msg.key())

            print(f"value = {value!r}; key = {key!r}; partition = {msg.partition()}")
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        c.close()

def mode_grep(conf, topic, pattern):
    if not pattern:
        print("Error: --expr is required for grep mode", file=sys.stderr)
        return

    conf = conf.copy()
    conf.update({
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    c = Consumer(conf)
    try:
        # get metadata for topic
        md = c.list_topics(topic=topic, timeout=10.0)
        if topic not in md.topics or md.topics[topic].error is not None:
            print(f"Topic `{topic}` not found or has error.", file=sys.stderr)
            return
        tmd = md.topics[topic]
        partitions = sorted(list(tmd.partitions.keys()))
        if not partitions:
            print("No partitions found for topic.", file=sys.stderr)
            return

        # find end offsets (high watermark) for each partition at the time of start
        end_offsets = {}
        for p in partitions:
            # get_watermark_offsets(topic, partition, timeout)
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), 10)
            end_offsets[p] = hi  # hi is the "end" offset (next offset to be written)

        # assign consumer to beginning of each partition
        tps = [TopicPartition(topic, p, 0) for p in partitions]
        c.assign(tps)

        reached = {p: False for p in partitions}
        prog = re.compile(pattern)
        print(f"Searching for pattern '{pattern}' in topic `{topic}` from beginning until current end.")

        # if partition empty already, mark reached
        for p in partitions:
            if end_offsets.get(p, 0) == 0:
                reached[p] = True

        while not all(reached.values()):
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error(), file=sys.stderr)
                continue
            p = msg.partition()
            offset = msg.offset()
            try:
                val = msg.value().decode('utf-8', errors='replace') if msg.value() is not None else ''
            except Exception:
                val = repr(msg.value())

            if prog.search(val):
                print(val)

            # hi is next offset to be written; when we've consumed offset == hi-1, we reached end
            if offset + 1 >= end_offsets.get(p, 0):
                reached[p] = True

    finally:
        c.close()

def main():
    ap = argparse.ArgumentParser(description="Simple Kafka CLI: produce / consume / grep")
    ap.add_argument('-b', '--broker', required=True, help='bootstrap broker address (host:port)')
    ap.add_argument('-m', '--mode', required=True, choices=['produce','consume','grep'], help='mode: produce|consume|grep')
    ap.add_argument('-t', '--topic', required=True, help='topic name')
    ap.add_argument('-e', '--expr', help='regex expression for grep mode')
    # SSL optional args
    ap.add_argument('--ssl-ca', help='path to CA PEM (ssl.ca.location)')
    ap.add_argument('--ssl-cert', help='path to client cert PEM (ssl.certificate.location)')
    ap.add_argument('--ssl-key', help='path to client key PEM (ssl.key.location)')
    ap.add_argument('--ssl-key-pass', help='client key password if any (ssl.key.password)')
    ap.add_argument('--group', help='consumer group id (optional)', default='ktool-group')
    args = ap.parse_args()

    conf_base = build_conf(args.broker, args.ssl_ca, args.ssl_cert, args.ssl_key, args.ssl_key_pass, group_id=args.group)

    try:
        if args.mode == 'produce':
            mode_produce(conf_base, args.topic)
        elif args.mode == 'consume':
            mode_consume(conf_base, args.topic)
        elif args.mode == 'grep':
            mode_grep(conf_base, args.topic, args.expr)
    except KafkaException as e:
        print("Kafka error:", e, file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
