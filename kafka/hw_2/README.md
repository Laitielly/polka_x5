# Результаты отчета

## Цель

Скрипт `kfile_copy.py`:

1. создаёт топик `copy_<имя_файла>` с указанным числом партиций;
2. построчно отправляет содержимое файла в топик;
3. запускает `C` параллельных потребителей в одной группе, каждый пишет сообщения своих назначенных партиций в файлы `partition{N}_{имя_файла}`.


## Команда запуска 

```bash
python3 kfile_copy.py -b localhost:9092 -f example.txt -p 2 -c 2
```

## Лог/вывод 

```
Topic 'copy_example_txt' created with 2 partitions.
%4|1759000015.542|CONFWARN|rdkafka#producer-2| [thrd:app]: Configuration property enable.auto.commit is a consumer property and will be ignored by this producer instance
Producing 'example.txt' -> topic 'copy_example_txt' (2 partitions)
Produced 6 messages.
Starting 2 consumer(s) in group 'copygroup_copy_example_txt_1759000015'...
[pid 36812] Assigned partition 1, end_offset=3, writing to 'partition1_example.txt'
[pid 36811] Assigned partition 0, end_offset=3, writing to 'partition0_example.txt'
[pid 36812] Consumer finished.
[pid 36811] Consumer finished.
Done. Check files: partition0_example.txt, partition1_example.txt
```

**Проверка метаданных топика:**

```bash
docker exec -it <kafka_container> kafka-topics --bootstrap-server localhost:9092 --describe --topic copy_example_txt
```

Вывод:

```
Topic: copy_example_txt	TopicId: mrz-OwGHS-6X-hp3t7XTww	PartitionCount: 2	ReplicationFactor: 1	Configs: 
	Topic: copy_example_txt	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: copy_example_txt	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
```

**Просмотр сообщений через `kafka-console-consumer`:**

```bash
docker exec -it <kafka_container> kafka-console-consumer --bootstrap-server localhost:9092 --topic copy_example_txt --from-beginning --timeout-ms 10000
```

Вывод (пример):

```
line-1
line-3
line-5
line-0
line-2
line-4
[2025-09-27 19:16:35,756] ERROR Error processing message, terminating consumer process:  (kafka.tools.ConsoleConsumer$)
org.apache.kafka.common.errors.TimeoutException
Processed a total of 6 messages
```

## Ожидаемый результат (файлы)

В рабочей директории должны появиться:

```
partition0_example.txt
partition1_example.txt
```

Содержимое при round-robin (пример для 6 строк):

* `partition0_example.txt`: `line-0`, `line-2`, `line-4`
* `partition1_example.txt`: `line-1`, `line-3`, `line-5`
