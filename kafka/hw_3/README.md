# Warehouse

## Описание

Простое приложение-имитатор склада (Python + FastAPI + SQLite + Kafka).
Поддерживает команды изменения остатков через REST и через Kafka.
При успешном изменении отправляет нотификацию в Kafka (`notifications`).
Если пополнение приходит на неизвестный артикул — он создаётся. Отрицательные остатки не допускаются. Реализована простая optimistic lock (через `version` в SQLAlchemy + retry).

## Что реализовано

* Приход/списание через REST `POST /commands` (также команды можно публиковать прямо в Kafka topic `commands`).
* Нотификации в Kafka topic `notifications` при успешной обработке.
* `GET /items` — получить список артикулов и текущие количества.
* Поведение при ошибках:

  * При попытке вычесть больше, чем есть — команда считается обработанной (offset/сообщение коммитится), нотификация **не отправляется**.
  * Негативные `qty` отклоняются.
* Оптимистичная блокировка — защита от параллельных конфликтов (retry при StaleDataError).

## Запуск 

1. Поднять Kafka/Zookeeper:

```bash
docker-compose up -d
```

2. Установить зависимости и запустить приложение:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt   
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

3. (Опционально) Создать топики, если авто-creation отключено:

```bash
docker exec -it <kafka_container> kafka-topics --bootstrap-server localhost:9092 --create --topic commands --partitions 1 --replication-factor 1
docker exec -it <kafka_container> kafka-topics --bootstrap-server localhost:9092 --create --topic notifications --partitions 1 --replication-factor 1
```

## API 

* `POST /commands` — принять команду `{ "action": "increase"|"decrease", "sku": "SKU...", "qty": N }`
  Возвращает `{"status":"accepted"}`.
* `GET /items` — вернуть список `[{ "sku": "...", "qty": N }, ...]`.

## Тестовая последовательность 

Отправляем команды через REST:

```bash
curl -X POST http://localhost:8000/commands \
  -H 'Content-Type: application/json' \
  -d '{"action":"increase","sku":"SKU123","qty":10}'
# -> {"status":"accepted"}

curl -X POST http://localhost:8000/commands \
  -H 'Content-Type: application/json' \
  -d '{"action":"decrease","sku":"SKU123","qty":4}'
# -> {"status":"accepted"}

curl http://localhost:8000/items
# -> [{"sku":"SKU123","qty":16}]
```

Проверка топиков в Kafka:

```bash
docker exec -it 2620783ba2b4 kafka-topics --bootstrap-server localhost:9092 --list
# -> 
__consumer_offsets
commands
notifications
```

Просмотр `commands` (console consumer):

```text
{"action": "increase", "sku": "SKU123", "qty": 10}
{"action": "increase", "sku": "SKU123", "qty": 10}
{"action": "decrease", "sku": "SKU123", "qty": 4}
[2025-09-28 10:18:52,766] ERROR Error processing message, terminating consumer process:  (kafka.tools.ConsoleConsumer$)
org.apache.kafka.common.errors.TimeoutException
Processed a total of 3 messages
```

Логи приложения (фрагменты):

```
INFO:     127.0.0.1:62913 - "POST /commands HTTP/1.1" 200 OK
INFO:warehouse.kafka:Processed and notified: {'action': 'increase', 'sku': 'SKU123', 'qty': 10}
INFO:     127.0.0.1:62931 - "POST /commands HTTP/1.1" 200 OK
INFO:warehouse.kafka:Processed and notified: {'action': 'decrease', 'sku': 'SKU123', 'qty': 4}
INFO:     127.0.0.1:62954 - "GET /items HTTP/1.1" 200 OK
```
