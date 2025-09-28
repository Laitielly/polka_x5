import os
import asyncio
import logging
import json
from fastapi import FastAPI, BackgroundTasks
from .schemas import Command, ItemOut
from .crud import list_items_sync, change_qty_sync
from .kafka_client import ProducerWrapper, ConsumerWrapper, producer, consumer
from .db import engine, Base
from . import kafka_client as kafka_client_module

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('warehouse')

# ensure tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title='Warehouse')

# wrappers (initialized on startup)
producer: ProducerWrapper | None = None
consumer: ConsumerWrapper | None = None

@app.on_event('startup')
async def startup():
    global producer, consumer
    loop = asyncio.get_event_loop()
    # создаём локальные обёртки
    producer = kafka_client_module.ProducerWrapper(loop)
    consumer = kafka_client_module.ConsumerWrapper(loop)

    # стартуем
    await producer.start()
    await consumer.start()

    # важно: положим ссылки в модуль kafka_client, чтобы endpoint мог их использовать
    kafka_client_module.producer = producer
    kafka_client_module.consumer = consumer

    logger.info('App started')

@app.on_event('shutdown')
async def shutdown():
    if consumer:
        await consumer.stop()
    if producer:
        await producer.stop()
    logger.info('App stopped')

@app.get('/items', response_model=list[ItemOut])
async def get_items():
    return await asyncio.to_thread(list_items_sync)

@app.post('/commands')
async def post_command(cmd: Command):
    payload = cmd.dict()

    # берем значения напрямую из kafka_client модуля
    if kafka_client_module.USE_KAFKA and getattr(kafka_client_module, 'producer', None) and getattr(kafka_client_module.producer, 'producer', None):
        # send to kafka commands topic
        await kafka_client_module.producer.producer.send_and_wait(
            os.getenv('KAFKA_TOPIC_COMMANDS', 'commands'),
            json.dumps(payload).encode('utf-8')
        )
    else:
        # in-memory queue must have been initialized on startup
        if kafka_client_module.in_queue is None:
            raise RuntimeError("in_queue is not initialized")
        await kafka_client_module.in_queue.put(payload)

    return {'status': 'accepted'}

