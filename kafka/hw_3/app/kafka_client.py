import os
import json
import asyncio
import logging
from typing import Optional, Dict, Any
from .crud import change_qty_sync, list_items_sync

logger = logging.getLogger('warehouse.kafka')

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '').strip()
TOPIC_COMMANDS = os.getenv('KAFKA_TOPIC_COMMANDS', 'commands')
TOPIC_NOTIF = os.getenv('KAFKA_TOPIC_NOTIF', 'notifications')
CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'warehouse_group')

# Try import aiokafka if available
try:
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
    AI_OKAFKA = True
except Exception:
    AI_OKAFKA = False

USE_KAFKA = bool(KAFKA_BOOTSTRAP) and AI_OKAFKA

in_queue: Optional[asyncio.Queue] = None
out_queue: Optional[asyncio.Queue] = None

class ProducerWrapper:
    def __init__(self, loop):
        self.loop = loop
        self.producer = None

    async def start(self):
        if USE_KAFKA:
            self.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
            await self.producer.start()
            logger.info('Kafka producer started')
        else:
            global out_queue
            out_queue = asyncio.Queue()
            logger.info('In-memory out_queue created')

    async def stop(self):
        if USE_KAFKA and self.producer:
            await self.producer.stop()

    async def send_notification(self, payload: Dict[str, Any]):
        data = json.dumps(payload).encode('utf-8')
        if USE_KAFKA:
            await self.producer.send_and_wait(TOPIC_NOTIF, data)
        else:
            await out_queue.put(payload)

class ConsumerWrapper:
    def __init__(self, loop):
        self.loop = loop
        self.consumer = None
        self.task = None
        self.running = False

    async def start(self):
        if USE_KAFKA:
            self.consumer = AIOKafkaConsumer(TOPIC_COMMANDS, bootstrap_servers=KAFKA_BOOTSTRAP, group_id=CONSUMER_GROUP, enable_auto_commit=False)
            await self.consumer.start()
            self.running = True
            self.task = asyncio.create_task(self._consume_loop_kafka())
            logger.info('Kafka consumer started')
        else:
            global in_queue
            in_queue = asyncio.Queue()
            self.running = True
            self.task = asyncio.create_task(self._consume_loop_memory())
            logger.info('In-memory in_queue created')

    async def stop(self):
        self.running = False
        if self.task:
            await self.task
        if USE_KAFKA and self.consumer:
            await self.consumer.stop()

    async def _handle_message(self, payload: Dict[str, Any], kafka_commit_callable=None):
        try:
            action = payload.get('action')
            sku = payload.get('sku')
            qty = int(payload.get('qty', 0))
        except Exception:
            if kafka_commit_callable:
                await kafka_commit_callable()
            return

        # call blocking db logic in threadpool
        result = await asyncio.to_thread(change_qty_sync, action, sku, qty)
        if result.get('success'):
            notif = {"sku": sku, "action": action, "qty": qty, "result_qty": result.get('item', {}).get('qty')}
            await producer.send_notification(notif)
            logger.info(f"Processed and notified: {payload}")
        else:
            logger.info(f"Command handled without notification: {payload}, reason={result.get('reason')}")
        if kafka_commit_callable:
            await kafka_commit_callable()

    async def _consume_loop_kafka(self):
        try:
            async for msg in self.consumer:
                try:
                    payload = json.loads(msg.value.decode('utf-8'))
                except Exception:
                    payload = {}
                async def _commit():
                    await self.consumer.commit()
                await self._handle_message(payload, kafka_commit_callable=_commit)
                if not self.running:
                    break
        except Exception as e:
            logger.exception('Consumer loop kafka error')

    async def _consume_loop_memory(self):
        while self.running:
            try:
                payload = await in_queue.get()
            except asyncio.CancelledError:
                break
            await self._handle_message(payload)

producer: Optional[ProducerWrapper] = None
consumer: Optional[ConsumerWrapper] = None