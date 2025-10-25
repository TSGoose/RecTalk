import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

class KafkaBridge:
    def __init__(self, bootstrap_servers, frames_topic, predictions_topic):
        self.bootstrap_servers = bootstrap_servers
        self.frames_topic = frames_topic
        self.predictions_topic = predictions_topic
        self.producer = None
        self.consumer = None

    async def start(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers, linger_ms=20)
        await self.producer.start()
        self.consumer = AIOKafkaConsumer(
            self.predictions_topic,
            bootstrap_servers=self.bootstrap_servers,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="webapp-consumers",
        )
        await self.consumer.start()

    async def stop(self):
        if self.consumer: await self.consumer.stop()
        if self.producer: await self.producer.stop()

    async def produce_frame(self, session_id, jpeg_bytes):
        await self.producer.send_and_wait(self.frames_topic, key=session_id.encode(), value=jpeg_bytes)

    async def consume_predictions(self):
        while True:
            msg = await self.consumer.getone()
            yield msg.value

