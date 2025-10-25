import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

class KafkaBridge:
    def __init__(self, bootstrap_servers, frames_topic, predictions_topic,
                 audio_topic=None, transcripts_topic=None):
        self.bootstrap_servers = bootstrap_servers
        self.frames_topic = frames_topic
        self.predictions_topic = predictions_topic
        self.audio_topic = audio_topic
        self.transcripts_topic = transcripts_topic
        self.producer = None
        self.pred_consumer = None
        self.tr_consumer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            linger_ms=20,
            max_request_size=int(int_or(self.env("AIOKAFKA_MAX_REQUEST_SIZE"), 10485760)),
        )
        await self.producer.start()

        # consumer для эмоций
        self.pred_consumer = AIOKafkaConsumer(
            self.predictions_topic,
            bootstrap_servers=self.bootstrap_servers,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="webapp-consumers-emotions",
        )
        await self.pred_consumer.start()

        # consumer для транскриптов (если указан transcripts_topic)
        if self.transcripts_topic:
            self.tr_consumer = AIOKafkaConsumer(
                self.transcripts_topic,
                bootstrap_servers=self.bootstrap_servers,
                enable_auto_commit=True,
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id="webapp-consumers-transcripts",
            )
            await self.tr_consumer.start()

    async def stop(self):
        if self.pred_consumer: await self.pred_consumer.stop()
        if self.tr_consumer: await self.tr_consumer.stop()
        if self.producer: await self.producer.stop()

    async def produce_frame(self, session_id, jpeg_bytes):
        await self.producer.send_and_wait(self.frames_topic, key=session_id.encode(), value=jpeg_bytes)

    async def produce_audio_chunk(self, session_id, pcm_bytes):
        # пустой value трактуем как EOF для сессии
        await self.producer.send_and_wait(self.audio_topic, key=session_id.encode(), value=pcm_bytes)

    async def consume_predictions(self):
        while True:
            msg = await self.pred_consumer.getone()
            yield msg.value

    async def consume_transcripts(self):
        if not self.tr_consumer:
            return
        while True:
            msg = await self.tr_consumer.getone()
            yield msg.value

    @staticmethod
    def env(name, default=None):
        import os
        return os.getenv(name, default)

def int_or(v, dflt):
    try: return int(v)
    except: return dflt

