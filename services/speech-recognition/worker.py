import asyncio, json, os, time
from collections import defaultdict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from vosk import Model, KaldiRecognizer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
AUDIO_TOPIC = os.getenv("KAFKA_AUDIO_TOPIC", "audio")
TRANSCRIPTS_TOPIC = os.getenv("KAFKA_TRANSCRIPTS_TOPIC", "transcripts")
SAMPLE_RATE = int(os.getenv("ASR_SAMPLE_RATE", "16000"))

# загружаем модель 1 раз
MODEL_PATH = os.getenv("ASR_MODEL_DIR", "/app/model")
model = Model(MODEL_PATH)

class SessionASR:
    def __init__(self):
        self.rec = KaldiRecognizer(model, SAMPLE_RATE)
        self.rec.SetWords(True)
        self.last_emit_partial = 0.0

async def main():
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, linger_ms=20)
    await producer.start()
    consumer = AIOKafkaConsumer(
        AUDIO_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        group_id="speech-recognition-workers",
    )
    await consumer.start()

    sessions: dict[str, SessionASR] = defaultdict(SessionASR)

    try:
        while True:
            msg = await consumer.getone()
            sid = msg.key.decode()
            payload = msg.value  # bytes; пусто => EOF
            asr = sessions[sid]

            if not payload:
                # EOF: добьём финал
                j = json.loads(asr.rec.FinalResult())
                if j.get("text"):
                    await producer.send_and_wait(TRANSCRIPTS_TOPIC, key=sid.encode(),
                        value=json.dumps({"session_id": sid, "text": j["text"], "is_final": True}).encode())
                sessions.pop(sid, None)
                continue

            # есть данные чанка
            if asr.rec.AcceptWaveform(payload):
                j = json.loads(asr.rec.Result())
                text = j.get("text", "")
                await producer.send_and_wait(TRANSCRIPTS_TOPIC, key=sid.encode(),
                    value=json.dumps({"session_id": sid, "text": text, "is_final": True}).encode())
            else:
                # частичный — дросселируем, чтобы не спамить
                now = time.time()
                if now - asr.last_emit_partial > 0.2:  # раз в 200 мс
                    pj = json.loads(asr.rec.PartialResult())
                    ptxt = pj.get("partial", "")
                    await producer.send_and_wait(TRANSCRIPTS_TOPIC, key=sid.encode(),
                        value=json.dumps({"session_id": sid, "text": ptxt, "is_final": False}).encode())
                    asr.last_emit_partial = now
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())

