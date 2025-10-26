import asyncio
import json
import logging
import os
from typing import Union, Optional
from collections import defaultdict

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

load_dotenv()

# ===================== Конфигурация =====================

API_URL = os.getenv("API_URL")
WS_PUBLISH_URL = os.getenv("WS_PUBLISH_URL", "http://websocket:8765/publish")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "transcripts")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "answers")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "message-api-relay")

SERVICE_NAME = os.getenv("SERVICE_NAME", "oldsaratov")
SOURCE_NAME = os.getenv("SOURCE_NAME", "oldsaratov")

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
)
logger = logging.getLogger("rest_api")

# ===================== FastAPI =====================

app = FastAPI(title="Message API", version="1.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===================== Модели HTTP =====================

class MessageIn(BaseModel):
    text: str
    chat_id: Union[str, int]

class MessageOut(BaseModel):
    answer: str

# ===================== Вспомогательные =====================

async def _publish_to_ws_async(client: httpx.AsyncClient, payload: dict) -> None:
    """Отправка события в WS /publish (best-effort)."""
    try:
        await client.post(WS_PUBLISH_URL, json=payload, timeout=10.0)
        logger.debug(f"[WS] published to {WS_PUBLISH_URL}: {payload}")
    except Exception as e:
        logger.warning(f"Ошибка публикации в WS (/publish): {e}")

async def call_external_api_async(client: httpx.AsyncClient, user_message: str, chat_id: Union[str, int]) -> str:
    """Асинхронный вызов внешнего API."""
    if not API_URL:
        return "Ошибка конфигурации: не задан API_URL"

    # <<< LOG >>> Перед запросом
    logger.info(f"[API] → POST {API_URL} chat_id={chat_id} text={user_message[:120]!r}")

    try:
        r = await client.post(
            API_URL,
            json={
                "text": user_message,
                "service": SERVICE_NAME,
                "source": SOURCE_NAME,
                "chat_id": str(chat_id),
            },
            timeout=30.0,
        )

        # <<< LOG >>> Ответ от API
        logger.info(f"[API] ← status={r.status_code} len={len(r.text)}")

        if r.status_code == 200:
            try:
                data = r.json()
            except Exception as e:
                logger.warning(f"[API] JSON decode failed: {e}")
                return r.text[:500]

            answer = data.get("answer", "")
            logger.info(f"[API] answer(chat_id={chat_id})={answer[:120]!r}")  # <<< LOG >>>
            return answer
        else:
            try:
                err_body = r.json()
            except ValueError:
                err_body = await r.aread()
            logger.error(f"[API] Error response: {err_body}")  # <<< LOG >>>
            return f"Ошибка API: {err_body}"
    except Exception as e:
        logger.exception(f"[API] Exception: {e}")  # <<< LOG >>>
        return f"Произошла ошибка при запросе: {e}"

def _extract_fields(record: dict) -> tuple[Optional[str], str, Optional[bool]]:
    """Извлекаем chat_id, text, is_final из записи Kafka."""
    chat_id = record.get("session_id") or record.get("sessionId") or record.get("chat_id") or record.get("chatId")
    text = (record.get("text") or "").strip()
    if "is_final" in record:
        is_final = bool(record.get("is_final"))
    elif "isFinal" in record:
        is_final = bool(record.get("isFinal"))
    else:
        is_final = None
    return chat_id, text, is_final

# ===================== Kafka consumer + producer =====================

async def kafka_relay_loop():
    """
    Консюмер читает сообщения из Kafka (transcripts),
    вызывает внешний API только на финальные (is_final=True) реплики,
    и публикует:
      - ответ в Kafka (answers),
      - объединённое сообщение в WS.
    """
    logger.info(
        f"[kafka] start consumer bootstrap={KAFKA_BOOTSTRAP} "
        f"topic={KAFKA_INPUT_TOPIC} -> {KAFKA_OUTPUT_TOPIC}"
    )

    consumer = AIOKafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await consumer.start()
    await producer.start()
    app.state.kafka_consumer = consumer
    app.state.kafka_producer = producer
    http_client: httpx.AsyncClient = app.state.http_client

    try:
        async for msg in consumer:
            record = msg.value or {}
            chat_id, text, is_final = _extract_fields(record)
            if not chat_id or not text:
                continue

            # 🔹 Логирование входящего текста
            logger.info(f"[pipe] IN transcript(chat_id={chat_id}) is_final={is_final}: {text!r}")

            # Только финальные сообщения отправляем во внешний API
            if not is_final:
                continue

            # 🔹 Логирование вызова API
            logger.info(f"[API] → POST {API_URL} chat_id={chat_id} text={text!r}")

            answer = await call_external_api_async(http_client, text, chat_id)

            # 🔹 Логирование ответа API
            logger.info(f"[API] ← chat_id={chat_id} answer={answer[:200]!r}")

            # Публикуем ответ в Kafka
            answer_payload = {
                "chat_id": chat_id,
                "request": text,
                "answer": answer,
                "is_final": bool(is_final),
                "service": SERVICE_NAME,
            }

            try:
                metadata = await producer.send_and_wait(
                    KAFKA_OUTPUT_TOPIC,
                    key=str(chat_id).encode(),
                    value=json.dumps(answer_payload).encode("utf-8"),
                )
                logger.info(
                    f"[kafka] OUT answers chat_id={chat_id} partition={metadata.partition} offset={metadata.offset}"
                )
            except Exception as e:
                logger.warning(f"[kafka] Ошибка отправки в answers: {e}")

            # Отправляем в WS объединённое сообщение
            ws_payload = {
                "chat_id": chat_id,
                "type": "speech_with_answer",
                "request": text,
                "answer": answer,
                "is_final": bool(is_final),
            }
            await _publish_to_ws_async(http_client, ws_payload)

    except asyncio.CancelledError:
        logger.info("[kafka] consumer loop cancelled")
    finally:
        await consumer.stop()
        await producer.stop()
        logger.info("[kafka] consumer stopped")


# ===================== Lifecycle =====================

@app.on_event("startup")
async def _on_startup():
    app.state.http_client = httpx.AsyncClient()
    app.state.consumer_task = asyncio.create_task(kafka_relay_loop())
    logger.info("Startup complete")

@app.on_event("shutdown")
async def _on_shutdown():
    task: asyncio.Task = app.state.consumer_task
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    if client := app.state.http_client:
        await client.aclose()
    logger.info("Shutdown complete")

# ===================== HTTP endpoints =====================

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/message", response_model=MessageOut)
def receive_message(payload: MessageIn):
    """Синхронный REST-вызов API (отдельно от Kafka конвейера)."""
    import requests
    try:
        logger.info(f"[HTTP] manual /message: {payload.text[:80]!r}")  # <<< LOG >>>
        response = requests.post(
            API_URL,
            json={
                "text": payload.text,
                "service": SERVICE_NAME,
                "source": SOURCE_NAME,
                "chat_id": str(payload.chat_id),
            },
            timeout=30,
        )
        logger.info(f"[HTTP] API response: {response.status_code} len={len(response.text)}")  # <<< LOG >>>
        if response.status_code == 200:
            answer = response.json().get("answer", "")
            return MessageOut(answer=answer)
        else:
            return MessageOut(answer=f"Ошибка API: {response.text}")
    except Exception as e:
        logger.exception("[HTTP] Error in /message")
        raise HTTPException(status_code=502, detail=f"Произошла ошибка: {e}")

if __name__ == "__main__":
    uvicorn.run("rest_api:app", host="0.0.0.0", port=int(os.getenv("PORT", "8010")), reload=True)

