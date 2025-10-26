import asyncio, base64, io, os, uuid, json, struct, math, time
from collections import defaultdict, deque
import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from aiokafka import AIOKafkaConsumer
from PIL import Image

from .kafka_bridge import KafkaBridge

# ---------- ЛОГИ ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("webapp")
log_answers = logging.getLogger("answers")

# ---------- КОНФИГ ----------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FRAMES_TOPIC = os.getenv("KAFKA_FRAMES_TOPIC", "frames")
PRED_TOPIC = os.getenv("KAFKA_PREDICTIONS_TOPIC", "emotions")

AUDIO_TOPIC = os.getenv("KAFKA_AUDIO_TOPIC", "audio")
TRANSCRIPTS_TOPIC = os.getenv("KAFKA_TRANSCRIPTS_TOPIC", "transcripts")
ANSWERS_TOPIC = os.getenv("KAFKA_ANSWERS_TOPIC", "answers")

# Буфер ожидания ответов, если WS ещё не подключён
PENDING_TTL = float(os.getenv("ANSWERS_PENDING_TTL", "60"))  # сек
PENDING_CAP = int(os.getenv("ANSWERS_PENDING_CAP", "20"))    # макс. сообщений в очереди на чат

pending_answers: dict[str, deque[tuple[float, dict]]] = defaultdict(deque)

# ---------- УТИЛЫ ----------
def rms_int16le(buf: bytes) -> float:
    """Нормированный RMS 0..1 для PCM16LE."""
    if not buf:
        return 0.0
    n = len(buf) // 2
    if n == 0:
        return 0.0
    s = struct.unpack("<" + "h" * n, buf[: n * 2])
    mean_sq = sum(v * v for v in s) / n
    return math.sqrt(mean_sq) / 32768.0

def downscale_jpeg_if_large(jpeg_bytes: bytes, max_dim: int = 512) -> bytes:
    with Image.open(io.BytesIO(jpeg_bytes)) as im:
        im = im.convert("RGB")
        w, h = im.size
        if max(w, h) > max_dim:
            ratio = max_dim / max(w, h)
            im = im.resize((int(w * ratio), int(h * ratio)))
        out = io.BytesIO()
        im.save(out, format="JPEG", quality=80)
        return out.getvalue()

# ---------- APP ----------
app = FastAPI(title="Webcam Emotion Detection")
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

connections: dict[str, WebSocket] = {}        # для эмоций
speech_connections: dict[str, WebSocket] = {} # для речи

# ссылки на фоновые задачи/консюмеры answers
_answers_task: asyncio.Task | None = None
_answers_consumer: AIOKafkaConsumer | None = None
_pending_gc_task: asyncio.Task | None = None

kafka_bridge = KafkaBridge(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    frames_topic=FRAMES_TOPIC,
    predictions_topic=PRED_TOPIC,
    audio_topic=AUDIO_TOPIC,
    transcripts_topic=TRANSCRIPTS_TOPIC,
)

# ---------- LIFECYCLE ----------
@app.on_event("startup")
async def startup_event():
    await kafka_bridge.start()
    asyncio.create_task(predictions_fanout())
    asyncio.create_task(transcripts_fanout())
    # answers consumer + GC loop
    global _answers_task, _pending_gc_task
    _answers_task = asyncio.create_task(answers_fanout())
    _pending_gc_task = asyncio.create_task(_pending_gc_loop())
    log.info("startup complete")

@app.on_event("shutdown")
async def shutdown_event():
    global _answers_task, _answers_consumer, _pending_gc_task
    # останов GC
    if _pending_gc_task and not _pending_gc_task.done():
        _pending_gc_task.cancel()
        try:
            await _pending_gc_task
        except asyncio.CancelledError:
            pass

    # останов answers consumer
    if _answers_task and not _answers_task.done():
        _answers_task.cancel()
        try:
            await _answers_task
        except asyncio.CancelledError:
            pass
    if _answers_consumer:
        try:
            await _answers_consumer.stop()
        except Exception:
            pass

    await kafka_bridge.stop()
    log.info("shutdown complete")

# ---------- ROUTES ----------
@app.get("/")
async def index():
    return HTMLResponse(open(os.path.join(os.path.dirname(__file__), "static", "index.html")).read())

# === WS: ВИДЕО ===
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    session_id = str(uuid.uuid4())
    await ws.send_json({"type": "session", "session_id": session_id})
    connections[session_id] = ws
    try:
        while True:
            msg = await ws.receive_json()
            if msg.get("type") == "frame":
                data_url = msg.get("data_url", "")
                b64 = data_url.split(",", 1)[1] if "," in data_url else data_url
                raw = base64.b64decode(b64)
                raw = downscale_jpeg_if_large(raw, 512)
                await kafka_bridge.produce_frame(session_id, raw)
    except WebSocketDisconnect:
        pass
    finally:
        connections.pop(session_id, None)

# === WS: РЕЧЬ ===
@app.websocket("/ws_speech")
async def websocket_speech(ws: WebSocket):
    await ws.accept()
    session_id = str(uuid.uuid4())
    await ws.send_json({"type": "session", "session_id": session_id})
    speech_connections[session_id] = ws
    log.info(f"[speech] WS connected session_id={session_id}")

    # При подключении — выгружаем отложенные ответы
    await _flush_pending_for(session_id, ws)

    MIN_LEN = 2000
    MIN_RMS = 1e-3
    DEBUG_INTERVAL = 0.5

    chunks = 0
    dropped_small = 0
    dropped_silent = 0
    last_debug_ts = time.time()

    try:
        while True:
            data = await ws.receive_bytes()
            l = len(data)
            lvl = rms_int16le(data)
            chunks += 1

            if l < MIN_LEN:
                dropped_small += 1
                log.warning(f"[speech] {session_id} drop: too short len={l} rms={lvl:.6f}")
            elif lvl < MIN_RMS:
                dropped_silent += 1
                log.warning(f"[speech] {session_id} len={l} rms={lvl:.8f}")
            else:
                await kafka_bridge.produce_audio_chunk(session_id, data)

            now = time.time()
            if now - last_debug_ts >= DEBUG_INTERVAL:
                try:
                    await ws.send_json({
                        "type": "speech_debug",
                        "len": l,
                        "rms": round(lvl, 6),
                        "chunks": chunks,
                        "dropped_small": dropped_small,
                        "dropped_silent": dropped_silent,
                        "ts": now
                    })
                except Exception:
                    pass
                last_debug_ts = now

    except WebSocketDisconnect:
        log.info(f"[speech] {session_id} disconnected; sending EOF to Kafka")
        try:
            await kafka_bridge.produce_audio_chunk(session_id, b"")
        except Exception as e:
            log.error(f"[speech] EOF send error: {e}")
    finally:
        speech_connections.pop(session_id, None)

# ---------- FANOUTS ----------
async def predictions_fanout():
    async for record in kafka_bridge.consume_predictions():
        ws = connections.get(record.get("session_id"))
        if ws:
            await ws.send_json({"type": "prediction", **record})

async def transcripts_fanout():
    async for record in kafka_bridge.consume_transcripts():
        ws = speech_connections.get(record.get("session_id"))
        if ws:
            await ws.send_json({"type": "speech", **record})

async def answers_fanout():
    """
    Читает Kafka ANSWERS_TOPIC и шлёт в WS {"type":"answer", ...}.
    Если WS отсутствует — кладёт в pending с TTL.
    """
    global _answers_consumer
    consumer = AIOKafkaConsumer(
        ANSWERS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        group_id="webapp-consumers-answers",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    _answers_consumer = consumer
    await consumer.start()
    log_answers.info(f"[answers] consumer started topic={ANSWERS_TOPIC} bootstrap={KAFKA_BOOTSTRAP_SERVERS}")
    try:
        async for msg in consumer:
            record = msg.value or {}
            chat_id = record.get("chat_id") or record.get("session_id") or record.get("sessionId")
            log_answers.info(
                f"[answers] got msg offset={msg.offset} key={msg.key} chat_id={chat_id} "
                f"payload={str(record)[:200]}"
            )
            if not chat_id:
                log_answers.warning(f"[answers] skip: no chat_id/session_id in {record}")
                continue

            ws = speech_connections.get(chat_id)
            if not ws:
                # Буферизуем с TTL
                q = pending_answers[chat_id]
                q.append((time.time(), record))
                while len(q) > PENDING_CAP:
                    q.popleft()
                log_answers.warning(
                    f"[answers] no WS for chat_id={chat_id}; queued={len(q)} "
                    f"active_sessions={list(speech_connections.keys())}"
                )
                continue

            try:
                await ws.send_json({"type": "answer", **record})
                log_answers.info(f"[answers] -> WS chat_id={chat_id} OK")
            except Exception as e:
                log_answers.warning(f"[answers] WS send failed chat_id={chat_id}: {e}")
    except asyncio.CancelledError:
        pass
    finally:
        try:
            await consumer.stop()
        except Exception:
            pass
        log_answers.info("[answers] consumer stopped")

# ---------- PENDING HELPERS ----------
async def _flush_pending_for(session_id: str, ws: WebSocket):
    """Отдать все отложенные ответы для данной сессии, не старше TTL."""
    q = pending_answers.get(session_id)
    if not q:
        return
    now = time.time()
    flushed = 0
    while q:
        ts, rec = q[0]
        if now - ts > PENDING_TTL:
            q.popleft()
            continue
        try:
            await ws.send_json({"type": "answer", **rec})
            flushed += 1
        except Exception:
            # если не получилось отправить — оставим как есть
            break
        q.popleft()
    if not q:
        pending_answers.pop(session_id, None)
    log_answers.info(f"[answers] flushed {flushed} pending for {session_id}")

async def _pending_gc_loop():
    """Периодическая очистка просроченных записей буфера."""
    try:
        while True:
            now = time.time()
            for sid, q in list(pending_answers.items()):
                changed = False
                while q and (now - q[0][0] > PENDING_TTL):
                    q.popleft()
                    changed = True
                if not q:
                    pending_answers.pop(sid, None)
                elif changed:
                    log_answers.info(f"[answers] GC trimmed queue for {sid}, left={len(q)}")
            await asyncio.sleep(10)
    except asyncio.CancelledError:
        pass

