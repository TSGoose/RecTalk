import asyncio, base64, io, os, uuid
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from PIL import Image
from .kafka_bridge import KafkaBridge

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FRAMES_TOPIC = os.getenv("KAFKA_FRAMES_TOPIC", "frames")
PRED_TOPIC = os.getenv("KAFKA_PREDICTIONS_TOPIC", "emotions")

# новое:
AUDIO_TOPIC = os.getenv("KAFKA_AUDIO_TOPIC", "audio")
TRANSCRIPTS_TOPIC = os.getenv("KAFKA_TRANSCRIPTS_TOPIC", "transcripts")

# --- ДОБАВЬ ЭТО ВВЕРХУ webapp.py ---
import struct, math, time, logging

log = logging.getLogger("speech")
logging.basicConfig(level=logging.INFO)

def rms_int16le(buf: bytes) -> float:
    """Нормированный RMS 0..1 для PCM16LE."""
    if not buf:
        return 0.0
    n = len(buf) // 2
    if n == 0:
        return 0.0
    # Берём только целые сэмплы
    s = struct.unpack("<" + "h"*n, buf[:n*2])
    mean_sq = sum(v*v for v in s) / n
    return math.sqrt(mean_sq) / 32768.0
# --- КОНЕЦ ДОБАВЛЕНИЙ ---

app = FastAPI(title="Webcam Emotion Detection")
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

connections = {}         # для эмоций (видео)
speech_connections = {}  # для речи (аудио)

kafka_bridge = KafkaBridge(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    frames_topic=FRAMES_TOPIC,
    predictions_topic=PRED_TOPIC,
    audio_topic=AUDIO_TOPIC,
    transcripts_topic=TRANSCRIPTS_TOPIC,
)

@app.on_event("startup")
async def startup_event():
    await kafka_bridge.start()
    asyncio.create_task(predictions_fanout())
    asyncio.create_task(transcripts_fanout())  # новое

@app.on_event("shutdown")
async def shutdown_event():
    await kafka_bridge.stop()

@app.get("/")
async def index():
    return HTMLResponse(open(os.path.join(os.path.dirname(__file__), "static", "index.html")).read())

# === WS для видео (как было)
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

# === НОВЫЙ WS для аудио
@app.websocket("/ws_speech")
async def websocket_speech(ws: WebSocket):
    await ws.accept()
    session_id = str(uuid.uuid4())
    await ws.send_json({"type": "session", "session_id": session_id})
    speech_connections[session_id] = ws

    # Пороговые значения для диагностики
    MIN_LEN = 2000          # байт; ожидание ~3200-5000 на 100-150 мс
    MIN_RMS = 1e-3          # очень тихо < 0.001
    DEBUG_INTERVAL = 0.5    # сек

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
                # пропускаем отправку в Kafka
            elif lvl < MIN_RMS:
                dropped_silent += 1
                log.warning(f"[speech] {session_id} len={l} rms={lvl:.8f}")
            else:
                # нормальный кадр — отправляем в Kafka
                await kafka_bridge.produce_audio_chunk(session_id, data)

            # Периодически отправляем отладочную телеметрию в браузер
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
                    # игнорируем ошибки отправки дебага
                    pass
                last_debug_ts = now

    except WebSocketDisconnect:
        log.info(f"[speech] {session_id} disconnected; sending EOF to Kafka")
        try:
            # EOF для закрытия сессии в ASR
            await kafka_bridge.produce_audio_chunk(session_id, b"")
        except Exception as e:
            log.error(f"[speech] EOF send error: {e}")
    finally:
        speech_connections.pop(session_id, None)


async def predictions_fanout():
    async for record in kafka_bridge.consume_predictions():
        ws = connections.get(record.get("session_id"))
        if ws:
            await ws.send_json({"type": "prediction", **record})

# новый фанаут транскриптов
async def transcripts_fanout():
    async for record in kafka_bridge.consume_transcripts():
        ws = speech_connections.get(record.get("session_id"))
        if ws:
            await ws.send_json({"type": "speech", **record})

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

