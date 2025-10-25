import asyncio, base64, io, os, uuid
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from PIL import Image
from .kafka_bridge import KafkaBridge

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FRAMES_TOPIC = os.getenv("KAFKA_FRAMES_TOPIC", "frames")
PRED_TOPIC = os.getenv("KAFKA_PREDICTIONS_TOPIC", "emotions")

app = FastAPI(title="Webcam Emotion Detection")
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

connections = {}
kafka_bridge = KafkaBridge(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    frames_topic=FRAMES_TOPIC,
    predictions_topic=PRED_TOPIC,
)

@app.on_event("startup")
async def startup_event():
    await kafka_bridge.start()
    asyncio.create_task(predictions_fanout())

@app.on_event("shutdown")
async def shutdown_event():
    await kafka_bridge.stop()

@app.get("/")
async def index():
    return HTMLResponse(open(os.path.join(os.path.dirname(__file__), "static", "index.html")).read())

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
                if "," in data_url:
                    b64 = data_url.split(",", 1)[1]
                else:
                    b64 = data_url
                raw = base64.b64decode(b64)
                raw = downscale_jpeg_if_large(raw, 512)
                await kafka_bridge.produce_frame(session_id, raw)
    except WebSocketDisconnect:
        pass
    finally:
        connections.pop(session_id, None)

async def predictions_fanout():
    async for record in kafka_bridge.consume_predictions():
        ws = connections.get(record.get("session_id"))
        if ws:
            await ws.send_json({"type": "prediction", **record})

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

