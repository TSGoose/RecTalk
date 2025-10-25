import os, asyncio, base64, re, cv2, numpy as np, orjson, logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# ---------- логирование ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("worker")

# ---------- Kafka конфиг ----------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_IN  = os.getenv("KAFKA_FRAMES_TOPIC", "frames")
TOPIC_OUT = os.getenv("KAFKA_PREDICTIONS_TOPIC", "emotions")
GROUP_ID  = os.getenv("KAFKA_GROUP_ID", "rectalk-worker")
FETCH_MAX = int(os.getenv("AIOKAFKA_FETCH_MAX_BYTES", 10 * 1024 * 1024))
REQ_MAX   = int(os.getenv("AIOKAFKA_MAX_REQUEST_SIZE", 10 * 1024 * 1024))

# ---------- ONNX FER+ ----------
EMOTION_ONNX_PATH = os.getenv("EMOTION_ONNX_PATH", "/models/emotion-ferplus.onnx")
EMOTION_LABELS = ["neutral","happiness","surprise","sadness","anger","disgust","fear","contempt"]

def softmax(x: np.ndarray) -> np.ndarray:
    x = x.astype(np.float32)
    x = x - np.max(x)
    e = np.exp(x)
    s = e.sum()
    return e / (s + 1e-12)

onnx_sess = None
try:
    import onnxruntime as ort
    sess_opts = ort.SessionOptions()
    sess_opts.intra_op_num_threads = 1
    sess_opts.inter_op_num_threads = 1
    onnx_sess = ort.InferenceSession(
        EMOTION_ONNX_PATH, sess_options=sess_opts, providers=["CPUExecutionProvider"]
    )
    log.info("ONNX FER+ loaded: %s", EMOTION_ONNX_PATH)
except Exception as e:
    log.warning("ONNX FER+ unavailable, fallback ON (reason: %s)", e)

# ---------- декодирование входа ----------
datauri_prefix = re.compile(br"^data:image/(png|jpeg);base64,", re.IGNORECASE)
b64_charset = set(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r")

def _imdecode(img_bytes: bytes) -> np.ndarray | None:
    arr = np.frombuffer(img_bytes, dtype=np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)

def decode_frame(payload: bytes) -> np.ndarray | None:
    """
    Принимает: raw JPEG/PNG bytes, dataURI base64, чистый base64,
    или JSON {frame|image|data|payload|content: <base64/dataURI>} .
    """
    if not payload:
        return None

    # raw JPEG/PNG
    if payload.startswith(b"\xff\xd8") or payload.startswith(b"\x89PNG"):
        return _imdecode(payload)

    # JSON?
    if payload[:1] == b"{":
        try:
            obj = orjson.loads(payload)
            b64 = (
                obj.get("frame") or obj.get("image") or obj.get("data")
                or obj.get("payload") or obj.get("content")
            )
            if not b64:
                return None
            if isinstance(b64, str):
                raw = datauri_prefix.sub(b"", b64.encode("utf-8", "ignore"))
            elif isinstance(b64, bytes):
                raw = datauri_prefix.sub(b"", b64)
            else:
                return None
            img_bytes = base64.b64decode(raw, validate=False)
            return _imdecode(img_bytes)
        except Exception as e:
            log.warning("JSON parse failed, try as base64: %s", e)

    # dataURI / чистый base64
    if payload.startswith(b"data:image/") or all(c in b64_charset for c in payload[: min(64, len(payload))]):
        raw = datauri_prefix.sub(b"", payload)
        try:
            img_bytes = base64.b64decode(raw, validate=False)
            return _imdecode(img_bytes)
        except Exception as e:
            log.warning("Base64 decode failed: %s", e)

    return None

# ---------- инференс ----------
def predict_emotion(bgr_img: np.ndarray) -> dict:
    if bgr_img is None or onnx_sess is None:
        return {"unknown": 1.0}
    try:
        # FER+: на вход 1x1x64x64 (grayscale)
        gray = cv2.cvtColor(bgr_img, cv2.COLOR_BGR2GRAY)
        resized = cv2.resize(gray, (64, 64), interpolation=cv2.INTER_AREA)
        inp = resized.astype(np.float32)[None, None, :, :]  # NCHW
        outputs = onnx_sess.run(None, {onnx_sess.get_inputs()[0].name: inp})
        logits = outputs[0][0]  # (8,)
        probs = softmax(logits)
        return {EMOTION_LABELS[i]: float(probs[i]) for i in range(len(EMOTION_LABELS))}
    except Exception as e:
        log.error("onnx predict error: %s", e)
        return {"unknown": 1.0}

def top1(d: dict[str, float]) -> tuple[str, float]:
    if not d:
        return "unknown", 1.0
    k = max(d, key=d.get)
    return k, float(d[k])

# ---------- основной цикл ----------
async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda d: orjson.dumps(d),
        max_request_size=REQ_MAX,
    )
    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        fetch_max_bytes=FETCH_MAX,
        max_partition_fetch_bytes=FETCH_MAX,
    )
    await producer.start()
    log.info("Producer started -> %s", BOOTSTRAP)
    await consumer.start()
    log.info("Consumer started <- %s (topic=%s)", BOOTSTRAP, TOPIC_IN)

    try:
        async for msg in consumer:
            # session_id кладём в Kafka key на стороне webapp → декодируем здесь
            sid = None
            if msg.key:
                try:
                    sid = msg.key.decode("utf-8", "ignore")
                except Exception:
                    sid = None

            img = decode_frame(msg.value)
            probabilities = predict_emotion(img)
            label, score = top1(probabilities)

            out = {
                "ts": int((msg.timestamp or 0) // 1000),  # время брокера
                "session_id": sid,
                "probabilities": probabilities,
                "top_emotion": label,
                "score": score,
            }
            await producer.send_and_wait(TOPIC_OUT, out)
            log.info("sent -> %s : %s", TOPIC_OUT, out)
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

