import os, asyncio, base64, re, cv2, numpy as np, orjson, logging, time
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

# ---------- Модель эмоций (ONNX FER+) ----------
EMOTION_ONNX_PATH = os.getenv("EMOTION_ONNX_PATH", "/models/emotion-ferplus.onnx")
EMOTION_LABELS = ["neutral","happiness","surprise","sadness","anger","disgust","fear","contempt"]

# ---------- Детектор лица (OpenCV DNN, Caffe) ----------
FACE_PROTO = os.getenv("FACE_PROTO", "/models/face/deploy.prototxt")
FACE_MODEL = os.getenv("FACE_MODEL", "/models/face/res10_300x300_ssd_iter_140000.caffemodel")
FACE_DET_T = float(os.getenv("FACE_DET_T", "0.6"))   # порог детекции

# ---------- Улучшение качества ----------
EMO_UNKNOWN_T = float(os.getenv("EMO_UNKNOWN_T", "0.45"))   # если max_prob < T → unknown
EMO_EMA_ALPHA = float(os.getenv("EMO_EMA_ALPHA", "0.6"))    # EMA сглаживание (0..1)
EMO_MAX_DIM   = int(os.getenv("EMO_MAX_DIM", "256"))        # max размер стороны перед обработкой

# ---------- Инициализация ONNX ----------
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

# ---------- Инициализация детектора лица ----------
face_net = None
if os.path.exists(FACE_PROTO) and os.path.exists(FACE_MODEL):
    try:
        face_net = cv2.dnn.readNetFromCaffe(FACE_PROTO, FACE_MODEL)
        log.info("Face DNN loaded: proto=%s model=%s", FACE_PROTO, FACE_MODEL)
    except Exception as e:
        log.warning("Face DNN unavailable (reason: %s)", e)
else:
    log.warning("Face DNN files not found (proto=%s, model=%s)", FACE_PROTO, FACE_MODEL)

# ---------- декодирование входа ----------
datauri_prefix = re.compile(br"^data:image/(png|jpeg|jpg);base64,", re.IGNORECASE)
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

# ---------- препроцессинг: даунскейл → crop face → gray+CLAHE → 64x64 ----------
def _crop_face_bgr(bgr: np.ndarray) -> np.ndarray:
    if face_net is None or bgr is None:
        return bgr
    h, w = bgr.shape[:2]
    blob = cv2.dnn.blobFromImage(cv2.resize(bgr, (300, 300)), 1.0, (300, 300),
                                 (104.0, 177.0, 123.0), swapRB=False, crop=False)
    try:
        face_net.setInput(blob)
        det = face_net.forward()
    except Exception:
        return bgr

    best = None
    best_area = 0
    for i in range(det.shape[2]):
        conf = float(det[0, 0, i, 2])
        if conf < FACE_DET_T:
            continue
        box = det[0, 0, i, 3:7] * np.array([w, h, w, h])
        x1, y1, x2, y2 = box.astype(int)
        x1, y1 = max(0, x1), max(0, y1)
        x2, y2 = min(w-1, x2), min(h-1, y2)
        area = max(0, x2 - x1) * max(0, y2 - y1)
        if area > best_area:
            best_area = area
            best = (x1, y1, x2, y2)
    if best is None:
        return bgr
    x1, y1, x2, y2 = best
    face = bgr[y1:y2, x1:x2]
    return face if face.size > 0 else bgr

def _preprocess_for_fer(bgr: np.ndarray) -> np.ndarray | None:
    if bgr is None:
        return None
    # ограничим размер
    h, w = bgr.shape[:2]
    m = max(h, w)
    if m > EMO_MAX_DIM:
        r = EMO_MAX_DIM / float(m)
        bgr = cv2.resize(bgr, (int(w*r), int(h*r)), interpolation=cv2.INTER_AREA)

    # детекция и кроп лица
    bgr = _crop_face_bgr(bgr)

    # препроцесс: gray + CLAHE + лёгкий блюр + resize 64x64
    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
    gray = clahe.apply(gray)
    gray = cv2.GaussianBlur(gray, (3,3), 0)

    resized = cv2.resize(gray, (64, 64), interpolation=cv2.INTER_AREA)
    inp = resized.astype(np.float32)[None, None, :, :]  # NCHW
    return inp

def softmax(x: np.ndarray) -> np.ndarray:
    x = x.astype(np.float32)
    x = x - np.max(x)
    e = np.exp(x)
    s = e.sum()
    return e / (s + 1e-12)

# EMA состояние по сессиям
_ema: dict[str, np.ndarray] = {}

def ema_update(session_id: str | None, probs_vec: np.ndarray) -> np.ndarray:
    if session_id is None:
        return probs_vec
    prev = _ema.get(session_id)
    if prev is None:
        _ema[session_id] = probs_vec
        return probs_vec
    newp = EMO_EMA_ALPHA * probs_vec + (1.0 - EMO_EMA_ALPHA) * prev
    _ema[session_id] = newp
    return newp

def predict_emotion(bgr_img: np.ndarray) -> dict:
    if bgr_img is None or onnx_sess is None:
        return {"unknown": 1.0}
    try:
        inp = _preprocess_for_fer(bgr_img)
        if inp is None:
            return {"unknown": 1.0}
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
            # session_id кладёт webapp в key сообщения
            sid = None
            if msg.key:
                try:
                    sid = msg.key.decode("utf-8", "ignore")
                except Exception:
                    sid = None

            img = decode_frame(msg.value)
            probs = predict_emotion(img)

            # вектор вероятностей в порядке меток
            vec = np.array([probs.get(k, 0.0) for k in EMOTION_LABELS], dtype=np.float32)
            vec = ema_update(sid, vec)
            probs = {EMOTION_LABELS[i]: float(vec[i]) for i in range(len(EMOTION_LABELS))}

            label, score = top1(probs)
            if score < EMO_UNKNOWN_T:
                label, score = "unknown", 1.0

            out = {
                "ts": int((msg.timestamp or 0) // 1000),
                "session_id": sid,
                "probabilities": probs,
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

