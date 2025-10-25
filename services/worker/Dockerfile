# syntax=docker/dockerfile:1.7

########## 1) База ##########
FROM python:3.11-slim AS base

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    OPENCV_LOG_LEVEL=ERROR

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates \
      libgomp1 \
      libgl1 libglib2.0-0 libsm6 libxext6 libxrender1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

########## 2) Wheels ##########
FROM base AS wheels
WORKDIR /wheels

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install -U "pip<25" "setuptools<73" wheel

COPY requirements.txt /tmp/requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip \
    pip wheel -r /tmp/requirements.txt -w /wheels

########## 3) Runtime ##########
FROM base AS runtime
WORKDIR /app

# deps офлайн
COPY --from=wheels /wheels /wheels
COPY requirements.txt ./requirements.txt
RUN pip install --no-index --find-links=/wheels -r requirements.txt

# ===== СКАЧАТЬ МОДЕЛИ =====
# 1) Emotion FER+ (ONNX)
RUN mkdir -p /models && set -e; \
    URL1="https://github.com/onnx/models/raw/main/vision/body_analysis/emotion_ferplus/model/emotion-ferplus-8.onnx"; \
    URL2="https://huggingface.co/onnxmodelzoo/emotion-ferplus-8/resolve/main/emotion-ferplus-8.onnx"; \
    echo "Downloading FER+ from $URL1"; \
    if ! curl -fsSL --retry 5 "$URL1" -o /models/emotion-ferplus.onnx; then \
        echo "Primary failed, fallback $URL2"; \
        curl -fsSL --retry 5 "$URL2" -o /models/emotion-ferplus.onnx; \
    fi; \
    test $(stat -c%s /models/emotion-ferplus.onnx) -gt 100000; \
    head -c 1 /models/emotion-ferplus.onnx | grep -qv '<'

# 2) OpenCV DNN face detector (Res10 SSD Caffe)
RUN mkdir -p /models/face && set -e; \
    P1="https://raw.githubusercontent.com/opencv/opencv/master/samples/dnn/face_detector/deploy.prototxt"; \
    P2="https://raw.githubusercontent.com/opencv/opencv/4.x/samples/dnn/face_detector/deploy.prototxt"; \
    M1="https://github.com/opencv/opencv_3rdparty/raw/dnn_samples_face_detector_20170830/res10_300x300_ssd_iter_140000.caffemodel"; \
    M2="https://raw.githubusercontent.com/opencv/opencv_3rdparty/dnn_samples_face_detector_20170830/res10_300x300_ssd_iter_140000.caffemodel"; \
    echo "Downloading face prototxt"; \
    (curl -fsSL --retry 5 "$P1" -o /models/face/deploy.prototxt || \
     curl -fsSL --retry 5 "$P2" -o /models/face/deploy.prototxt); \
    echo "Downloading face caffemodel"; \
    (curl -fsSL --retry 5 "$M1" -o /models/face/res10_300x300_ssd_iter_140000.caffemodel || \
     curl -fsSL --retry 5 "$M2" -o /models/face/res10_300x300_ssd_iter_140000.caffemodel); \
    test $(stat -c%s /models/face/res10_300x300_ssd_iter_140000.caffemodel) -gt 2000000; \
    head -c 1 /models/face/res10_300x300_ssd_iter_140000.caffemodel | grep -qv '<'

# код
COPY worker.py ./worker.py

# non-root
RUN useradd -u 1000 -m appuser && chown -R appuser:appuser /app /models
USER appuser

# пути к моделям
ENV EMOTION_ONNX_PATH=/models/emotion-ferplus.onnx \
    FACE_PROTO=/models/face/deploy.prototxt \
    FACE_MODEL=/models/face/res10_300x300_ssd_iter_140000.caffemodel

CMD ["python", "worker.py"]

