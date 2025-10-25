# syntax=docker/dockerfile:1.7

########## 1) Базовый слой с системными lib для OpenCV ##########
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

########## 2) Стадия wheelhouse ##########
FROM base AS wheels
WORKDIR /wheels

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install -U "pip<25" "setuptools<73" wheel

COPY requirements.txt /tmp/requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip \
    pip wheel -r /tmp/requirements.txt -w /wheels

########## 3) Финальный образ ##########
FROM base AS runtime
WORKDIR /app

# зависимости офлайн
COPY --from=wheels /wheels /wheels
COPY requirements.txt ./requirements.txt
RUN pip install --no-index --find-links=/wheels -r requirements.txt

# (опционально) скачивание модели в образ — НЕ нужно, если монтируешь с хоста
# оставляю закомментированным, т.к. ты уже монтируешь /models:ro
# RUN mkdir -p /models && \
#     curl -fsSL --retry 5 https://github.com/onnx/models/raw/main/vision/body_analysis/emotion_ferplus/model/emotion-ferplus-8.onnx \
#       -o /models/emotion-ferplus.onnx

# код приложения
COPY worker.py ./worker.py

# non-root и права
RUN useradd -u 1000 -m appuser && chown -R appuser:appuser /app
USER appuser

# путь к модели задаётся из compose: EMOTION_ONNX_PATH=/models/emotion-ferplus.onnx
CMD ["python", "worker.py"]

