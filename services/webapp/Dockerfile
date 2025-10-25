# syntax=docker/dockerfile:1.7

########## 1) Базовый слой ##########
FROM python:3.11-slim AS base

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# build-essential нужен для части питон-зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

########## 2) Wheelhouse для зависимостей ##########
FROM base AS wheels
WORKDIR /wheels

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install -U pip setuptools wheel

COPY requirements.txt /tmp/requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip \
    pip wheel -r /tmp/requirements.txt -w /wheels

########## 3) Финальный образ ##########
FROM base AS runtime
WORKDIR /app

COPY --from=wheels /wheels /wheels
COPY requirements.txt ./requirements.txt
RUN pip install --no-index --find-links=/wheels -r requirements.txt

# код приложения кладём после установки зависимостей
COPY app ./app

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

