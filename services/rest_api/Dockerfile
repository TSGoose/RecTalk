# ---------- Base ----------
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Устанавливаем системные зависимости (curl — для HEALTHCHECK)
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---------- Deps (wheels) ----------
FROM base AS deps

# Скопируем только список зависимостей для кэшируемой сборки
COPY requirements.txt /app/requirements.txt

# Соберём колёсики для оффлайн-установки на следующем этапе
RUN pip wheel --wheel-dir /wheels -r /app/requirements.txt

# ---------- Runtime ----------
FROM base AS runtime

# Непривилегированный пользователь
RUN useradd -m -u 10001 appuser

# Копируем зависимости и устанавливаем из wheelhouse (быстро и без сети)
COPY --from=deps /wheels /wheels
RUN pip install --no-index --find-links=/wheels /wheels/*

# Копируем исходники приложения
COPY . /app

# Права на проект
RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# Проверка живости (ожидает, что у тебя есть эндпоинт /health)
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -fsS http://localhost:8000/health || exit 1

# Запуск через uvicorn (модуль app.py содержит объект app)
CMD ["uvicorn", "rest_api:app", "--host", "0.0.0.0", "--port", "8010"]
