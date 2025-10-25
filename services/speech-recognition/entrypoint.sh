#!/usr/bin/env bash
set -euo pipefail

: "${VOSK_MODEL_URL:=https://alphacephei.com/vosk/models/vosk-model-small-ru-0.22.zip}"
MODEL_DIR="/app/model"

# Если модель уже есть (любой непустой каталог), просто запускаем воркер
if [ -d "$MODEL_DIR" ] && [ -n "$(ls -A "$MODEL_DIR" 2>/dev/null || true)" ]; then
  echo "[entrypoint] model already present at $MODEL_DIR"
  exec "$@"
fi

echo "[entrypoint] downloading model: $VOSK_MODEL_URL"
apt-get update && apt-get install -y --no-install-recommends curl unzip rsync \
  && rm -rf /var/lib/apt/lists/*

# Временная папка
TMPDIR="$(mktemp -d /tmp/vosk.XXXXXX)"
cd "$TMPDIR"
curl -L "$VOSK_MODEL_URL" -o model.zip
unzip -q model.zip

# Находим распакованный каталог вида vosk-model-*
FOUND_DIR="$(find . -maxdepth 1 -type d -name 'vosk-model*' | head -n1)"
if [ -z "$FOUND_DIR" ]; then
  echo "[entrypoint] ERROR: unpacked model dir not found"
  exit 1
fi

# Гарантируем наличие mountpoint и копируем содержимое (не сам каталог)
mkdir -p "$MODEL_DIR"
# Очистим содержимое mountpoint корректно, не удаляя сам каталог
# (скрытые файлы тоже)
shopt -s dotglob nullglob
rm -rf "$MODEL_DIR"/* || true

# Копируем модель внутрь
rsync -a "$FOUND_DIR"/ "$MODEL_DIR"/

echo "[entrypoint] model ready at $MODEL_DIR"
exec "$@"

