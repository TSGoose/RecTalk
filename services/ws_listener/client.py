# ws_listener.py
import json
import os
import random
import signal
import sys
import threading
import time

import websocket  # pip install websocket-client

# --- Настройки ---
WEBSOCKET_URL = os.getenv("WEBSOCKET_URL", "ws://websocket:8765/ws")
PING_INTERVAL = int(os.getenv("PING_INTERVAL", "20"))  # сек
PING_TIMEOUT  = int(os.getenv("PING_TIMEOUT",  "10"))  # сек
TRACE         = os.getenv("WS_TRACE", "0") == "1"      # включить подробную трассировку

# Глобальный флаг мягкой остановки
stop_event = threading.Event()

def _pretty(payload: str) -> str:
    try:
        return json.dumps(json.loads(payload), ensure_ascii=False, indent=2)
    except Exception:
        return payload

def on_message(ws, message):
    print(f"[WS] msg:\n{_pretty(message)}", flush=True)

def on_error(ws, error):
    print(f"[WS] error: {error}", flush=True)

def on_close(ws, status_code, close_msg):
    print(f"[WS] closed: code={status_code} reason={close_msg}", flush=True)

def on_open(ws):
    print("[WS] connected", flush=True)

def run_forever_with_reconnect(url: str):
    """
    Бесконечный цикл с экспоненциальным бэкоффом и джиттером.
    Пинги поддерживают соединение и помогают быстрее детектить обрывы.
    """
    base = 1.0
    cap = 30.0

    while not stop_event.is_set():
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                # sslopt={"cert_reqs": ssl.CERT_NONE}  # для wss с самоподписанным, если нужно
            )

        except Exception as e:
            print(f"[WS] connect error: {e}", flush=True)

        if stop_event.is_set():
            break

        # Экспоненциальный бэкофф с джиттером
        sleep = min(cap, base * 2)
        jitter = random.uniform(0, sleep / 2)
        delay = sleep + jitter
        base = sleep
        print(f"[WS] reconnect in {delay:.1f}s", flush=True)
        stop_event.wait(delay)

def main():
    # Небуферизованный stdout (на всякий случай поверх PYTHONUNBUFFERED/flush)
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    if TRACE:
        websocket.enableTrace(True)  # покажет ping/pong и кадры

    def _graceful_exit(signum, _frame):
        print(f"[WS] signal {signum}: stopping...", flush=True)
        stop_event.set()

    signal.signal(signal.SIGINT, _graceful_exit)
    signal.signal(signal.SIGTERM, _graceful_exit)

    t = threading.Thread(target=run_forever_with_reconnect, args=(WEBSOCKET_URL,), daemon=True)
    t.start()

    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        stop_event.set()
        t.join(timeout=5)
        print("[WS] stopped", flush=True)
        sys.exit(0)

if __name__ == "__main__":
    main()
