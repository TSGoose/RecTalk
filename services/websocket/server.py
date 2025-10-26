from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body
from starlette.websockets import WebSocketState
import asyncio, json, re, time
from typing import Dict, List, Tuple

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

# --- СЛОВАРЬ УДАРЕНИЙ ---
ACCENTED = [
    "ув+ек", "ук+ек","ув+екское город+ище","хор+езм","трапез+унд","каш+ин","куб+анкин","к+ашникова",
    "раск+оп","археол+огия","рубр+ук","+ибн бат+ута","ладжвард+ина","мин+аи","л+юстр",
    "визант+ия","м+арганца","сграфф+ито",
]
def _unplus(s: str) -> str: return s.replace("+", "")
_PHRASES = [s for s in ACCENTED if " " in s or "-" in s]
_SINGLE  = [s for s in ACCENTED if s not in _PHRASES]
_PHRASE_MAP: Dict[str,str] = {_unplus(s).lower(): s for s in _PHRASES}
_SINGLE_MAP: Dict[str,str] = {_unplus(s).lower(): s for s in _SINGLE}
_PHRASE_ITEMS: List[Tuple[re.Pattern,str]] = [
    (re.compile(re.escape(bare), flags=re.IGNORECASE), accented)
    for bare, accented in sorted(_PHRASE_MAP.items(), key=lambda kv: len(kv[0]), reverse=True)
]
_SINGLE_KEYS = sorted(_SINGLE_MAP.keys(), key=len, reverse=True)
_SINGLE_RE = re.compile(r"\b(" + "|".join(map(re.escape, _SINGLE_KEYS)) + r")\b", re.IGNORECASE) if _SINGLE_KEYS else None

def accent_only_from_dictionary(text: str) -> str:
    if not text: return text
    for pattern, accented in _PHRASE_ITEMS:
        text = pattern.sub(accented, text)
    if _SINGLE_RE:
        def _sub(m: re.Match) -> str:
            return _SINGLE_MAP.get(m.group(0).lower(), m.group(0))
        text = _SINGLE_RE.sub(_sub, text)
    return text

def accent_in_json(value):
    if isinstance(value, str):  return accent_only_from_dictionary(value)
    if isinstance(value, list): return [accent_in_json(v) for v in value]
    if isinstance(value, dict): return {k: accent_in_json(v) for k,v in value.items()}
    return value

# --- Менеджер подключений ---
def _is_connected(ws: WebSocket) -> bool:
    return (ws.client_state is WebSocketState.CONNECTED and
            ws.application_state is WebSocketState.CONNECTED)

class ConnectionManager:
    def __init__(self):
        self._active: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._active.add(ws)
        print("[SERVER] Клиент подключен. Всего:", len(self._active))

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self._active.discard(ws)
        try:
            if _is_connected(ws):
                await ws.close()
        except Exception:
            pass
        print("[SERVER] Клиент удалён. Осталось:", len(self._active))

    async def broadcast_text(self, text: str):
        async with self._lock:
            targets = list(self._active)
        dead, sent = [], 0
        for ws in targets:
            try:
                if _is_connected(ws):
                    await ws.send_text(text)
                    sent += 1
                else:
                    dead.append(ws)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.disconnect(ws)
        # print(f"[SERVER] broadcast sent={sent}, cleaned={len(dead)}")

manager = ConnectionManager()

# --- WS endpoint ---
MAX_LEN = 256 * 1024  # ограничение на входящее сообщение

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    try:
        await manager.connect(websocket)
        while True:
            raw = await websocket.receive_text()
            if len(raw) > MAX_LEN:
                continue  # или залогировать/ответить ошибкой
            try:
                parsed = json.loads(raw)
                accented = accent_in_json(parsed)
                outbound = json.dumps(accented, ensure_ascii=False)
            except json.JSONDecodeError:
                outbound = accent_only_from_dictionary(raw)
            except Exception as e:
                print("[SERVER] parse error:", e)
                continue
            await manager.broadcast_text(outbound)
    except WebSocketDisconnect:
        print("[SERVER] Клиент отключился")
    except Exception as e:
        print("[SERVER] Ошибка:", e)
    finally:
        await manager.disconnect(websocket)

# --- HTTP публикация (исправляет 404 /publish) ---
@app.post("/publish")
async def publish(payload: dict | str = Body(...)):
    try:
        if isinstance(payload, str):
            text = accent_only_from_dictionary(payload)
        else:
            text = json.dumps(accent_in_json(payload), ensure_ascii=False)
    except Exception:
        text = str(payload)
    await manager.broadcast_text(text)
    return {"ok": True}

# --- Heartbeat: держит соединения живыми ---
async def _heartbeat():
    while True:
        try:
            await manager.broadcast_text(json.dumps({"type": "ping", "ts": int(time.time())}))
        except Exception as e:
            print("[SERVER] heartbeat error:", e)
        await asyncio.sleep(25)

@app.on_event("startup")
def _on_startup():
    asyncio.create_task(_heartbeat())
