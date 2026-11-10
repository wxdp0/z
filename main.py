import threading
from concurrent.futures import ThreadPoolExecutor
import requests
import json
import time
import random
import re
import base64
import os
import socket
from datetime import datetime

# ==================== ĐỌC CONFIG ====================
_CONFIG_FILE = "config.json"

def _load_config() -> dict:
    if not os.path.exists(_CONFIG_FILE):
        default = {
            "TOKEN_A":   "token_acc_stream", # đổi ở files config.json
            "TOKEN_B":   "token_acc_view", # đổi ở files config.json
            "GUILD_ID":  "điền_id_sever_vào_đay", # đổi ở files config.json
            "CHANNEL_ID":"điền_id_kênh_voice_vào_đay" # đổi ở files config.json
        }
        with open(_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(default, f, indent=2, ensure_ascii=False)
        print(f"[!] Chưa có {_CONFIG_FILE} — đã tạo file mẫu.")
        print(f"[!] Mở {_CONFIG_FILE} điền token và ID rồi chạy lại.")
        exit(0)
    with open(_CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)

_CFG       = _load_config()
TOKEN_A    = _CFG.get("TOKEN_A",    "")
TOKEN_B    = _CFG.get("TOKEN_B",    "")
GUILD_ID   = _CFG.get("GUILD_ID",   "")
CHANNEL_ID = _CFG.get("CHANNEL_ID", "")

# ==================== CẤU HÌNH ====================
MAX_HOURS = 5000   # auto stop

# Nút bấm presence
BUTTON_1_LABEL = "cách treo"
BUTTON_1_URL   = "https://www.tiktok.com/@minhtuyen523?_r=1&_t=ZS-9BAkxhiMJYS"
BUTTON_2_LABEL = "join Server"
BUTTON_2_URL   = "https://discord.gg/A4Uxfzp9q"

STREAM_DETAILS = "Sharing screen"
STREAM_STATE   = "In voice channel"

MAX_RECONNECT_ATTEMPTS = 5
MAX_REJOIN_ATTEMPTS    = 3
# =====================================================================

DISCORD_API = "https://discord.com/api/v9"
GATEWAY_URL = "wss://gateway.discord.gg/?v=9&encoding=json"

_POOL = ThreadPoolExecutor(max_workers=20, thread_name_prefix="sbbot")
_STOP_EVENT = threading.Event()

# ==================== BUILD NUMBER ====================
def fetch_build_number() -> int:
    FALLBACK = 504649
    try:
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/128.0.0.0 Safari/537.36"
        r  = requests.get("https://discord.com/app", headers={"User-Agent": ua}, timeout=15)
        if r.status_code != 200: return FALLBACK
        for h in re.findall(r'/assets/([a-f0-9]+)\.js', r.text)[-5:]:
            try:
                ar = requests.get(f"https://discord.com/assets/{h}.js",
                                  headers={"User-Agent": ua}, timeout=15)
                m  = re.search(r'buildNumber["\s:]+["\s]*(\d{5,7})', ar.text)
                if m: return int(m.group(1))
            except: continue
        return FALLBACK
    except: return FALLBACK

def make_super_props(bn: int) -> str:
    return base64.b64encode(json.dumps({
        "os": "Android", "browser": "Discord VR", "device": "eureka",
        "release_channel": "stable", "client_version": "1.0.9175",
        "os_version": "10.0.26100", "os_arch": "x64", "app_arch": "x64",
        "system_locale": "en-US",
        "browser_user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "discord/1.0.9175 Chrome/128.0.6613.186 Electron/32.2.7 Safari/537.36"
        ),
        "browser_version": "32.2.7",
        "client_build_number": bn,
        "native_build_number": 59498,
        "client_event_source": None,
    }).encode()).decode()

print("[*] Đang lấy build number...")
_BUILD_NUMBER = fetch_build_number()
_SUPER_PROPS  = make_super_props(_BUILD_NUMBER)
print(f"[*] Build: {_BUILD_NUMBER}")

_HEADERS_BASE = {
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "discord/1.0.9175 Chrome/128.0.6613.186 Electron/32.2.7 Safari/537.36"
    ),
    "X-Super-Properties": _SUPER_PROPS,
    "X-Discord-Locale":   "en-US",
}

# ==================== VALIDATOR ====================
def validate_token(token: str, guild_id: str) -> dict:
    h = {**_HEADERS_BASE, "Authorization": token}
    try:
        r = requests.get(f"{DISCORD_API}/users/@me", headers=h, timeout=15)
        if r.status_code == 401: return {"ok": False, "reason": "invalid"}
        if r.status_code != 200: return {"ok": False, "reason": "error"}
        user_data = r.json()
        rg = requests.get(f"{DISCORD_API}/users/@me/guilds", headers=h, timeout=15)
        if rg.status_code == 200:
            if guild_id not in [g["id"] for g in rg.json()]:
                return {"ok": False, "reason": "not_in_guild", "user": user_data}
        return {"ok": True, "user": user_data}
    except: return {"ok": False, "reason": "error"}

def check_token_alive(token: str) -> bool:
    try:
        r = requests.get(f"{DISCORD_API}/users/@me",
            headers={"Authorization": token, "User-Agent": "Mozilla/5.0"}, timeout=10)
        return r.status_code == 200
    except: return False

# ==================== WEBSOCKET HELPER ====================
def _create_ws(url: str):
    import websocket as ws_lib
    ws = ws_lib.WebSocket(skip_utf8_validation=True)
    ws.connect(url, timeout=90, ping_interval=0, ping_timeout=0)
    try:
        s = ws.sock
        if s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            try:
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE,  5)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 3)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT,   5)
            except (AttributeError, OSError): pass
    except (AttributeError, OSError): pass
    return ws

# ==================== GATEWAY CLIENT ====================
class GatewayClient:
    def __init__(self, token: str, role: str):
        self.token          = token
        self.role           = role
        self.ws             = None
        self.seq            = None
        self.session_id     = None
        self.user_id        = None
        self.username       = ""
        self.hb_interval    = 41250
        self.voice_joined   = False
        self._stream_key    = ""
        self._lock          = threading.Lock()
        self._stop_event    = threading.Event()
        self.on_token_dead  = None
        self.on_voice_left  = None

    @property
    def is_running(self): return not self._stop_event.is_set()

    def _send(self, data: dict):
        with self._lock:
            if self.ws:
                try: self.ws.send(json.dumps(data))
                except: pass

    def _ws_ping_loop(self, ws_ref):
        while not self._stop_event.is_set():
            self._stop_event.wait(10)
            if self._stop_event.is_set(): break
            with self._lock:
                if self.ws is not ws_ref: break
                try: ws_ref.ping()
                except: break

    def _identify(self):
        init_activities = []
        if self.role == "streamer":
            init_activities = [{
                "name":           "Tuyendev",
                "type":           0,
                "application_id": "411916947773587456",
                "timestamps":     {"start": int(time.time() * 1000)},
                "details":        "treo stream",
                "state":          "tích giờ badge Discord",
                "assets": {
                    "large_image": "mp:external/JBc-5VFiTBfBNIBh-xGvf2lUGJi-b1O2zxEzXb1WXKM/https/i.imgur.com/Dqy5Dxo.png",
                    "large_text":  "Tuyendev",
                    "small_image": "mp:external/JBc-5VFiTBfBNIBh-xGvf2lUGJi-b1O2zxEzXb1WXKM/https/i.imgur.com/Dqy5Dxo.png",
                    "small_text":  "stream badge",
                },
            }]
        self._send({
            "op": 2,
            "d": {
                "token": self.token,
                "capabilities": 16381,
                "properties": {
                    "os":                  "Android",
                    "browser":             "Discord VR",   # hiển thị VR
                    "device":              "eureka",        # Meta Quest 3
                    "browser_user_agent":  "",
                    "browser_version":     "",
                    "os_version":          "",
                    "referrer":            "",
                    "referring_domain":    "",
                    "release_channel":     "stable",
                    "client_build_number": _BUILD_NUMBER,
                    "client_event_source": None,
                },
                "presence": {
                    "status":     "online",
                    "since":      0,
                    "activities": init_activities,
                    "afk":        False,
                },
                "compress": False,
            },
        })

    def _join_voice(self):
        is_s = self.role == "streamer"
        self._send({
            "op": 4,
            "d": {
                "guild_id":    GUILD_ID,
                "channel_id":  CHANNEL_ID,
                "self_mute":   not is_s,
                "self_deaf":   not is_s,
                "self_video":  is_s,
                "self_stream": is_s,
            },
        })

    def _send_stream_signal(self):
        self._send({
            "op": 18,
            "d": {
                "type": "guild",
                "guild_id": GUILD_ID,
                "channel_id": CHANNEL_ID,
                "preferred_region": "us-east",
            },
        })

    def _send_stream_presence(self, start_ts: int):
        buttons, button_urls = [], []
        if BUTTON_1_LABEL and BUTTON_1_URL:
            buttons.append(BUTTON_1_LABEL); button_urls.append(BUTTON_1_URL)
        if BUTTON_2_LABEL and BUTTON_2_URL:
            buttons.append(BUTTON_2_LABEL); button_urls.append(BUTTON_2_URL)

        act_stream: dict = {
            "name": "Screen Share",
            "type": 1,
            "url": "https://www.twitch.tv/discord",
            "application_id": "383226320970055681",
            "timestamps": {"start": start_ts},
            "details": STREAM_DETAILS,
            "state": STREAM_STATE,
        }

        act_playing: dict = {
            "name":           "Tuyendev",
            "type":           0,
            "application_id": "411916947773587456",
            "timestamps":     {"start": start_ts},
            "details":        "treo stream",
            "state":          "tích giờ badge Discord",
            "assets": {
                "large_image": "mp:external/JBc-5VFiTBfBNIBh-xGvf2lUGJi-b1O2zxEzXb1WXKM/https/i.imgur.com/Dqy5Dxo.png",
                "large_text":  "Tuyendev",
                "small_image": "mp:external/JBc-5VFiTBfBNIBh-xGvf2lUGJi-b1O2zxEzXb1WXKM/https/i.imgur.com/Dqy5Dxo.png",
                "small_text":  "stream badge",
            },
        }
        if buttons:
            act_playing["buttons"]  = buttons
            act_playing["metadata"] = {"button_urls": button_urls}

        self._send({
            "op": 3,
            "d": {
                "since": 0, "status": "online", "afk": False,
                "activities": [act_stream, act_playing],
            },
        })

    def _heartbeat_loop(self):
        while not self._stop_event.is_set():
            self._stop_event.wait(self.hb_interval / 1000)
            if self._stop_event.is_set(): break
            self._send({"op": 1, "d": self.seq})

    def _voice_keepalive_loop(self, start_ts: int):
        while not self._stop_event.is_set():
            self._stop_event.wait(55)
            if self._stop_event.is_set(): break
            if self.voice_joined:
                self._send_stream_presence(start_ts)
                self._send({
                    "op": 4,
                    "d": {
                        "guild_id":    GUILD_ID,
                        "channel_id":  CHANNEL_ID,
                        "self_mute":   False, "self_deaf": False,
                        "self_video":  True,  "self_stream": True,
                    },
                })

    def _stream_keepalive_loop(self):
        while not self._stop_event.is_set():
            self._stop_event.wait(25)
            if self._stop_event.is_set(): break
            if self.voice_joined:
                self._send_stream_signal()

    def _fire_dead(self, cb_name: str):
        if self._stop_event.is_set(): return
        self._stop_event.set()
        with self._lock:
            try:
                if self.ws: self.ws.close()
            except: pass
        cb = getattr(self, cb_name, None)
        if cb:
            try: cb()
            except: pass

    def run(self, start_ts: int):
        self._stop_event.clear()
        consec_fail    = 0
        got_ready_once = False

        while not self._stop_event.is_set():
            if consec_fail >= MAX_RECONNECT_ATTEMPTS:
                print(f"[{self.role}] {consec_fail} lần liên tiếp không lên READY → stop")
                self._fire_dead("on_token_dead")
                return

            stream_sent = False; keepalive_sent = False
            rejoin_count = 0; ws = None
            token_dead_in_finally = False

            try:
                ws = _create_ws(GATEWAY_URL)
                with self._lock: self.ws = ws

                hello = json.loads(ws.recv())
                self.hb_interval = hello["d"]["heartbeat_interval"]
                _POOL.submit(self._heartbeat_loop)
                _POOL.submit(self._ws_ping_loop, ws)
                self._identify()
                self.voice_joined = False

                for raw in ws:
                    if self._stop_event.is_set(): break
                    try: msg = json.loads(raw)
                    except: continue

                    op = msg.get("op"); t = msg.get("t")
                    d  = msg.get("d") or {}; s = msg.get("s")
                    if s: self.seq = s

                    if t == "READY":
                        self.session_id = d.get("session_id")
                        u = d.get("user", {})
                        self.user_id  = u.get("id", self.user_id)
                        self.username = u.get("username", self.username)
                        print(f"[{self.role}] READY @{self.username}")
                        self._stop_event.wait(random.uniform(1.5, 3.0))
                        self._join_voice()
                        consec_fail    = 0
                        got_ready_once = True

                    elif t == "VOICE_STATE_UPDATE":
                        uid = d.get("user_id", "")
                        cid = d.get("channel_id", "")
                        if uid == self.user_id:
                            if cid == CHANNEL_ID:
                                if not self.voice_joined:
                                    self.voice_joined = True
                                    rejoin_count = 0
                                    print(f"[{self.role}] Voice joined ✅")
                                    if self.role == "streamer" and not stream_sent:
                                        stream_sent = True
                                        def _do_stream():
                                            self._stop_event.wait(2)
                                            if self._stop_event.is_set(): return
                                            self._send_stream_signal()
                                            self._stop_event.wait(1)
                                            if self._stop_event.is_set(): return
                                            self._send_stream_presence(start_ts)
                                        _POOL.submit(_do_stream)
                                    if self.role == "streamer" and not keepalive_sent:
                                        keepalive_sent = True
                                        _POOL.submit(self._voice_keepalive_loop, start_ts)
                                        _POOL.submit(self._stream_keepalive_loop)
                            else:
                                if self.voice_joined:
                                    self.voice_joined = False
                                    rejoin_count += 1
                                    print(f"[{self.role}] Left → rejoin ({rejoin_count}/{MAX_REJOIN_ATTEMPTS})")
                                    if rejoin_count >= MAX_REJOIN_ATTEMPTS:
                                        self._fire_dead("on_voice_left"); return
                                    if not self._stop_event.is_set():
                                        self._stop_event.wait(random.uniform(2, 4))
                                        self._join_voice()

                    elif t == "STREAM_CREATE":
                        self._stream_key = d.get("stream_key", "")
                        print(f"[{self.role}] STREAM_CREATE ✅")

                    elif t == "STREAM_DELETE":
                        if self.role == "streamer" and self.voice_joined:
                            stream_sent = False; keepalive_sent = False
                            def _full_rejoin():
                                self._send({"op": 4, "d": {
                                    "guild_id": GUILD_ID, "channel_id": None,
                                    "self_mute": False, "self_deaf": False,
                                    "self_video": False, "self_stream": False,
                                }})
                                self._stop_event.wait(2)
                                if self._stop_event.is_set(): return
                                self._send({"op": 4, "d": {
                                    "guild_id": GUILD_ID, "channel_id": CHANNEL_ID,
                                    "self_mute": False, "self_deaf": False,
                                    "self_video": True, "self_stream": True,
                                }})
                                self._stop_event.wait(2)
                                if self._stop_event.is_set(): return
                                self._send_stream_signal()
                                print("[streamer] STREAM_DELETE rejoin ✅")
                            _POOL.submit(_full_rejoin)

                    elif t == "RESUMED":
                        self.voice_joined = False
                        self._stop_event.wait(1)
                        self._join_voice()

                    elif op == 7: break
                    elif op == 9:
                        self._stop_event.wait(random.uniform(3, 6)); break

            except Exception as ex:
                if not self._stop_event.is_set():
                    err = str(ex)
                    if any(c in err for c in ["4004", "4005", "4003", "4013"]):
                        print(f"[{self.role}] Token die ({err}) → stop")
                        self._fire_dead("on_token_dead"); return
                    if got_ready_once:
                        consec_fail = 0
                        print(f"[{self.role}] Drop mạng tạm: {ex}")
                    else:
                        consec_fail += 1
                        print(f"[{self.role}] Drop consec={consec_fail}/{MAX_RECONNECT_ATTEMPTS}: {ex}")

            finally:
                self.voice_joined = False
                try:
                    if ws and hasattr(ws, "close_code"):
                        cc = ws.close_code
                        if cc in (4004, 4005, 4003, 4013) and not self._stop_event.is_set():
                            print(f"[{self.role}] Token die close_code={cc}")
                            self._fire_dead("on_token_dead")
                            token_dead_in_finally = True
                except: pass
                with self._lock:
                    if self.ws is ws: self.ws = None
                try:
                    if ws: ws.close()
                except: pass

            if token_dead_in_finally: return
            if not self._stop_event.is_set():
                wait = random.uniform(3, 8)
                print(f"[{self.role}] Reconnect in {wait:.1f}s...")
                self._stop_event.wait(wait)

    def stop(self):
        self._stop_event.set()
        with self._lock:
            try:
                if self.ws: self.ws.close()
            except: pass


# ==================== MAIN RUNNER ====================
def format_duration(s: int) -> str:
    h = s // 3600; m = (s % 3600) // 60; sc = s % 60
    return f"{h}h {m:02d}m {sc:02d}s" if h else f"{m:02d}m {sc:02d}s"

def run():
    print("=" * 50)
    print("  Stream Badge Bot — standalone by @tuyen")
    print("=" * 50)

    # Validate
    print("[*] Đang kiểm tra Token A...")
    va = validate_token(TOKEN_A, GUILD_ID)
    if not va["ok"]:
        reason_map = {
            "invalid":      "Token A không hợp lệ hoặc đã hết hạn.",
            "not_in_guild": f"Token A chưa vào server {GUILD_ID}.",
            "error":        "Không thể kết nối để kiểm tra Token A.",
        }
        print(f"[!] {reason_map.get(va['reason'], 'Lỗi Token A')}")
        return
    username_a = va["user"].get("username", "?")
    print(f"[✓] Token A OK → @{username_a}")

    print("[*] Đang kiểm tra Token B...")
    vb = validate_token(TOKEN_B, GUILD_ID)
    if not vb["ok"]:
        reason_map = {
            "invalid":      "Token B không hợp lệ hoặc đã hết hạn.",
            "not_in_guild": f"Token B chưa vào server {GUILD_ID}.",
            "error":        "Không thể kết nối để kiểm tra Token B.",
        }
        print(f"[!] {reason_map.get(vb['reason'], 'Lỗi Token B')}")
        return
    username_b = vb["user"].get("username", "?")
    print(f"[✓] Token B OK → @{username_b}")

    start_time = time.time()
    start_str  = datetime.now().strftime("%H:%M %d/%m/%Y")
    start_ts   = int(start_time * 1000)

    print(f"\n[*] Bắt đầu: {start_str}")
    print(f"[*] Streamer: @{username_a}")
    print(f"[*] Viewer:   @{username_b}")
    print(f"[*] Kênh:     {CHANNEL_ID}")
    print(f"[*] Auto stop sau: {MAX_HOURS}h")
    print("[*] Nhấn Ctrl+C để dừng thủ công\n")

    streamer = GatewayClient(TOKEN_A, "streamer")
    viewer   = GatewayClient(TOKEN_B, "viewer")

    stop_reason = [""]

    def _on_streamer_dead():
        stop_reason[0] = f"Token A (@{username_a}) bị vô hiệu hóa"
        print(f"[!] {stop_reason[0]}")
        viewer.stop(); _STOP_EVENT.set()

    def _on_viewer_dead():
        stop_reason[0] = f"Token B (@{username_b}) bị vô hiệu hóa"
        print(f"[!] {stop_reason[0]}")
        streamer.stop(); _STOP_EVENT.set()

    def _on_streamer_voice_left():
        stop_reason[0] = f"Streamer (@{username_a}) rời voice channel"
        print(f"[!] {stop_reason[0]}")
        viewer.stop(); _STOP_EVENT.set()

    def _on_viewer_voice_left():
        stop_reason[0] = f"Viewer (@{username_b}) rời voice channel"
        print(f"[!] {stop_reason[0]}")
        streamer.stop(); _STOP_EVENT.set()

    streamer.on_token_dead = _on_streamer_dead
    viewer.on_token_dead   = _on_viewer_dead
    streamer.on_voice_left = _on_streamer_voice_left
    viewer.on_voice_left   = _on_viewer_voice_left

    _POOL.submit(streamer.run, start_ts)

    for _ in range(40):
        if streamer.voice_joined: break
        if _STOP_EVENT.wait(1): break
    else:
        print("[~] Streamer chưa vào voice sau 40s — vẫn tiếp tục")

    if not _STOP_EVENT.is_set():
        _POOL.submit(viewer.run, start_ts)

    def _token_watcher():
        while not _STOP_EVENT.is_set():
            _STOP_EVENT.wait(300)
            if _STOP_EVENT.is_set(): break
            if not check_token_alive(TOKEN_A):
                _on_streamer_dead(); return
            if not check_token_alive(TOKEN_B):
                _on_viewer_dead(); return

    _POOL.submit(_token_watcher)

    def _auto_stop():
        while not _STOP_EVENT.is_set():
            _STOP_EVENT.wait(60)
            if _STOP_EVENT.is_set(): break
            elapsed_h = (time.time() - start_time) / 3600
            if elapsed_h >= MAX_HOURS:
                stop_reason[0] = f"Hệ thống (đủ {MAX_HOURS}h)"
                print(f"[*] Đủ {MAX_HOURS}h — auto stop")
                streamer.stop(); viewer.stop(); _STOP_EVENT.set()

    _POOL.submit(_auto_stop)

    def _status_printer():
        while not _STOP_EVENT.is_set():
            _STOP_EVENT.wait(600)
            if _STOP_EVENT.is_set(): break
            elapsed = int(time.time() - start_time)
            s_ok = "🟢" if streamer.voice_joined else "🟡"
            v_ok = "🟢" if viewer.voice_joined   else "🟡"
            print(f"[Status] {format_duration(elapsed)} | "
                  f"Streamer {s_ok} | Viewer {v_ok} | "
                  f"{elapsed/3600:.2f}h / {MAX_HOURS}h")

    _POOL.submit(_status_printer)

    try:
        _STOP_EVENT.wait()
    except KeyboardInterrupt:
        stop_reason[0] = "Ctrl+C (thủ công)"
        print(f"\n[*] Dừng thủ công")

    streamer.stop(); viewer.stop()
    _STOP_EVENT.set()

    elapsed = int(time.time() - start_time)
    end_str = datetime.now().strftime("%H:%M %d/%m/%Y")

    print("\n" + "=" * 50)
    print("  KẾT THÚC SESSION")
    print("=" * 50)
    print(f"  Streamer:    @{username_a}")
    print(f"  Viewer:      @{username_b}")
    print(f"  Bắt đầu:    {start_str}")
    print(f"  Kết thúc:   {end_str}")
    print(f"  Thời gian:  {format_duration(elapsed)}")
    print(f"  Giờ tích:   {elapsed/3600:.2f}h")
    if stop_reason[0]:
        print(f"  Lý do dừng: {stop_reason[0]}")
    print("=" * 50)
    print("  Badge Discord cập nhật sau vài phút.")
    print("=" * 50)

if __name__ == "__main__":
    run()
