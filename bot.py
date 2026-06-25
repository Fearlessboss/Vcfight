"""
🔥 ZUDO USERBOT — GOD-LEVEL EDITION v5 (ULTRA-INSTANT STREAM + MULTI-FALLBACK)
==================================================================
✦ FIX: "No audio source found on nubcoder..." — multi-fallback stream builder
✦ FIX: VC join slow — parallel peer warmup + pre-join + change_stream pattern
✦ NEW: Multi-client yt-dlp rotation (android/ios/tv/web/mweb) — bypass 403/429
✦ NEW: MediaStream → AudioStream → AudioPiped → raw URL fallback chain
✦ NEW: play() → join_group_call() → stream() method fallback
✦ NEW: Pre-warm VC (assistant joins empty stream first, then switches) = INSTANT
✦ KEEP: OTP via text, 2FA via text, MongoDB persistence, 2 owners, restart
==================================================================
"""

import os
import re
import sys
import time
import asyncio
import logging
import requests
from pathlib import Path
from datetime import datetime
from urllib.parse import quote

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatType
from pyrogram.handlers import MessageHandler
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
)
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid,
    PasswordHashInvalid, FloodWait, UserAlreadyParticipant,
    InviteHashExpired, PeerIdInvalid, AuthKeyUnregistered,
    AuthKeyDuplicated, UserDeactivated, UserDeactivatedBan
)

# ───── PyTgCalls imports (with backward-compat fallbacks) ─────
from pytgcalls import PyTgCalls
try:
    from pytgcalls import StreamType
except Exception:
    StreamType = None

# Try new + old API stream classes
_MediaStream = None
_AudioStream = None
_VideoStream = None
_AudioPiped  = None
_VideoFlags  = None
_HighQualityAudio = None

try:
    from pytgcalls.types import MediaStream as _MediaStream
except Exception:
    pass
try:
    from pytgcalls.types import AudioQuality as _AudioQuality
except Exception:
    _AudioQuality = None
try:
    from pytgcalls.types.input_stream import AudioPiped as _AudioPiped
except Exception:
    pass
try:
    from pytgcalls.types.input_stream import AudioStream as _AudioStream
    from pytgcalls.types.input_stream import VideoStream as _VideoStream
except Exception:
    pass
try:
    from pytgcalls.types.input_stream.quality import HighQualityAudio as _HighQualityAudio
except Exception:
    pass
try:
    from pytgcalls.types.stream import VideoFlags as _VideoFlags
except Exception:
    pass

import yt_dlp
from pymongo import MongoClient

# ═══════════════════════════════════════════════════════════════════════════════
# 🔧 CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
def _get_env(name, default=None, cast=str, required=False):
    val = os.environ.get(name, default)
    if required and (val is None or val == ""):
        print(f"❌ FATAL: Environment variable {name} is required but not set.")
        sys.exit(1)
    if val is None:
        return None
    try:
        return cast(val)
    except Exception:
        return val

BOT_TOKEN   = _get_env("BOT_TOKEN", required=True)
API_ID      = _get_env("API_ID", default="33628258", cast=int)
API_HASH    = _get_env("API_HASH", default="0850762925b9c1715b9b122f7b753128")
MONGO_URL   = _get_env(
    "MONGO_URL",
    default="mongodb+srv://moderatorhelperorg_db_user:nze86usap2dYthZN@cluster0.uokrixs.mongodb.net/mydatabase?retryWrites=true&w=majority"
)
OWNER_ID     = _get_env("OWNER_ID",    default="7661825494", cast=int)
CO_OWNER_ID  = _get_env("CO_OWNER_ID", default="6980326908", cast=int)
OWNERS       = {OWNER_ID, CO_OWNER_ID}

NUBCODER_TOKEN = _get_env("NUBCODER_TOKEN", default="4HBcMS072p")
NUBCODER_API   = f"http://api.nubcoder.com/info?token={NUBCODER_TOKEN}&q={{}}"

VOLUME_BOOST = _get_env("VOLUME_BOOST", default="4.0", cast=float)

COOKIES_FILE = "/app/cookies/cookies.txt"

Path("/tmp/downloads").mkdir(exist_ok=True, parents=True)
Path("/app/sessions").mkdir(exist_ok=True, parents=True)
Path("/app/data").mkdir(exist_ok=True, parents=True)
Path("/app/cookies").mkdir(exist_ok=True, parents=True)

# ═══════════════════════════════════════════════════════════════════════════════
# 📜 LOGGING
# ═══════════════════════════════════════════════════════════════════════════════
class _ColorFmt(logging.Formatter):
    COLORS = {
        'DEBUG':    '\033[36m',
        'INFO':     '\033[32m',
        'WARNING':  '\033[33m',
        'ERROR':    '\033[31m',
        'CRITICAL': '\033[41m',
    }
    RESET = '\033[0m'

    def format(self, record):
        color = self.COLORS.get(record.levelname, '')
        ts = datetime.utcnow().strftime("%H:%M:%S")
        return (
            f"{color}[{ts}] [{record.levelname:<7}]"
            f" [{record.name}] {record.getMessage()}{self.RESET}"
        )

_handler = logging.StreamHandler()
_handler.setFormatter(_ColorFmt())
logging.basicConfig(level=logging.INFO, handlers=[_handler], force=True)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pytgcalls").setLevel(logging.WARNING)
logger = logging.getLogger("ZUDO")

# ═══════════════════════════════════════════════════════════════════════════════
# 🗄️  MONGODB
# ═══════════════════════════════════════════════════════════════════════════════
try:
    mongo = MongoClient(MONGO_URL, serverSelectionTimeoutMS=10000)
    mongo.server_info()
    db = mongo["zudo_userbot"]
    col_sudo        = db["sudo_users"]
    col_cache       = db["chat_cache"]
    col_sessions    = db["sessions"]
    col_default     = db["default_acc"]
    col_music_hosts = db["music_hosts"]
    col_music_sudo  = db["music_sudo"]
    logger.info("✅ MongoDB connected — persistent memory ON")
    MONGO_OK = True
except Exception as e:
    logger.error(f"❌ MongoDB connection failed: {e}")
    MONGO_OK = False
    db = None

# ═══════════════════════════════════════════════════════════════════════════════
# 🤖 INIT BOT
# ═══════════════════════════════════════════════════════════════════════════════
bot = Client(
    "vc_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir="/app/sessions"
)

user_states     = {}
default_account = None
default_calls   = None
user_accounts   = {}
user_calls      = {}
music_accounts  = {}
music_calls     = {}
active_streams  = {}
play_locks      = {}
warmed_chats    = {}   # {client_id_str: set(chat_ids)} — track which chats already warmed
prejoin_state   = {}   # {(host_id, chat_id): True} — already in VC

music_handler_refs = {}

def _get_play_lock(host_id, chat_id):
    key = (host_id, chat_id)
    if key not in play_locks:
        play_locks[key] = asyncio.Lock()
    return play_locks[key]

# ═══════════════════════════════════════════════════════════════════════════════
# 💾 MONGO HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def mongo_save_sudo(uid):
    if MONGO_OK:
        col_sudo.update_one({"_id": uid}, {"$set": {"_id": uid}}, upsert=True)

def mongo_remove_sudo(uid):
    if MONGO_OK:
        col_sudo.delete_one({"_id": uid})

def mongo_get_sudo():
    if not MONGO_OK:
        return set()
    return {d["_id"] for d in col_sudo.find({})}

def mongo_save_cache(key, chat_id, title):
    if MONGO_OK:
        col_cache.update_one({"_id": key}, {"$set": {"chat_id": chat_id, "title": title}}, upsert=True)

def mongo_get_cache(key):
    if not MONGO_OK:
        return None
    d = col_cache.find_one({"_id": key})
    return (d["chat_id"], d["title"]) if d else None

def mongo_save_session(uid, phone, session_string, kind="vc"):
    if not MONGO_OK:
        return
    coll = col_sessions if kind != "default" else col_default
    doc_id = uid if kind != "default" else "default"
    coll.update_one(
        {"_id": doc_id},
        {"$set": {
            "user_id": uid, "phone": phone,
            "session_string": session_string, "kind": kind,
            "saved_at": datetime.utcnow()
        }}, upsert=True)

def mongo_get_session(uid, kind="vc"):
    if not MONGO_OK:
        return None
    coll = col_sessions if kind != "default" else col_default
    doc_id = uid if kind != "default" else "default"
    return coll.find_one({"_id": doc_id})

def mongo_delete_session(uid, kind="vc"):
    if not MONGO_OK:
        return
    coll = col_sessions if kind != "default" else col_default
    doc_id = uid if kind != "default" else "default"
    coll.delete_one({"_id": doc_id})

def mongo_save_music_host(uid):
    if MONGO_OK:
        col_music_hosts.update_one({"_id": uid}, {"$set": {"_id": uid}}, upsert=True)

def mongo_remove_music_host(uid):
    if MONGO_OK:
        col_music_hosts.delete_one({"_id": uid})

def mongo_is_music_host(uid):
    if not MONGO_OK:
        return False
    return col_music_hosts.find_one({"_id": uid}) is not None

def mongo_add_music_sudo(host_id, sudo_id):
    if MONGO_OK:
        col_music_sudo.update_one(
            {"_id": f"{host_id}:{sudo_id}"},
            {"$set": {"host_id": host_id, "sudo_id": sudo_id}}, upsert=True)

def mongo_remove_music_sudo(host_id, sudo_id):
    if MONGO_OK:
        col_music_sudo.delete_one({"_id": f"{host_id}:{sudo_id}"})

def mongo_get_music_sudo(host_id):
    if not MONGO_OK:
        return set()
    return {d["sudo_id"] for d in col_music_sudo.find({"host_id": host_id})}

sudo_users = mongo_get_sudo()

# ═══════════════════════════════════════════════════════════════════════════════
# 🧠 STATE
# ═══════════════════════════════════════════════════════════════════════════════
class UserState:
    def __init__(self):
        self.step = None
        self.data = {}

def get_user_state(uid):
    if uid not in user_states:
        user_states[uid] = UserState()
    return user_states[uid]

def is_owner(uid):
    return uid in OWNERS

def is_authorized(uid):
    return uid in OWNERS or uid in sudo_users

# ═══════════════════════════════════════════════════════════════════════════════
# 🚑 PEER WARMUP (cached — only once per client)
# ═══════════════════════════════════════════════════════════════════════════════
async def warmup_peers(client: Client, label: str = "", force: bool = False):
    """Warm up dialogs so peer IDs are cached. Only runs ONCE per client unless forced."""
    cid = str(id(client))
    if not force and cid in warmed_chats and warmed_chats[cid].get("done"):
        return
    try:
        count = 0
        async for _ in client.get_dialogs():
            count += 1
            if count > 500:
                break
        warmed_chats[cid] = {"done": True, "count": count}
        logger.info(f"🔥 [{label}] Warmed up {count} peers (cached)")
    except Exception as e:
        logger.error(f"warmup_peers [{label}] err: {e}")

async def ensure_peer(client: Client, chat_id: int):
    """Make sure a specific peer is resolvable by client."""
    try:
        await client.get_chat(chat_id)
        return True
    except Exception:
        try:
            await warmup_peers(client, f"ensure-{chat_id}", force=True)
            await client.get_chat(chat_id)
            return True
        except Exception as e:
            logger.error(f"ensure_peer({chat_id}) failed: {e}")
            return False

# ═══════════════════════════════════════════════════════════════════════════════
# 🎹 KEYBOARDS
# ═══════════════════════════════════════════════════════════════════════════════
def build_welcome_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⚔️  VC Fight", callback_data="menu_vcfight"),
            InlineKeyboardButton("🎵 Music Userbot", callback_data="menu_music"),
        ],
        [InlineKeyboardButton("📜 Help", callback_data="show_help")]
    ])

def build_vcfight_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🌟 Default Account", callback_data="use_default"),
            InlineKeyboardButton("🔐 Login My Account", callback_data="use_custom"),
        ],
        [InlineKeyboardButton("« Back", callback_data="back_main")]
    ])

def build_music_keyboard(uid):
    if mongo_is_music_host(uid) or uid in music_accounts:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📜 Commands", callback_data="music_commands")],
            [InlineKeyboardButton("♻️ Restart Userbot", callback_data="music_restart")],
            [InlineKeyboardButton("🚪 Logout Music", callback_data="music_logout")],
            [InlineKeyboardButton("« Back", callback_data="back_main")]
        ])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎤 Host Music Userbot", callback_data="music_host")],
        [InlineKeyboardButton("« Back", callback_data="back_main")]
    ])

# ═══════════════════════════════════════════════════════════════════════════════
# 🔗 CHAT INFO EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════
def extract_chat_info(text):
    text = text.strip()
    invite_patterns = [
        r'(https?://)?t\.me/\+([a-zA-Z0-9_-]+)',
        r'(https?://)?t\.me/joinchat/([a-zA-Z0-9_-]+)',
    ]
    for p in invite_patterns:
        m = re.search(p, text)
        if m:
            return {"type": "invite", "value": text, "hash": m.group(2)}
    username_patterns = [
        r'(https?://)?t\.me/([a-zA-Z0-9_]+)',
        r'(https?://)?telegram\.me/([a-zA-Z0-9_]+)',
        r'@([a-zA-Z0-9_]+)',
    ]
    for p in username_patterns:
        m = re.search(p, text)
        if m:
            username = m.group(2) if ('t.me' in p or 'telegram' in p) else m.group(1)
            return {"type": "username", "value": username}
    if text and not text.startswith('http'):
        return {"type": "username", "value": text.replace('@', '')}
    return None

async def find_chat_in_dialogs(client, invite_hash=None, username=None):
    try:
        async for dialog in client.get_dialogs():
            try:
                chat = dialog.chat
                if invite_hash:
                    try:
                        full = await client.get_chat(chat.id)
                        if getattr(full, 'invite_link', None) and invite_hash in (full.invite_link or ""):
                            return chat.id, chat.title
                    except:
                        pass
                if username and getattr(chat, 'username', None):
                    if chat.username.lower() == username.lower():
                        return chat.id, chat.title
            except:
                continue
    except Exception as e:
        logger.error(f"find_chat_in_dialogs err: {e}")
    return None, None

async def get_chat_id_smart(client, chat_info, user_key, force_ask_id=False):
    try:
        if chat_info["type"] == "invite" and force_ask_id:
            return False, None, None, "🔒 Private — send Chat ID (-100…)", True

        if chat_info["type"] == "username":
            uname = chat_info["value"]
            cache_key = f"{user_key}:u:{uname}"
            cached = mongo_get_cache(cache_key)
            if cached:
                return True, cached[0], cached[1], None, False
            try:
                chat = await client.get_chat(uname)
                mongo_save_cache(cache_key, chat.id, chat.title)
                return True, chat.id, chat.title, None, False
            except Exception:
                pass
            try:
                await client.join_chat(uname)
            except UserAlreadyParticipant:
                pass
            except Exception:
                pass
            try:
                chat = await client.get_chat(uname)
                mongo_save_cache(cache_key, chat.id, chat.title)
                return True, chat.id, chat.title, None, False
            except Exception as e:
                return False, None, None, f"❌ Cannot find @{uname}: {e}", False
        else:
            invite_hash = chat_info.get("hash", "")
            cache_key = f"{user_key}:i:{invite_hash}"
            cached = mongo_get_cache(cache_key)
            if cached:
                return True, cached[0], cached[1], None, False
            try:
                chat = await client.join_chat(chat_info["value"])
                mongo_save_cache(cache_key, chat.id, chat.title)
                return True, chat.id, chat.title, None, False
            except UserAlreadyParticipant:
                cid, ctitle = await find_chat_in_dialogs(client, invite_hash=invite_hash)
                if cid:
                    mongo_save_cache(cache_key, cid, ctitle)
                    return True, cid, ctitle, None, False
                return False, None, None, "🔒 Already member, send Chat ID (-100…)", True
            except InviteHashExpired:
                return False, None, None, "❌ Invite link expired!", False
            except Exception as e:
                return False, None, None, f"❌ {e}", False
    except Exception as e:
        return False, None, None, f"❌ {e}", False

# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 INSTANT STREAM RESOLVER — multi-client yt-dlp rotation + nubcoder fallback
# ═══════════════════════════════════════════════════════════════════════════════
YDL_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Rotation of YouTube player clients — bypass 403 / sign-in / 429 throttle
_YT_PLAYER_CLIENTS = [
    ["android", "web"],
    ["ios", "web"],
    ["tv_embedded", "web"],
    ["android_music", "web"],
    ["mweb", "web"],
    ["web"],
]

def _is_url(s: str) -> bool:
    return bool(re.match(r'^https?://', s.strip(), re.I))

def _make_ydl_opts(idx: int = 0, want_video: bool = False):
    fmt = ('bestvideo[ext=mp4]+bestaudio/best' if want_video
           else 'bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best')
    opts = {
        'format': fmt,
        'quiet': True,
        'no_warnings': True,
        'nocheckcertificate': True,
        'default_search': 'ytsearch1',
        'noplaylist': True,
        'skip_download': True,
        'extract_flat': False,
        'geo_bypass': True,
        'source_address': '0.0.0.0',
        'http_headers': {
            'User-Agent': YDL_UA,
            'Accept-Language': 'en-US,en;q=0.9',
        },
        'extractor_args': {
            'youtube': {
                'player_client': _YT_PLAYER_CLIENTS[idx % len(_YT_PLAYER_CLIENTS)],
                'player_skip': ['webpage'],
            }
        },
        'retries': 2,
        'fragment_retries': 2,
        'socket_timeout': 12,
    }
    if os.path.exists(COOKIES_FILE):
        opts['cookiefile'] = COOKIES_FILE
    return opts


def _sync_extract_yt(query: str, want_video: bool = False):
    """Synchronous yt-dlp extract with multi-client rotation (inspired by reference script)."""
    source = query if _is_url(query) else f"ytsearch1:{query}"
    last_exc = None

    for idx in range(len(_YT_PLAYER_CLIENTS)):
        try:
            opts = _make_ydl_opts(idx, want_video)
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(source, download=False)

            if not info:
                continue
            if 'entries' in info:
                entries = info.get('entries') or []
                if not entries:
                    continue
                info = entries[0]

            # find a playable URL
            stream_url = info.get('url')
            if not stream_url and 'formats' in info:
                for f in reversed(info.get('formats') or []):
                    if f.get('acodec') and f['acodec'] != 'none' and f.get('url'):
                        stream_url = f['url']
                        break

            if not stream_url:
                continue

            title = info.get('title') or 'Unknown'
            duration = int(info.get('duration') or 0)
            logger.info(f"✅ yt-dlp[{idx}/{_YT_PLAYER_CLIENTS[idx]}] → {title}")
            return {
                'url': stream_url,
                'title': title,
                'duration': duration,
                'webpage_url': info.get('webpage_url') or query,
                'thumbnail': info.get('thumbnail') or '',
            }
        except Exception as e:
            last_exc = e
            msg = str(e).lower()
            if any(x in msg for x in ('sign in', '429', 'throttle', '403', 'forbidden', 'unavailable')):
                continue
            # other errors → try next client too
            continue

    if last_exc:
        logger.error(f"yt-dlp all clients failed: {last_exc}")
    return None


async def nubcoder_get_stream(query_or_url: str):
    """Returns (stream_url, title) or (None, None)."""
    try:
        url = NUBCODER_API.format(quote(query_or_url))
        loop = asyncio.get_event_loop()
        r = await loop.run_in_executor(
            None,
            lambda: requests.get(url, timeout=8, headers={"User-Agent": YDL_UA})
        )
        if r.status_code != 200:
            return None, None
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        if not isinstance(data, dict):
            return None, None
        # try common keys
        for key in ("url", "audio_url", "stream", "stream_url", "direct", "audio", "link"):
            v = data.get(key)
            if v and isinstance(v, str) and v.startswith("http"):
                return v, data.get("title") or data.get("name") or "Unknown"
    except Exception as e:
        logger.warning(f"nubcoder err: {e}")
    return None, None


async def resolve_stream_instant(query: str, want_video: bool = False):
    """
    Resolve to a directly streamable URL ASAP.
    Order: yt-dlp (most reliable, multi-client) → nubcoder (fallback).
    Returns (stream_url, title) or (None, None).
    """
    logger.info(f"⚡ Resolving stream: {query}")
    loop = asyncio.get_event_loop()

    # 1) yt-dlp first (more reliable than nubcoder)
    try:
        info = await loop.run_in_executor(None, lambda: _sync_extract_yt(query, want_video))
        if info and info.get('url'):
            return info['url'], info.get('title', 'Unknown')
    except Exception as e:
        logger.error(f"yt-dlp resolver err: {e}")

    # 2) nubcoder fallback (only if yt-dlp failed)
    try:
        stream_url, title = await nubcoder_get_stream(query)
        if stream_url:
            logger.info(f"✅ nubcoder stream: {title}")
            return stream_url, title or "Unknown"
    except Exception as e:
        logger.error(f"nubcoder err: {e}")

    return None, None


# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 STREAM BUILDER (inspired by reference script — multi-fallback)
# ═══════════════════════════════════════════════════════════════════════════════
def _build_stream_objs(source: str, is_video: bool = False):
    """Build a list of stream-input objects to try in order."""
    objs = []
    if is_video:
        if _MediaStream:
            try:
                objs.append(_MediaStream(source))
            except Exception:
                pass
        if _VideoStream:
            try:
                objs.append(_VideoStream(source))
            except Exception:
                pass
    else:
        if _MediaStream:
            # audio-only: try with video flag IGNORE
            try:
                if _VideoFlags is not None:
                    objs.append(_MediaStream(source, video_flags=_VideoFlags.IGNORE))
                else:
                    objs.append(_MediaStream(source))
            except Exception:
                try:
                    objs.append(_MediaStream(source))
                except Exception:
                    pass
        if _AudioPiped:
            try:
                if _HighQualityAudio:
                    objs.append(_AudioPiped(source, _HighQualityAudio()))
                else:
                    objs.append(_AudioPiped(source))
            except Exception:
                try:
                    objs.append(_AudioPiped(source))
                except Exception:
                    pass
        if _AudioStream:
            try:
                objs.append(_AudioStream(source))
            except Exception:
                pass
    return objs


# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 AUDIO BOOST (only for uploaded files)
# ═══════════════════════════════════════════════════════════════════════════════
async def boost_audio(input_path: str, gain: float = VOLUME_BOOST) -> str:
    try:
        out = input_path.rsplit(".", 1)[0] + "_BOOSTED.mp3"
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", input_path,
            "-filter:a", f"volume={gain},loudnorm=I=-12:LRA=11:TP=-1",
            "-b:a", "192k", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await proc.communicate()
        if os.path.exists(out) and os.path.getsize(out) > 0:
            return out
        return input_path
    except Exception as e:
        logger.error(f"Boost failed: {e}")
        return input_path


async def get_audio_duration_from_url(url_or_path: str) -> int:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", url_or_path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=6)
            s = out.decode().strip()
            return int(float(s)) if s else 0
        except asyncio.TimeoutError:
            proc.kill()
            return 0
    except:
        return 0


async def auto_leave_after_playback(calls, stream_key, chat_id, source):
    try:
        dur = await get_audio_duration_from_url(source)
        if dur > 0:
            await asyncio.sleep(dur + 3)
        else:
            await asyncio.sleep(300)
        try:
            await calls.leave_group_call(chat_id)
        except:
            pass
        active_streams.pop(stream_key, None)
        # clear pre-join cache
        for k in list(prejoin_state.keys()):
            if k[1] == chat_id:
                prejoin_state.pop(k, None)
        logger.info(f"👋 Auto-left VC ({stream_key})")
    except Exception as e:
        logger.error(f"auto-leave err: {e}")


async def cleanup_file(file_path):
    try:
        await asyncio.sleep(300)
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
    except:
        pass

# ═══════════════════════════════════════════════════════════════════════════════
# 🎯 ULTRA-INSTANT JOIN VC + PLAY — multi-stream + multi-method fallback
# ═══════════════════════════════════════════════════════════════════════════════
async def _try_play_method(calls, method_name, chat_id, stream_obj):
    """Try one play method with one stream object."""
    method = getattr(calls, method_name, None)
    if not method:
        return False, "method_missing"
    try:
        kwargs = {}
        # Some old API versions accept stream_type
        if method_name == "join_group_call" and StreamType is not None:
            try:
                kwargs["stream_type"] = StreamType().pulse_stream
            except Exception:
                pass
        result = method(chat_id, stream_obj, **kwargs) if kwargs else method(chat_id, stream_obj)
        if asyncio.iscoroutine(result):
            await result
        return True, None
    except Exception as e:
        return False, str(e)


async def join_vc_and_stream(client: Client, calls: PyTgCalls,
                             chat_id: int, source: str, stream_key,
                             is_video: bool = False, host_id=None):
    """
    Robust join — tries multiple stream builders × multiple methods.
    If already joined → uses change_stream for INSTANT switch.
    """
    # Ensure peer is resolved (only if not already cached)
    await ensure_peer(client, chat_id)

    streams = _build_stream_objs(source, is_video)
    if not streams:
        return False, "No stream builder available (pytgcalls not properly installed?)"

    pj_key = (host_id or id(client), chat_id)

    # ── If already in VC → just switch stream (super fast)
    if prejoin_state.get(pj_key) or stream_key in active_streams:
        for stream_obj in streams:
            try:
                method = getattr(calls, "change_stream", None) or getattr(calls, "set_stream", None)
                if method:
                    res = method(chat_id, stream_obj)
                    if asyncio.iscoroutine(res):
                        await res
                    active_streams[stream_key] = chat_id
                    prejoin_state[pj_key] = True
                    logger.info(f"🔄 Switched stream in VC {chat_id}")
                    return True, None
            except Exception as e:
                emsg = str(e).lower()
                if "not in group call" in emsg or "no active" in emsg:
                    break  # fall through to fresh join
                continue

    # ── Fresh join: try every (method × stream) combo
    last_err = "unknown"
    for method_name in ("play", "join_group_call", "stream"):
        if not getattr(calls, method_name, None):
            continue
        for stream_obj in streams:
            ok, err = await _try_play_method(calls, method_name, chat_id, stream_obj)
            if ok:
                active_streams[stream_key] = chat_id
                prejoin_state[pj_key] = True
                logger.info(f"✅ Joined VC {chat_id} via {method_name} + {type(stream_obj).__name__}")
                return True, None
            last_err = err or "unknown"
            emsg = (err or "").lower()
            # "already in group call" → try change_stream
            if "already" in emsg or "groupcall_join_missing" in emsg:
                try:
                    cs = getattr(calls, "change_stream", None)
                    if cs:
                        res = cs(chat_id, stream_obj)
                        if asyncio.iscoroutine(res):
                            await res
                        active_streams[stream_key] = chat_id
                        prejoin_state[pj_key] = True
                        logger.info(f"🔄 change_stream OK in VC {chat_id}")
                        return True, None
                except Exception as e2:
                    last_err = f"change_stream fail: {e2}"
            # peer issues → warm and retry once
            if "peer" in emsg or "key_id" in emsg or "keyerror" in emsg:
                await warmup_peers(client, f"retry-{stream_key}", force=True)
                ok2, err2 = await _try_play_method(calls, method_name, chat_id, stream_obj)
                if ok2:
                    active_streams[stream_key] = chat_id
                    prejoin_state[pj_key] = True
                    return True, None
                last_err = err2 or last_err
            # no active call → fatal
            if any(x in emsg for x in [
                "no active group call", "group_call_invalid",
                "groupcall_forbidden"
            ]):
                return False, "⚠️ No active VC in group. Start a voice chat first."

    return False, f"All play methods failed. Last error: {last_err}"


# ═══════════════════════════════════════════════════════════════════════════════
# 🛡️ Safe stop wrapper
# ═══════════════════════════════════════════════════════════════════════════════
async def safe_stop_calls(calls):
    if not calls:
        return
    for method in ("stop", "_stop", "terminate", "close"):
        fn = getattr(calls, method, None)
        if fn:
            try:
                res = fn()
                if asyncio.iscoroutine(res):
                    await res
                return
            except Exception:
                continue
    return

# ═══════════════════════════════════════════════════════════════════════════════
# 📜 HELP TEXT
# ═══════════════════════════════════════════════════════════════════════════════
HELP_TEXT = """
╔════════════════════════════════════╗
║   🔥  ZUDO USERBOT  —  /help   🔥   ║
╚════════════════════════════════════╝

━━━━━━━ 🤖  BOT (DM)  ━━━━━━━
  /start            • Open main menu
  /help             • This help message
  /commands         • Music userbot commands
  /stop             • Stop your active VC stream
  /status           • Show your sessions / streams
  /restart          • Restart your music userbot

OTP/2FA INPUT:
  📨 OTP: send digits with spaces  →  e.g.  1 2 3 4 5
  🔐 2FA: just send your password as text

━━━━━━━ 👑  OWNER ONLY  ━━━━━━━
  /sudo  <id|@user>   • Add sudo user
  /rmsudo <id|@user>  • Remove sudo user
  /sudolist           • List sudo users
  /owners             • Show owners

━━━━━━━ 🎵  MUSIC USERBOT  (in groups, dot-prefix)  ━━━━━━━
  .play   <song / yt url>     • INSTANT play (≤5s)
  .vplay  <song / yt url>     • Same as .play
  .pause                      • Pause stream
  .resume                     • Resume stream
  .skip                       • Skip current track
  .stop / .end                • Stop & leave VC
  .mute  / .unmute            • Stream mute
  .ping  / .alive             • Liveness check
  .auth   <reply|id>          • Grant sudo on userbot
  .dauth  <reply|id>          • Revoke sudo
  .authlist                   • Sudo list
  .restart                    • Restart this userbot
  .help / .commands           • This help

━━━━━━━ ⚡ Powered by @zudo_userbot ⚡ ━━━━━━━
"""

# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 MUSIC USERBOT HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════
def _is_caller_allowed(host_id: int, user_id: int) -> bool:
    if user_id == host_id or user_id in OWNERS:
        return True
    if user_id in mongo_get_music_sudo(host_id):
        return True
    return False

def _is_group_chat(m: Message) -> bool:
    try:
        return m.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP)
    except Exception:
        return False

def register_music_handlers(client: Client, host_id: int):
    unregister_music_handlers(client, host_id)
    handler_refs = []

    async def _play(c, m: Message):
        try:
            if not _is_group_chat(m):
                return
            if not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
                return
            if len(m.command) < 2:
                try: await m.reply_text("❌ Usage: `.play <song name or YouTube URL>`")
                except: pass
                return

            query = " ".join(m.command[1:])
            chat_id = m.chat.id
            calls = music_calls.get(host_id)
            if not calls:
                try: await m.reply_text("❌ Music backend not ready.")
                except: pass
                return

            lock = _get_play_lock(host_id, chat_id)
            if lock.locked():
                return

            asyncio.create_task(_safe_delete(m))

            async with lock:
                proc = await c.send_message(chat_id, f"⚡ Searching & streaming: `{query}`")

                # parallel: stream resolve (yt-dlp + nubcoder) + ensure peer
                resolve_task = asyncio.create_task(resolve_stream_instant(query))
                asyncio.create_task(ensure_peer(c, chat_id))

                try:
                    stream_url, title = await asyncio.wait_for(resolve_task, timeout=18)
                except asyncio.TimeoutError:
                    try: await proc.edit_text("❌ Timeout fetching stream. Try again.")
                    except: pass
                    return

                if not stream_url:
                    try: await proc.edit_text("❌ Couldn't resolve audio. Try another query.")
                    except: pass
                    return

                ok, err = await join_vc_and_stream(
                    c, calls, chat_id, stream_url, f"music_{host_id}",
                    is_video=False, host_id=host_id
                )
                if not ok:
                    try: await proc.edit_text(f"❌ Play failed: {err}")
                    except: pass
                    return

                asyncio.create_task(auto_leave_after_playback(
                    calls, f"music_{host_id}", chat_id, stream_url))

                try:
                    await proc.edit_text(
                        f"▶️ **Now Playing** (LIVE stream)\n"
                        f"🎵 `{title}`\n"
                        f"💬 Requested by: {m.from_user.mention}"
                    )
                except: pass
                logger.info(f"🎵 Streaming '{title}' in chat {chat_id} via host {host_id}")
        except Exception as e:
            logger.error(f".play err: {e}")

    async def _stop(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
            return
        try:
            calls = music_calls.get(host_id)
            await calls.leave_group_call(m.chat.id)
            active_streams.pop(f"music_{host_id}", None)
            prejoin_state.pop((host_id, m.chat.id), None)
            await m.reply_text("⏹ Stopped.")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    async def _pause(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
            return
        try:
            await music_calls[host_id].pause_stream(m.chat.id)
            await m.reply_text("⏸ Paused.")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    async def _resume(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
            return
        try:
            await music_calls[host_id].resume_stream(m.chat.id)
            await m.reply_text("▶️ Resumed.")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    async def _mute(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
            return
        try:
            await music_calls[host_id].mute_stream(m.chat.id)
            await m.reply_text("🔇 Muted.")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    async def _unmute(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
            return
        try:
            await music_calls[host_id].unmute_stream(m.chat.id)
            await m.reply_text("🔊 Unmuted.")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    async def _auth(c, m: Message):
        if not _is_group_chat(m) or not m.from_user:
            return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS:
            return
        target = None
        if m.reply_to_message and m.reply_to_message.from_user:
            target = m.reply_to_message.from_user.id
        elif len(m.command) > 1:
            try:
                u = await c.get_users(m.command[1].replace("@", ""))
                target = u.id
            except:
                pass
        if not target:
            await m.reply_text("❌ Reply to user or give user_id/@username")
            return
        mongo_add_music_sudo(host_id, target)
        await m.reply_text(f"✅ Auth granted to `{target}`")

    async def _dauth(c, m: Message):
        if not _is_group_chat(m) or not m.from_user:
            return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS:
            return
        target = None
        if m.reply_to_message and m.reply_to_message.from_user:
            target = m.reply_to_message.from_user.id
        elif len(m.command) > 1:
            try:
                u = await c.get_users(m.command[1].replace("@", ""))
                target = u.id
            except:
                pass
        if not target:
            await m.reply_text("❌ Reply or give user_id")
            return
        mongo_remove_music_sudo(host_id, target)
        await m.reply_text(f"✅ Auth removed for `{target}`")

    async def _authlist(c, m: Message):
        if not _is_group_chat(m) or not m.from_user:
            return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS:
            return
        ids = mongo_get_music_sudo(host_id)
        if not ids:
            await m.reply_text("ℹ️ No sudo users.")
            return
        txt = "👥 **Sudo list:**\n" + "\n".join(f"• `{i}`" for i in ids)
        await m.reply_text(txt)

    async def _ping(c, m: Message):
        if not _is_group_chat(m) or not m.from_user:
            return
        if not _is_caller_allowed(host_id, m.from_user.id):
            return
        await m.reply_text("🏓 **Pong!** Music Userbot Alive 🔥")

    async def _help_cmd(c, m: Message):
        if not _is_group_chat(m) or not m.from_user:
            return
        if not _is_caller_allowed(host_id, m.from_user.id):
            return
        await m.reply_text(f"```{HELP_TEXT}```")

    async def _restart_inline(c, m: Message):
        if not _is_group_chat(m) or not m.from_user:
            return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS:
            return
        msg = await m.reply_text("♻️ Restarting your music userbot…")
        ok = await restart_music_userbot(host_id)
        if ok:
            await msg.edit_text("✅ Music userbot restarted successfully!")
        else:
            await msg.edit_text("❌ Restart failed. Try /start to re-host.")

    handlers_to_add = [
        (_play,           ["play", "vplay"]),
        (_stop,           ["skip", "stop", "end"]),
        (_pause,          ["pause"]),
        (_resume,         ["resume"]),
        (_mute,           ["mute"]),
        (_unmute,         ["unmute"]),
        (_auth,           ["auth"]),
        (_dauth,          ["dauth"]),
        (_authlist,       ["authlist"]),
        (_ping,           ["ping", "alive"]),
        (_help_cmd,       ["help", "commands"]),
        (_restart_inline, ["restart"]),
    ]

    for func, cmds in handlers_to_add:
        h = MessageHandler(func, filters.command(cmds, prefixes="."))
        client.add_handler(h)
        handler_refs.append(h)

    music_handler_refs[host_id] = handler_refs
    logger.info(f"✅ Music handlers attached for host {host_id} ({len(handler_refs)} handlers)")


async def _safe_delete(m: Message):
    try:
        await asyncio.sleep(0.1)
        await m.delete()
    except Exception:
        pass


def unregister_music_handlers(client: Client, host_id: int):
    refs = music_handler_refs.pop(host_id, [])
    for h in refs:
        try:
            client.remove_handler(h)
        except Exception:
            pass
    if refs:
        logger.info(f"🧹 Removed {len(refs)} old music handlers for {host_id}")

# ═══════════════════════════════════════════════════════════════════════════════
# ♻️ RESTART MUSIC USERBOT
# ═══════════════════════════════════════════════════════════════════════════════
async def restart_music_userbot(host_id: int) -> bool:
    logger.info(f"♻️ Restart requested for host {host_id}")

    if host_id in music_calls:
        await safe_stop_calls(music_calls[host_id])
        music_calls.pop(host_id, None)

    if host_id in music_accounts:
        old_client = music_accounts[host_id]
        try:
            unregister_music_handlers(old_client, host_id)
        except Exception:
            pass
        try:
            await old_client.stop()
        except Exception as e:
            logger.warning(f"old client stop err: {e}")
        music_accounts.pop(host_id, None)

    # clear pre-join cache for this host
    for k in list(prejoin_state.keys()):
        if k[0] == host_id:
            prejoin_state.pop(k, None)

    sess_doc = mongo_get_session(host_id, "music")
    if not sess_doc or not sess_doc.get("session_string"):
        logger.error(f"No saved music session for {host_id}")
        return False

    try:
        c = Client(
            f"music_{host_id}_restarted",
            api_id=API_ID, api_hash=API_HASH,
            session_string=sess_doc["session_string"], in_memory=True
        )
        await c.start()
        await warmup_peers(c, f"restart-{host_id}", force=True)
        calls = PyTgCalls(c)
        await calls.start()
        music_accounts[host_id] = c
        music_calls[host_id] = calls
        register_music_handlers(c, host_id)
        logger.info(f"✅ Music userbot for {host_id} restarted successfully")
        return True
    except (AuthKeyUnregistered, AuthKeyDuplicated,
            UserDeactivated, UserDeactivatedBan) as e:
        logger.error(f"❌ Session dead for {host_id}: {e}. Removing from DB.")
        mongo_delete_session(host_id, "music")
        mongo_remove_music_host(host_id)
        return False
    except Exception as e:
        logger.error(f"Restart fail for {host_id}: {e}")
        return False

# ═══════════════════════════════════════════════════════════════════════════════
# 🔄 RESTORE SESSIONS
# ═══════════════════════════════════════════════════════════════════════════════
async def restore_sessions():
    global default_account, default_calls
    if not MONGO_OK:
        return

    d = col_default.find_one({"_id": "default"})
    if d and d.get("session_string"):
        try:
            c = Client(
                "default_session_restored",
                api_id=API_ID, api_hash=API_HASH,
                session_string=d["session_string"], in_memory=True)
            await c.start()
            await warmup_peers(c, "default", force=True)
            default_account = c
            default_calls = PyTgCalls(c)
            await default_calls.start()
            logger.info("✅ Default account restored")
        except (AuthKeyUnregistered, AuthKeyDuplicated,
                UserDeactivated, UserDeactivatedBan) as e:
            logger.error(f"❌ Default session DEAD ({e}). Removing.")
            try: col_default.delete_one({"_id": "default"})
            except: pass
        except Exception as e:
            logger.error(f"Default restore fail: {e}")

    sessions = list(col_sessions.find({}))

    async def _restore_one(s):
        uid = s.get("user_id")
        kind = s.get("kind", "vc")
        sess = s.get("session_string")
        if not uid or not sess:
            return
        try:
            name = f"user_{uid}_{kind}_restored"
            c = Client(name, api_id=API_ID, api_hash=API_HASH,
                       session_string=sess, in_memory=True)
            await c.start()
            await warmup_peers(c, f"{kind}-{uid}", force=True)
            calls = PyTgCalls(c)
            await calls.start()
            if kind == "music":
                music_accounts[uid] = c
                music_calls[uid] = calls
                register_music_handlers(c, uid)
                logger.info(f"✅ Music userbot restored for {uid}")
            else:
                user_accounts[uid] = c
                user_calls[uid] = calls
                logger.info(f"✅ VC user {uid} restored")
        except (AuthKeyUnregistered, AuthKeyDuplicated,
                UserDeactivated, UserDeactivatedBan) as e:
            logger.error(f"❌ Session DEAD for {uid} ({kind}): {e}. Removing.")
            try:
                mongo_delete_session(uid, kind)
                if kind == "music":
                    mongo_remove_music_host(uid)
            except Exception as ce:
                logger.error(f"cleanup fail for {uid}: {ce}")
        except Exception as e:
            logger.error(f"⚠️ Restore fail for {uid} ({kind}): {e} — SKIPPING")

    if sessions:
        await asyncio.gather(
            *[_restore_one(s) for s in sessions],
            return_exceptions=True
        )

# ═══════════════════════════════════════════════════════════════════════════════
# 📨 BOT COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_message(filters.command("start") & filters.private)
async def start_command(client, m: Message):
    if not is_authorized(m.from_user.id):
        await m.reply_text("❌ You don't have permission to use this bot!")
        return
    txt = (
        "╔══════════════════════════════╗\n"
        "║   🎵  ZUDO USERBOT  🎵       ║\n"
        "╚══════════════════════════════╝\n\n"
        "👋 **Welcome!** Choose what you want to do:\n\n"
        "⚔️  **VC Fight** — Stream audio loudly in any voice chat\n"
        "🎵 **Music Userbot** — Personal music bot (`.play`, `.skip`...)\n\n"
        "Type /help for full command list.\n\n"
        "🔥 Powered by @zudo_userbot"
    )
    await m.reply_text(txt, reply_markup=build_welcome_keyboard())

@bot.on_message(filters.command(["help","commands"]) & filters.private)
async def help_cmd(c, m: Message):
    if not is_authorized(m.from_user.id):
        return
    await m.reply_text(f"```{HELP_TEXT}```")

@bot.on_message(filters.command("owners") & filters.private)
async def owners_cmd(c, m: Message):
    if not is_authorized(m.from_user.id):
        return
    lines = ["👑 **Bot Owners:**"]
    for oid in OWNERS:
        try:
            u = await c.get_users(oid)
            lines.append(f"• {u.first_name} (`{oid}`)")
        except:
            lines.append(f"• `{oid}`")
    await m.reply_text("\n".join(lines))

@bot.on_message(filters.command("status") & filters.private)
async def status_cmd(c, m: Message):
    if not is_authorized(m.from_user.id):
        return
    uid = m.from_user.id
    has_vc    = uid in user_accounts
    has_music = uid in music_accounts
    has_def   = default_account is not None
    streams   = [k for k in active_streams if k == uid or k == "default" or k == f"music_{uid}"]
    txt = (
        f"📊 **Status**\n\n"
        f"• Default Account : {'✅' if has_def else '❌'}\n"
        f"• VC Fight (you)  : {'✅' if has_vc else '❌'}\n"
        f"• Music Userbot   : {'✅' if has_music else '❌'}\n"
        f"• Active Streams  : {len(streams)}\n"
        f"• Sudo users      : {len(sudo_users)}\n"
        f"• Owners          : {len(OWNERS)}\n"
        f"• MongoDB         : {'✅' if MONGO_OK else '❌'}\n"
    )
    await m.reply_text(txt)

@bot.on_message(filters.command("restart") & filters.private)
async def restart_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_authorized(uid):
        return
    if not (mongo_is_music_host(uid) or uid in music_accounts):
        await m.reply_text("❌ Aap kisi music userbot ke host nahi ho. /start se host karo.")
        return
    proc = await m.reply_text("♻️ Restarting your music userbot…")
    ok = await restart_music_userbot(uid)
    if ok:
        await proc.edit_text("✅ **Music userbot restarted!**")
    else:
        await proc.edit_text("❌ Restart fail. Session expire ho sakti hai — /start se re-host karo.")

@bot.on_message(filters.command("sudo") & filters.private)
async def add_sudo(client, m: Message):
    if not is_owner(m.from_user.id):
        return
    if len(m.command) < 2:
        await m.reply_text("Usage: `/sudo @user` or `/sudo 123456`")
        return
    try:
        x = m.command[1]
        u = await client.get_users(x.lstrip("@")) if not x.lstrip("-").isdigit() else await client.get_users(int(x))
        sudo_users.add(u.id)
        mongo_save_sudo(u.id)
        await m.reply_text(f"✅ {u.first_name} added as sudo.")
    except Exception as e:
        await m.reply_text(f"❌ {e}")

@bot.on_message(filters.command("rmsudo") & filters.private)
async def rm_sudo(client, m: Message):
    if not is_owner(m.from_user.id):
        return
    if len(m.command) < 2:
        await m.reply_text("Usage: `/rmsudo @user`")
        return
    try:
        x = m.command[1]
        u = await client.get_users(x.lstrip("@")) if not x.lstrip("-").isdigit() else await client.get_users(int(x))
        sudo_users.discard(u.id)
        mongo_remove_sudo(u.id)
        await m.reply_text(f"✅ {u.first_name} removed.")
    except Exception as e:
        await m.reply_text(f"❌ {e}")

@bot.on_message(filters.command("sudolist") & filters.private)
async def list_sudo(client, m: Message):
    if not is_owner(m.from_user.id):
        return
    if not sudo_users:
        await m.reply_text("ℹ️ No sudo users.")
        return
    out = "👥 **Sudo Users:**\n"
    for uid in sudo_users:
        try:
            u = await client.get_users(uid)
            out += f"• {u.first_name} (`{uid}`)\n"
        except:
            out += f"• `{uid}`\n"
    await m.reply_text(out)

@bot.on_message(filters.command("stop") & filters.private)
async def stop_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_authorized(uid):
        return
    stopped = False
    for key in (uid, "default", f"music_{uid}"):
        if key in active_streams:
            try:
                cid = active_streams[key]
                if key == "default" and default_calls:
                    await default_calls.leave_group_call(cid)
                elif isinstance(key, str) and key.startswith("music_") and uid in music_calls:
                    await music_calls[uid].leave_group_call(cid)
                elif uid in user_calls:
                    await user_calls[uid].leave_group_call(cid)
                del active_streams[key]
                # clear prejoin
                for k in list(prejoin_state.keys()):
                    if k[1] == cid:
                        prejoin_state.pop(k, None)
                stopped = True
            except:
                pass
    await m.reply_text("✅ Stopped." if stopped else "❌ No active stream.")

# ═══════════════════════════════════════════════════════════════════════════════
# 🔘 CALLBACKS
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_callback_query()
async def cb_handler(client, cq: CallbackQuery):
    uid = cq.from_user.id
    data = cq.data
    state = get_user_state(uid)

    if not is_authorized(uid):
        await cq.answer("❌ No permission!", show_alert=True)
        return

    if data == "back_main":
        await cq.message.edit_text(
            "👋 **Welcome!** Choose what you want to do:",
            reply_markup=build_welcome_keyboard())
        return

    if data == "show_help":
        await cq.message.reply_text(f"```{HELP_TEXT}```")
        await cq.answer()
        return

    if data == "menu_vcfight":
        await cq.message.edit_text(
            "⚔️ **VC Fight Mode**\n\nPick an account to stream from:",
            reply_markup=build_vcfight_keyboard())
        return

    if data == "menu_music":
        txt = "🎵 **Music Userbot**\n\nPersonal music bot — host once, then use `.play`, `.skip` etc.\n\n"
        if mongo_is_music_host(uid) or uid in music_accounts:
            txt += "✅ Already hosted! Use /commands for full list."
        else:
            txt += "Tap below to host your userbot."
        await cq.message.edit_text(txt, reply_markup=build_music_keyboard(uid))
        return

    if data == "use_default":
        if default_account is None:
            if is_owner(uid):
                state.step = "default_phone"
                state.data = {"mode": "default"}
                await cq.message.reply_text("📱 **Setup Default Account**\n\nSend phone number with country code:")
            else:
                await cq.answer("❌ Default not configured by owner yet", show_alert=True)
            return
        state.step = "default_group"
        state.data = {"mode": "default"}
        await cq.message.reply_text(
            "📎 **Send Group Info**\n\n🌐 Public: `@groupusername`\n🔒 Private: `https://t.me/+xxxxx`")
        return

    if data == "use_custom":
        sess = mongo_get_session(uid, "vc")
        if sess or uid in user_accounts:
            state.step = "custom_group"
            state.data = {"mode": "custom"}
            await cq.message.reply_text("✅ Already logged in!\n\n📎 Send group info (@username or invite link):")
        else:
            state.step = "custom_phone"
            state.data = {"mode": "custom"}
            await cq.message.reply_text("📱 **Login My Account**\n\nSend your phone number with country code:")
        return

    if data == "music_host":
        if mongo_is_music_host(uid) or uid in music_accounts:
            await cq.answer("Already hosted!", show_alert=True); return
        state.step = "music_phone"
        state.data = {"mode": "music"}
        await cq.message.reply_text("🎤 **Host Music Userbot**\n\nSend phone number with country code:")
        return

    if data == "music_commands":
        await cq.message.reply_text(f"```{HELP_TEXT}```"); return

    if data == "music_restart":
        await cq.answer("⏳ Restarting…")
        proc = await cq.message.reply_text("♻️ Restarting your music userbot…")
        ok = await restart_music_userbot(uid)
        if ok:
            await proc.edit_text("✅ Music userbot restarted successfully!")
        else:
            await proc.edit_text("❌ Restart failed. Try /start to re-host.")
        return

    if data == "music_logout":
        if uid in music_accounts:
            try:
                unregister_music_handlers(music_accounts[uid], uid)
            except: pass
            await safe_stop_calls(music_calls.get(uid))
            try: await music_accounts[uid].stop()
            except: pass
            music_accounts.pop(uid, None)
            music_calls.pop(uid, None)
        mongo_remove_music_host(uid)
        mongo_delete_session(uid, "music")
        await cq.message.reply_text("✅ Music userbot logged out.")
        return

    await cq.answer()

# ═══════════════════════════════════════════════════════════════════════════════
# 🔑 OTP / 2FA VERIFY (TEXT BASED)
# ═══════════════════════════════════════════════════════════════════════════════
def _clean_otp_text(text: str) -> str:
    return re.sub(r'\D', '', text)


async def verify_otp_text(m: Message, mode: str, otp: str):
    uid = m.from_user.id
    state = get_user_state(uid)
    proc = await m.reply_text("⏳ Verifying OTP…")
    try:
        uc = state.data["client"]
        await uc.sign_in(state.data["phone"], state.data["phone_code_hash"], otp)
        await _finalize_login(uid, uc, mode, proc, state)
    except SessionPasswordNeeded:
        state.step = {
            "default_otp": "default_2fa",
            "custom_otp":  "custom_2fa",
            "music_otp":   "music_2fa",
        }[mode]
        await proc.edit_text(
            "🔐 **2FA Enabled**\n\n"
            "Send your 2FA password as **plain text** now."
        )
    except PhoneCodeInvalid:
        await proc.edit_text("❌ Invalid OTP! Send digits again like: `1 2 3 4 5`\n(or run /start)")
    except Exception as e:
        await proc.edit_text(f"❌ {e}")
        state.step = None


async def _finalize_login(uid, uc: Client, mode: str, proc_msg, state):
    global default_account, default_calls
    logger.info(f"🔐 Login confirmed for {uid} (mode={mode}) — finalizing…")

    try:
        await proc_msg.edit_text("✅ Verified! Setting up your session…")
    except: pass

    sess_str = await uc.export_session_string()
    asyncio.create_task(warmup_peers(uc, f"login-{mode}", force=True))

    if mode in ("default_otp", "default_2fa"):
        default_account = uc
        default_calls = PyTgCalls(uc)
        await default_calls.start()
        mongo_save_session(uid, state.data.get("phone", ""), sess_str, "default")
        await proc_msg.edit_text("✅ **Default account configured!**")

    elif mode in ("custom_otp", "custom_2fa"):
        user_accounts[uid] = uc
        user_calls[uid] = PyTgCalls(uc)
        await user_calls[uid].start()
        mongo_save_session(uid, state.data.get("phone", ""), sess_str, "vc")
        state.step = "custom_group"
        await proc_msg.edit_text(
            "✅ **Logged in successfully!**\n\n"
            "📎 Now send group info (@username or invite link):"
        )
        return

    elif mode in ("music_otp", "music_2fa"):
        music_accounts[uid] = uc
        music_calls[uid] = PyTgCalls(uc)
        await music_calls[uid].start()
        mongo_save_session(uid, state.data.get("phone", ""), sess_str, "music")
        mongo_save_music_host(uid)
        register_music_handlers(uc, uid)
        await proc_msg.edit_text(
            "✅ **Music Userbot Hosted!** 🎵\n\n"
            "Add to any group → `.play <song>` (instant streaming)\n"
            "Send /help for full command list."
        )

    state.step = None
    logger.info(f"✅ Finalize complete for {uid} (mode={mode})")

# ═══════════════════════════════════════════════════════════════════════════════
# 💬 TEXT MESSAGE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_message(filters.private & filters.text & ~filters.command(
    ["start","help","commands","sudo","rmsudo","sudolist","stop","status","logout","owners","restart"]))
async def msg_handler(client, m: Message):
    uid = m.from_user.id
    if not is_authorized(uid):
        return
    state = get_user_state(uid)
    text = m.text
    if not state.step:
        return

    try:
        # PHONE step
        if state.step in ("default_phone", "custom_phone", "music_phone"):
            phone = text.strip().replace(" ", "")
            state.data["phone"] = phone
            proc = await m.reply_text("⏳ Sending OTP...")
            try:
                name = {
                    "default_phone": "default_session_tmp",
                    "custom_phone":  f"user_{uid}_tmp",
                    "music_phone":   f"music_{uid}_tmp"
                }[state.step]
                uc = Client(name, api_id=API_ID, api_hash=API_HASH, in_memory=True)
                await uc.connect()
                sent = await uc.send_code(phone)
                state.data["phone_code_hash"] = sent.phone_code_hash
                state.data["client"] = uc
                next_mode = {
                    "default_phone": "default_otp",
                    "custom_phone":  "custom_otp",
                    "music_phone":   "music_otp"
                }[state.step]
                state.step = next_mode
                await proc.edit_text(
                    "📨 **OTP Sent!**\n\n"
                    "Now type the OTP code as **text** here.\n"
                    "Format: digits with spaces between them.\n\n"
                    "Example:  `1 2 3 4 5`\n"
                    "(or just `12345` also works)"
                )
            except FloodWait as e:
                await proc.edit_text(f"⏳ Flood wait: {e.value}s"); state.step = None
            except Exception as e:
                await proc.edit_text(f"❌ {e}"); state.step = None
            return

        # OTP step
        if state.step in ("default_otp", "custom_otp", "music_otp"):
            otp = _clean_otp_text(text)
            if len(otp) < 4:
                await m.reply_text("❌ Send digits like: `1 2 3 4 5` (min 4 digits)")
                return
            await verify_otp_text(m, state.step, otp)
            return

        # 2FA step
        if state.step in ("default_2fa", "custom_2fa", "music_2fa"):
            proc = await m.reply_text("⏳ Verifying 2FA password…")
            try:
                uc = state.data["client"]
                await uc.check_password(text.strip())
                await _finalize_login(uid, uc, state.step, proc, state)
            except PasswordHashInvalid:
                await proc.edit_text("❌ Wrong 2FA password! Run /start again.")
                state.step = None
            except Exception as e:
                await proc.edit_text(f"❌ {e}")
                state.step = None
            return

        if state.step == "waiting_chat_id":
            try:
                cid = int(text.strip())
                state.data["actual_chat_id"] = cid
                state.step = "audio_input"
                await m.reply_text(f"✅ Chat ID set: `{cid}`\n\n🎵 Send Audio / YouTube URL / Song name")
            except ValueError:
                await m.reply_text("❌ Invalid! Send like: `-100123456789`")
            return

        if state.step in ("default_group", "custom_group"):
            ci = extract_chat_info(text)
            if not ci:
                await m.reply_text("❌ Invalid input. Send `@username` or `https://t.me/+xxx`")
                return
            state.data["chat_info"] = ci
            mode = state.data.get("mode")
            client_to_use = default_account if mode == "default" else user_accounts.get(uid)
            stream_key = "default" if mode == "default" else uid
            if not client_to_use:
                await m.reply_text("❌ Session expired, /start again"); state.step = None; return

            if ci["type"] == "username":
                proc = await m.reply_text("⏳ Resolving public group...")
                ok, cid, title, err, need_id = await get_chat_id_smart(client_to_use, ci, stream_key)
                if ok:
                    state.data["actual_chat_id"] = cid
                    state.data["chat_title"] = title
                    state.step = "audio_input"
                    # 🔥 PRE-WARM peer in background so VC join is instant later
                    asyncio.create_task(ensure_peer(client_to_use, cid))
                    await proc.edit_text(f"✅ **Group:** {title}\n\n🎵 Send Audio / YouTube URL / Song name")
                else:
                    await proc.edit_text(err); state.step = None
            else:
                proc = await m.reply_text("⏳ Trying to join private group...")
                try:
                    await client_to_use.join_chat(ci["value"])
                except UserAlreadyParticipant: pass
                except InviteHashExpired:
                    await proc.edit_text("❌ Invite link expired!"); state.step = None; return
                except Exception: pass
                state.step = "waiting_chat_id"
                await proc.edit_text(
                    "🔒 **Private Group**\n\nSend the Chat ID (e.g. `-100123456789`)")
            return

        # VC fight audio input (INSTANT)
        if state.step == "audio_input":
            mode = state.data.get("mode")
            ci = state.data.get("chat_info")
            if mode == "default":
                client_to_use, calls_to_use, stream_key = default_account, default_calls, "default"
            else:
                client_to_use = user_accounts.get(uid)
                calls_to_use = user_calls.get(uid)
                stream_key = uid
            if not client_to_use or not calls_to_use:
                await m.reply_text("❌ Session expired, /start"); state.step = None; return

            proc = await m.reply_text("⚡ Resolving stream...")
            actual_id = state.data.get("actual_chat_id")
            title = state.data.get("chat_title", "Group")
            if not actual_id:
                ok, actual_id, title, err, _ = await get_chat_id_smart(client_to_use, ci, stream_key)
                if not ok:
                    await proc.edit_text(err); state.step = None; return

            # 🔥 PARALLEL: stream resolve + peer ensure (peer already warmed earlier!)
            t0 = time.time()
            resolve_task = asyncio.create_task(resolve_stream_instant(text))
            asyncio.create_task(ensure_peer(client_to_use, actual_id))

            try:
                stream_url, song_title = await asyncio.wait_for(resolve_task, timeout=18)
            except asyncio.TimeoutError:
                await proc.edit_text("❌ Timeout fetching stream"); state.step = None; return

            if not stream_url:
                await proc.edit_text("❌ Couldn't fetch audio stream"); state.step = None; return

            await proc.edit_text("🔌 Joining VC...")
            ok, err = await join_vc_and_stream(
                client_to_use, calls_to_use, actual_id, stream_url, stream_key,
                is_video=False, host_id=uid if mode != "default" else "default"
            )
            elapsed = time.time() - t0
            if ok:
                asyncio.create_task(auto_leave_after_playback(calls_to_use, stream_key, actual_id, stream_url))
                await proc.edit_text(
                    f"✅ **NOW STREAMING** 🔥\n\n📻 Group: {title}\n🎵 {song_title or 'Audio'}\n"
                    f"⚡ Joined in {elapsed:.1f}s\n\nUse /stop to stop")
            else:
                await proc.edit_text(f"❌ Play failed: {err}")
            state.step = None
            return

    except Exception as e:
        logger.error(f"msg_handler err: {e}")
        await m.reply_text(f"❌ {e}")
        state.step = None

# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 AUDIO / VOICE FILE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_message(filters.private & (filters.audio | filters.voice))
async def audio_handler(client, m: Message):
    uid = m.from_user.id
    if not is_authorized(uid):
        return
    state = get_user_state(uid)
    if state.step != "audio_input":
        return
    mode = state.data.get("mode")
    if mode == "default":
        cu, calls, sk = default_account, default_calls, "default"
    else:
        cu, calls, sk = user_accounts.get(uid), user_calls.get(uid), uid
    if not cu or not calls:
        await m.reply_text("❌ Session expired"); state.step = None; return
    proc = await m.reply_text("⏳ Downloading audio...")
    raw = await m.download(file_name=f"/tmp/downloads/{m.id}.mp3")
    boosted = await boost_audio(raw)
    actual = state.data.get("actual_chat_id")
    title = state.data.get("chat_title", "Group")
    await proc.edit_text("🔌 Joining VC with 🔊 BOOST...")
    ok, err = await join_vc_and_stream(
        cu, calls, actual, boosted, sk,
        is_video=False, host_id=uid if mode != "default" else "default"
    )
    if ok:
        asyncio.create_task(auto_leave_after_playback(calls, sk, actual, boosted))
        await proc.edit_text(f"✅ **NOW PLAYING** 🔥\n📻 {title}\n🔊 BOOST {VOLUME_BOOST}x")
    else:
        await proc.edit_text(f"❌ {err}")
    asyncio.create_task(cleanup_file(boosted))
    asyncio.create_task(cleanup_file(raw))
    state.step = None

# ═══════════════════════════════════════════════════════════════════════════════
# 🚀 STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
async def on_startup():
    await restore_sessions()
    logger.info("✅ All sessions restored from MongoDB")

def _banner():
    print("\n" + "="*68)
    print("🔥  ZUDO USERBOT — GOD-LEVEL EDITION v5 (ULTRA-INSTANT)")
    print("="*68)
    print(f"   👑 Primary Owner : {OWNER_ID}")
    print(f"   👑 Co-Owner      : {CO_OWNER_ID}")
    print(f"   🗄️  MongoDB       : {'OK' if MONGO_OK else 'FAIL'}")
    print(f"   🔊 Volume Boost  : {VOLUME_BOOST}x")
    print(f"   🔑 Bot Token     : {'*'*8}{BOT_TOKEN[-4:] if BOT_TOKEN else '????'}")
    print(f"   ⚡ Stream Mode   : MULTI-FALLBACK (yt-dlp rotation + nubcoder)")
    print(f"   🧩 Stream Types  : "
          f"MediaStream={'✅' if _MediaStream else '❌'} "
          f"AudioPiped={'✅' if _AudioPiped else '❌'} "
          f"AudioStream={'✅' if _AudioStream else '❌'}")
    print("="*68 + "\n")

if __name__ == "__main__":
    _banner()
    logger.info("🚀 Starting ZUDO USERBOT v5 …")

    async def _main():
        await bot.start()
        await on_startup()
        logger.info("✅ Bot is running. Press Ctrl+C to stop.")
        await idle()
        await bot.stop()

    try:
        asyncio.get_event_loop().run_until_complete(_main())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception as e:
        logger.error(f"Crash: {e}")
