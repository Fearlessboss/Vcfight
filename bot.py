
"""
🔥 ZUDO USERBOT — GOD-LEVEL EDITION v11 (FIXES OVER v10)

✦ FIX v11: MongoDB restore — sudo_users, music_hosts, default_account ALL load on startup
✦ FIX v11: ENTITY_BOUNDS_INVALID — all reply_text uses safe escaping for user input
✦ FIX v11: Auto-leave VC after song ends — silent leave when queue empty (duration-based fallback)
✦ FIX v11: parse_mode explicitly set to avoid markdown entity corruption
✦ FIX v11: All error messages sanitized — no raw exception text injected into markdown
✦ KEEP: All v10 features (per-userbot isolation, 1000-userbot ready, MongoDB persistence)
"""

import os
import re
import gc
import sys
import time
import uuid
import shutil
import asyncio
import logging
import functools
import requests
import traceback
import concurrent.futures
from collections import deque
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatType, ParseMode
from pyrogram.handlers import MessageHandler
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
)
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid,
    PasswordHashInvalid, FloodWait, UserAlreadyParticipant,
    InviteHashExpired, PeerIdInvalid, AuthKeyUnregistered,
    AuthKeyDuplicated, UserDeactivated, UserDeactivatedBan,
    ChannelPrivate, ChatAdminRequired
)

# ───── PyTgCalls imports (with backward-compat fallbacks) ─────
from pytgcalls import PyTgCalls
try:
    from pytgcalls import StreamType
except Exception:
    StreamType = None

_MediaStream = None
_AudioPiped  = None
_VideoPiped  = None
_HighQualityAudio = None
_HighQualityVideo = None
_AudioQuality = None
_VideoQuality = None

try:
    from pytgcalls.types import MediaStream as _MediaStream
except Exception:
    pass
try:
    from pytgcalls.types import AudioQuality as _AudioQuality
except Exception:
    pass
try:
    from pytgcalls.types import VideoQuality as _VideoQuality
except Exception:
    pass
try:
    from pytgcalls.types.input_stream import AudioPiped as _AudioPiped
except Exception:
    pass
try:
    from pytgcalls.types.input_stream import AudioVideoPiped as _VideoPiped
except Exception:
    pass
try:
    from pytgcalls.types.input_stream.quality import HighQualityAudio as _HighQualityAudio
except Exception:
    pass
try:
    from pytgcalls.types.input_stream.quality import HighQualityVideo as _HighQualityVideo
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

VOLUME_BOOST = _get_env("VOLUME_BOOST", default="2.0", cast=float)
ENABLE_AUDIO_BOOST = _get_env("ENABLE_AUDIO_BOOST", default="0", cast=int)

COOKIES_FILE  = "/app/cookies/cookies.txt"
DOWNLOADS_DIR = "/tmp/downloads"

MAX_RECENT_CACHE     = 50
MAX_DOWNLOAD_DIR_MB  = 800
GC_INTERVAL_SEC      = 180
WATCHDOG_INTERVAL    = 30

GLOBAL_DOWNLOAD_POOL = concurrent.futures.ThreadPoolExecutor(
    max_workers=32,
    thread_name_prefix="yt-dl"
)
GLOBAL_FFPROBE_POOL = concurrent.futures.ThreadPoolExecutor(
    max_workers=16,
    thread_name_prefix="ffprobe"
)

Path(DOWNLOADS_DIR).mkdir(exist_ok=True, parents=True)
Path("/app/sessions").mkdir(exist_ok=True, parents=True)
Path("/app/data").mkdir(exist_ok=True, parents=True)
Path("/app/cookies").mkdir(exist_ok=True, parents=True)

# ═══════════════════════════════════════════════════════════════════════════════
# 📜 LOGGING + LOG BUFFER
# ═══════════════════════════════════════════════════════════════════════════════
LOG_BUFFER = deque(maxlen=300)

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
        msg = record.getMessage()
        try:
            LOG_BUFFER.append(f"[{ts}] [{record.levelname}] {msg}")
        except Exception:
            pass
        return f"{color}[{ts}] [{record.levelname:<7}] {msg}{self.RESET}"

_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_ColorFmt())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
logger = logging.getLogger("ZUDO")

for noisy in ("pyrogram", "pytgcalls", "ntgcalls", "asyncio"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

def _global_exception_handler(loop, context):
    exc = context.get("exception")
    msg = context.get("message", "no-msg")
    if exc:
        logger.error(f"[GLOBAL-EXC] {msg}: {exc!r}")
    else:
        logger.error(f"[GLOBAL-EXC] {msg}")

# ═══════════════════════════════════════════════════════════════════════════════
# 🛡 SAFE TEXT HELPERS (v11 FIX for ENTITY_BOUNDS_INVALID)
# ═══════════════════════════════════════════════════════════════════════════════
def _escape_md(text) -> str:
    """Escape markdown special chars to prevent ENTITY_BOUNDS_INVALID."""
    if text is None:
        return ""
    s = str(text)
    # Escape markdown v1 special chars
    for ch in ('\\', '`', '*', '_', '[', ']'):
        s = s.replace(ch, '\\' + ch)
    return s

def _safe_truncate(text, max_len=200) -> str:
    """Truncate and strip markdown-breaking chars from raw error text."""
    if text is None:
        return ""
    s = str(text)
    # Strip markdown breakers entirely from raw error strings
    s = s.replace('`', "'").replace('*', '').replace('_', ' ').replace('[', '(').replace(']', ')')
    if len(s) > max_len:
        s = s[:max_len] + "..."
    return s

async def safe_reply(m_or_q, text, **kwargs):
    """Reply with markdown; on failure fallback to plain text."""
    try:
        if hasattr(m_or_q, "reply_text"):
            return await m_or_q.reply_text(text, **kwargs)
        elif hasattr(m_or_q, "edit_text"):
            return await m_or_q.edit_text(text, **kwargs)
    except Exception as e:
        # Fallback: send as plain text without parse_mode
        try:
            kwargs2 = dict(kwargs)
            kwargs2["parse_mode"] = ParseMode.DISABLED
            if hasattr(m_or_q, "reply_text"):
                return await m_or_q.reply_text(text, **kwargs2)
            elif hasattr(m_or_q, "edit_text"):
                return await m_or_q.edit_text(text, **kwargs2)
        except Exception as e2:
            logger.warning(f"safe_reply fallback err: {e2}")

async def safe_edit(msg, text, **kwargs):
    """Edit message safely; fallback to plain text on entity errors."""
    try:
        return await msg.edit_text(text, **kwargs)
    except Exception:
        try:
            kwargs2 = dict(kwargs)
            kwargs2["parse_mode"] = ParseMode.DISABLED
            return await msg.edit_text(text, **kwargs2)
        except Exception as e2:
            logger.warning(f"safe_edit fallback err: {e2}")

# ═══════════════════════════════════════════════════════════════════════════════
# 🗄️ MONGODB
# ═══════════════════════════════════════════════════════════════════════════════
try:
    mongo_client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=8000)
    mongo_client.admin.command("ping")
    db                = mongo_client.get_database()
    col_default       = db["default_account"]
    col_sessions      = db["user_sessions"]
    col_sudo          = db["sudo_users"]
    col_cache         = db["chat_cache"]
    col_music_hosts   = db["music_hosts"]
    col_music_sudo    = db["music_sudo"]
    MONGO_OK = True
    logger.info("✅ MongoDB connected")
except Exception as e:
    logger.error(f"❌ MongoDB connection failed: {e}")
    MONGO_OK = False

# ═══════════════════════════════════════════════════════════════════════════════
# 🤖 BOT CLIENT
# ═══════════════════════════════════════════════════════════════════════════════
bot = Client(
    "ZudoBot_v11",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
    workers=32,
    max_concurrent_transmissions=8,
    parse_mode=ParseMode.MARKDOWN,
)

# ═══════════════════════════════════════════════════════════════════════════════
# 🌐 GLOBAL STATE
# ═══════════════════════════════════════════════════════════════════════════════
default_account = None
default_calls   = None

user_accounts  = {}
user_calls     = {}

music_accounts = {}
music_calls    = {}
music_handler_refs = {}

USERBOT_SEMAPHORES = {}
USERBOT_DOWNLOAD_DIRS = {}

user_states    = {}
warmed_chats   = {}

active_streams = {}
prejoin_state  = {}
stream_owners  = {}

music_queues       = {}
music_now_playing  = {}
music_loop_mode    = {}
stream_start_time  = {}

# v11: auto-leave watchdog tasks per (host_id, chat_id)
music_autoleave_tasks = {}  # (host_id, chat_id) -> asyncio.Task

play_locks   = {}

RECENT_DL_CACHE = {}
RECENT_DL_TTL   = 30 * 60

BOT_START_TIME = time.time()


def _get_play_lock(host_id, chat_id):
    key = (host_id, chat_id)
    if key not in play_locks:
        play_locks[key] = asyncio.Lock()
    return play_locks[key]


def _get_userbot_sem(host_id):
    if host_id not in USERBOT_SEMAPHORES:
        USERBOT_SEMAPHORES[host_id] = asyncio.Semaphore(2)
    return USERBOT_SEMAPHORES[host_id]


def _get_userbot_dl_dir(host_id):
    if host_id not in USERBOT_DOWNLOAD_DIRS:
        path = os.path.join(DOWNLOADS_DIR, f"ub_{host_id}")
        Path(path).mkdir(exist_ok=True, parents=True)
        USERBOT_DOWNLOAD_DIRS[host_id] = path
    return USERBOT_DOWNLOAD_DIRS[host_id]


# ═══════════════════════════════════════════════════════════════════════════════
# 🛡 SAFE HANDLER DECORATOR (v11 — sanitized error text)
# ═══════════════════════════════════════════════════════════════════════════════
def safe_handler(func):
    @functools.wraps(func)
    async def wrapper(c, m, *a, **kw):
        try:
            return await func(c, m, *a, **kw)
        except (PeerIdInvalid, KeyError, ValueError) as e:
            logger.warning(f"[safe_handler] {func.__name__} peer/key err: {e}")
            try:
                await safe_reply(m, "⚠️ Peer error — try again or restart with .restart",
                                 parse_mode=ParseMode.DISABLED)
            except Exception:
                pass
        except FloodWait as e:
            logger.warning(f"[safe_handler] FloodWait {e.value}s in {func.__name__}")
            try:
                await safe_reply(m, f"⏳ FloodWait: {e.value}s. Try again later.",
                                 parse_mode=ParseMode.DISABLED)
            except Exception:
                pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[safe_handler] {func.__name__} crashed: {e}\n{traceback.format_exc()[:800]}")
            try:
                safe_err = _safe_truncate(e, 200)
                await safe_reply(m, f"❌ Error: {safe_err}", parse_mode=ParseMode.DISABLED)
            except Exception:
                pass
    return wrapper


# ═══════════════════════════════════════════════════════════════════════════════
# 💾 MONGO HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def _safe_mongo(fn):
    @functools.wraps(fn)
    def w(*a, **kw):
        if not MONGO_OK:
            return None
        try:
            return fn(*a, **kw)
        except Exception as e:
            logger.warning(f"mongo {fn.__name__} err: {e}")
            return None
    return w

@_safe_mongo
def mongo_save_sudo(uid):
    col_sudo.update_one({"_id": uid}, {"$set": {"_id": uid}}, upsert=True)

@_safe_mongo
def mongo_remove_sudo(uid):
    col_sudo.delete_one({"_id": uid})

def mongo_get_sudo():
    if not MONGO_OK: return set()
    try:
        return {d["_id"] for d in col_sudo.find({})}
    except Exception as e:
        logger.warning(f"mongo_get_sudo err: {e}")
        return set()

@_safe_mongo
def mongo_save_cache(key, chat_id, title):
    col_cache.update_one({"_id": key}, {"$set": {"chat_id": chat_id, "title": title}}, upsert=True)

def mongo_get_cache(key):
    if not MONGO_OK: return None
    try:
        d = col_cache.find_one({"_id": key})
        return (d["chat_id"], d["title"]) if d else None
    except: return None

def mongo_save_session(uid, phone, session_string, kind="vc"):
    if not MONGO_OK: return
    try:
        coll = col_sessions if kind != "default" else col_default
        doc_id = uid if kind != "default" else "default"
        coll.update_one(
            {"_id": doc_id},
            {"$set": {
                "user_id": uid, "phone": phone,
                "session_string": session_string, "kind": kind,
                "saved_at": datetime.utcnow()
            }}, upsert=True)
        logger.info(f"💾 Session saved: uid={uid} kind={kind}")
    except Exception as e:
        logger.error(f"mongo_save_session err: {e}")

def mongo_get_session(uid, kind="vc"):
    if not MONGO_OK: return None
    try:
        coll = col_sessions if kind != "default" else col_default
        doc_id = uid if kind != "default" else "default"
        return coll.find_one({"_id": doc_id})
    except: return None

def mongo_delete_session(uid, kind="vc"):
    if not MONGO_OK: return
    try:
        coll = col_sessions if kind != "default" else col_default
        doc_id = uid if kind != "default" else "default"
        coll.delete_one({"_id": doc_id})
    except: pass

@_safe_mongo
def mongo_save_music_host(uid):
    col_music_hosts.update_one({"_id": uid}, {"$set": {"_id": uid}}, upsert=True)

@_safe_mongo
def mongo_remove_music_host(uid):
    col_music_hosts.delete_one({"_id": uid})

def mongo_is_music_host(uid):
    if not MONGO_OK: return False
    try:
        return col_music_hosts.find_one({"_id": uid}) is not None
    except: return False

def mongo_get_all_music_hosts():
    if not MONGO_OK: return []
    try:
        return [d["_id"] for d in col_music_hosts.find({})]
    except: return []

@_safe_mongo
def mongo_add_music_sudo(host_id, sudo_id):
    col_music_sudo.update_one(
        {"_id": f"{host_id}:{sudo_id}"},
        {"$set": {"host_id": host_id, "sudo_id": sudo_id}}, upsert=True)

@_safe_mongo
def mongo_remove_music_sudo(host_id, sudo_id):
    col_music_sudo.delete_one({"_id": f"{host_id}:{sudo_id}"})

def mongo_get_music_sudo(host_id):
    if not MONGO_OK: return set()
    try:
        return {d["sudo_id"] for d in col_music_sudo.find({"host_id": host_id})}
    except: return set()

# v11 FIX: Load sudo_users from MongoDB at startup
sudo_users = mongo_get_sudo()
logger.info(f"🔑 Loaded {len(sudo_users)} sudo users from MongoDB")

# ═══════════════════════════════════════════════════════════════════════════════
# 🧠 USER STATE
# ═══════════════════════════════════════════════════════════════════════════════
class UserState:
    __slots__ = ("step", "data", "created_at")
    def __init__(self):
        self.step = None
        self.data = {}
        self.created_at = time.time()

def get_user_state(uid):
    if uid not in user_states:
        user_states[uid] = UserState()
    return user_states[uid]

def reset_user_state(uid):
    if uid in user_states:
        try:
            c = user_states[uid].data.get("client")
            if c:
                try: asyncio.create_task(c.disconnect())
                except: pass
        except: pass
        user_states.pop(uid, None)

def is_owner(uid):
    return uid in OWNERS

def is_authorized(uid):
    return uid in OWNERS or uid in sudo_users

# ═══════════════════════════════════════════════════════════════════════════════
# 🚑 PEER WARMUP / RESOLVE
# ═══════════════════════════════════════════════════════════════════════════════
async def warmup_peers(client: Client, label: str = "", force: bool = False):
    cid = str(id(client))
    if not force and cid in warmed_chats and warmed_chats[cid].get("done"):
        return
    try:
        count = 0
        async for _ in client.get_dialogs():
            count += 1
            if count > 400:
                break
        warmed_chats[cid] = {"done": True, "count": count}
        logger.info(f"🔥 [{label}] Warmed {count} peers")
    except Exception as e:
        logger.warning(f"warmup_peers [{label}] err: {e}")

async def ensure_peer(client: Client, chat_id: int):
    try:
        await client.get_chat(chat_id)
        return True
    except Exception:
        try:
            await warmup_peers(client, f"ensure-{chat_id}", force=True)
            await client.get_chat(chat_id)
            return True
        except Exception as e:
            logger.warning(f"ensure_peer({chat_id}) failed: {e}")
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
                    except: pass
                if username and getattr(chat, 'username', None):
                    if chat.username.lower() == username.lower():
                        return chat.id, chat.title
            except: continue
    except Exception as e:
        logger.warning(f"find_chat_in_dialogs err: {e}")
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
            except Exception: pass
            try: await client.join_chat(uname)
            except UserAlreadyParticipant: pass
            except Exception: pass
            try:
                chat = await client.get_chat(uname)
                mongo_save_cache(cache_key, chat.id, chat.title)
                return True, chat.id, chat.title, None, False
            except Exception as e:
                return False, None, None, f"❌ Cannot find @{uname}: {_safe_truncate(e, 100)}", False
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
                return False, None, None, f"❌ {_safe_truncate(e, 100)}", False
    except Exception as e:
        return False, None, None, f"❌ {_safe_truncate(e, 100)}", False

# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 DOWNLOAD RESOLVER
# ═══════════════════════════════════════════════════════════════════════════════
YDL_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

_YT_PLAYER_CLIENTS = [
    ["android_music", "android"],
    ["android", "web"],
    ["ios", "web"],
    ["tv_embedded", "web"],
    ["mweb", "web"],
    ["web"],
]
_VIDEO_PLAYER_CLIENTS = [
    ["android", "web"],
    ["ios", "web"],
    ["tv_embedded", "web"],
    ["web"],
]

def _is_url(s: str) -> bool:
    return bool(re.match(r'^https?://', s.strip(), re.I))

def _recent_cache_get(query: str):
    key = query.strip().lower()
    rec = RECENT_DL_CACHE.get(key)
    if not rec: return None
    if (time.time() - rec["ts"]) > RECENT_DL_TTL:
        RECENT_DL_CACHE.pop(key, None); return None
    if not os.path.exists(rec["file_path"]):
        RECENT_DL_CACHE.pop(key, None); return None
    return rec

def _recent_cache_put(query: str, info: dict):
    key = query.strip().lower()
    if len(RECENT_DL_CACHE) >= MAX_RECENT_CACHE:
        try:
            oldest = min(RECENT_DL_CACHE.items(), key=lambda x: x[1].get("ts", 0))
            RECENT_DL_CACHE.pop(oldest[0], None)
        except: pass
    RECENT_DL_CACHE[key] = {**info, "ts": time.time()}

def _make_dl_opts(idx: int, out_template: str, want_video: bool = False):
    if want_video:
        fmt = ('best[ext=mp4][height<=480]/best[height<=480]/best[ext=mp4]/best')
    else:
        fmt = 'bestaudio[ext=m4a]/bestaudio/best'
    clients_list = _VIDEO_PLAYER_CLIENTS if want_video else _YT_PLAYER_CLIENTS
    clients = clients_list[idx % len(clients_list)]
    opts = {
        "format": fmt,
        "outtmpl": out_template,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "geo_bypass": True,
        "nocheckcertificate": True,
        "ignoreerrors": False,
        "extract_flat": False,
        "socket_timeout": 25,
        "retries": 2,
        "fragment_retries": 2,
        "concurrent_fragment_downloads": 4,
        "user_agent": YDL_UA,
        "http_headers": {
            "User-Agent": YDL_UA,
            "Accept-Language": "en-US,en;q=0.9",
        },
        "extractor_args": {"youtube": {"player_client": clients}},
        "no_part": True,
    }
    if os.path.exists(COOKIES_FILE) and os.path.getsize(COOKIES_FILE) > 50:
        opts["cookiefile"] = COOKIES_FILE
    return opts, clients

def _yt_search_first(query: str, want_video: bool = False):
    if _is_url(query):
        return query
    return f"ytsearch1:{query}"

def _ydl_extract_sync(query: str, want_video: bool, dl_dir: str):
    fname = f"{uuid.uuid4().hex[:10]}.{'mp4' if want_video else 'm4a'}"
    out_path = os.path.join(dl_dir, fname)
    last_err = None
    clients_list = _VIDEO_PLAYER_CLIENTS if want_video else _YT_PLAYER_CLIENTS
    target = _yt_search_first(query, want_video)
    for idx in range(len(clients_list)):
        opts, clients_used = _make_dl_opts(idx, out_path, want_video=want_video)
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(target, download=True)
                if info and "entries" in info and info["entries"]:
                    info = info["entries"][0]
                title = info.get("title", "Unknown")
                duration = info.get("duration", 0) or 0
                actual = None
                for f in os.listdir(dl_dir):
                    if f.startswith(fname.split(".")[0]):
                        actual = os.path.join(dl_dir, f); break
                if not actual: actual = out_path
                size = os.path.getsize(actual) if os.path.exists(actual) else 0
                logger.info(f"✅ yt-dlp[{idx}/{clients_used}] '{title[:50]}' ({size/1024/1024:.1f}MB)")
                return actual, title, duration
        except Exception as e:
            last_err = str(e)[:200]
            logger.warning(f"yt-dlp[{idx}/{clients_used}] failed: {last_err}")
            continue
    raise RuntimeError(f"All yt-dlp clients failed. Last: {last_err}")

async def resolve_stream_instant(query: str, want_video: bool = False, host_id: int = 0):
    cache_key = query + ("|v" if want_video else "|a")
    cached = _recent_cache_get(cache_key)
    if cached:
        logger.info(f"♻️ Cache hit: {cached['title']}")
        return cached["file_path"], cached["title"], cached["duration"]

    dl_dir = _get_userbot_dl_dir(host_id) if host_id else DOWNLOADS_DIR
    sem = _get_userbot_sem(host_id) if host_id else None

    async def _do_download():
        logger.info(f"⚡ [ub={host_id}] Downloading: {query[:80]} (video={want_video})")
        loop = asyncio.get_event_loop()
        file_path, title, duration = await loop.run_in_executor(
            GLOBAL_DOWNLOAD_POOL,
            _ydl_extract_sync, query, want_video, dl_dir
        )
        return file_path, title, duration

    if sem:
        async with sem:
            file_path, title, duration = await _do_download()
    else:
        file_path, title, duration = await _do_download()

    _recent_cache_put(cache_key, {"file_path": file_path, "title": title, "duration": duration})
    return file_path, title, duration

# ═══════════════════════════════════════════════════════════════════════════════
# 🔊 AUDIO HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
async def boost_audio(input_path: str, gain: float = None) -> str:
    if not ENABLE_AUDIO_BOOST:
        return input_path
    gain = gain or VOLUME_BOOST
    try:
        out = input_path.rsplit(".", 1)[0] + "_BOOSTED.mp3"
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", input_path,
            "-filter:a", f"volume={gain}",
            "-b:a", "128k", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL)
        await proc.communicate()
        if os.path.exists(out) and os.path.getsize(out) > 0:
            try: os.remove(input_path)
            except: pass
            return out
        return input_path
    except Exception as e:
        logger.warning(f"Boost failed: {e}")
        return input_path

async def get_audio_duration(url_or_path: str) -> int:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", url_or_path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
            s = out.decode().strip()
            return int(float(s)) if s else 0
        except asyncio.TimeoutError:
            try: proc.kill()
            except: pass
            return 0
    except: return 0

# ═══════════════════════════════════════════════════════════════════════════════
# 🎯 STREAM BUILDER
# ═══════════════════════════════════════════════════════════════════════════════
def _build_stream_objs(source: str, is_video: bool = False):
    streams = []
    if is_video:
        if _MediaStream:
            try:
                kwargs = {}
                if _AudioQuality: kwargs["audio_parameters"] = _AudioQuality.HIGH
                if _VideoQuality:
                    vq = getattr(_VideoQuality, "SD_480p", None) or getattr(_VideoQuality, "HD_720p", None)
                    if vq: kwargs["video_parameters"] = vq
                streams.append(_MediaStream(source, **kwargs))
            except Exception:
                try: streams.append(_MediaStream(source))
                except: pass
        if _VideoPiped:
            try:
                if _HighQualityAudio and _HighQualityVideo:
                    streams.append(_VideoPiped(source, _HighQualityAudio(), _HighQualityVideo()))
                else:
                    streams.append(_VideoPiped(source))
            except: pass
    else:
        if _MediaStream:
            try:
                kwargs = {}
                if _AudioQuality: kwargs["audio_parameters"] = _AudioQuality.HIGH
                streams.append(_MediaStream(source, **kwargs))
            except Exception:
                try: streams.append(_MediaStream(source))
                except: pass
        if _AudioPiped:
            try:
                if _HighQualityAudio:
                    streams.append(_AudioPiped(source, _HighQualityAudio()))
                else:
                    streams.append(_AudioPiped(source))
            except: pass
    return [s for s in streams if s is not None]

# ═══════════════════════════════════════════════════════════════════════════════
# 🎯 JOIN VC + PLAY
# ═══════════════════════════════════════════════════════════════════════════════
async def _try_play_method(calls, method_name, chat_id, stream_obj):
    method = getattr(calls, method_name, None)
    if not method: return False, "method_missing"
    try:
        kwargs = {}
        if method_name == "join_group_call" and StreamType is not None:
            try: kwargs["stream_type"] = StreamType().pulse_stream
            except: pass
        result = method(chat_id, stream_obj, **kwargs) if kwargs else method(chat_id, stream_obj)
        if asyncio.iscoroutine(result):
            await result
        return True, None
    except Exception as e:
        return False, str(e)

async def join_vc_and_stream(client: Client, calls: PyTgCalls,
                             chat_id: int, source: str, account_key,
                             is_video: bool = False, owner_uid: int = None):
    try:
        await ensure_peer(client, chat_id)
    except Exception as e:
        logger.warning(f"ensure_peer pre-stream err: {e}")

    streams = _build_stream_objs(source, is_video)
    if not streams:
        return False, "No stream builder available"
    sk = (account_key, chat_id)

    if prejoin_state.get(sk) or sk in active_streams:
        for stream_obj in streams:
            try:
                method = (getattr(calls, "change_stream", None)
                          or getattr(calls, "set_stream", None)
                          or getattr(calls, "play", None))
                if method:
                    res = method(chat_id, stream_obj)
                    if asyncio.iscoroutine(res): await res
                    active_streams[sk] = chat_id
                    prejoin_state[sk] = True
                    stream_start_time[sk] = time.time()
                    if owner_uid is not None: stream_owners[sk] = owner_uid
                    logger.info(f"🔄 Switched stream in VC {chat_id} ({account_key})")
                    return True, None
            except Exception as e:
                emsg = str(e).lower()
                if "not in group call" in emsg or "no active" in emsg: break
                continue

    last_err = "unknown"
    for method_name in ("play", "join_group_call", "stream"):
        if not getattr(calls, method_name, None): continue
        for stream_obj in streams:
            ok, err = await _try_play_method(calls, method_name, chat_id, stream_obj)
            if ok:
                active_streams[sk] = chat_id
                prejoin_state[sk] = True
                stream_start_time[sk] = time.time()
                if owner_uid is not None: stream_owners[sk] = owner_uid
                logger.info(f"✅ Joined VC {chat_id} via {method_name} ({account_key})")
                return True, None
            last_err = err or "unknown"
            emsg = (err or "").lower()
            if "already" in emsg or "groupcall_join_missing" in emsg:
                try:
                    cs = getattr(calls, "change_stream", None)
                    if cs:
                        res = cs(chat_id, stream_obj)
                        if asyncio.iscoroutine(res): await res
                        active_streams[sk] = chat_id
                        prejoin_state[sk] = True
                        stream_start_time[sk] = time.time()
                        if owner_uid is not None: stream_owners[sk] = owner_uid
                        return True, None
                except Exception as e2:
                    last_err = f"change_stream fail: {e2}"
            if "peer" in emsg or "key_id" in emsg or "keyerror" in emsg:
                try:
                    await warmup_peers(client, f"retry-{sk}", force=True)
                    ok2, err2 = await _try_play_method(calls, method_name, chat_id, stream_obj)
                    if ok2:
                        active_streams[sk] = chat_id
                        prejoin_state[sk] = True
                        stream_start_time[sk] = time.time()
                        if owner_uid is not None: stream_owners[sk] = owner_uid
                        return True, None
                    last_err = err2 or last_err
                except Exception as e3:
                    last_err = f"warmup retry fail: {e3}"
            if any(x in emsg for x in ["no active group call", "group_call_invalid", "groupcall_forbidden"]):
                return False, "⚠️ No active VC in group. Start a voice chat first."
    return False, f"All play methods failed. Last: {_safe_truncate(last_err, 150)}"

async def safe_stop_calls(calls):
    if not calls: return
    for method in ("stop", "_stop", "terminate", "close"):
        fn = getattr(calls, method, None)
        if fn:
            try:
                res = fn()
                if asyncio.iscoroutine(res): await res
                return
            except: continue

def _clear_stream_state(account_key, chat_id):
    sk = (account_key, chat_id)
    active_streams.pop(sk, None)
    prejoin_state.pop(sk, None)
    stream_owners.pop(sk, None)
    stream_start_time.pop(sk, None)

# ═══════════════════════════════════════════════════════════════════════════════
# 📡 STREAM-END HANDLER + DURATION-BASED FALLBACK (v11 FIX for auto-leave)
# ═══════════════════════════════════════════════════════════════════════════════
def _register_stream_end_handler(calls: PyTgCalls, host_id: int, bot_client: Client):
    try:
        @calls.on_update()
        async def on_update(_, update):
            try:
                evt_name = type(update).__name__.lower()
                is_end = ("streamended" in evt_name or "audioended" in evt_name
                          or "videoended" in evt_name or "closed" in evt_name)
                if not is_end: return
                chat_id = getattr(update, "chat_id", None)
                if chat_id is None: return
                sk = (("music", host_id), chat_id)
                if sk not in active_streams:
                    logger.debug(f"Ignored stream-end for {sk} (not ours)")
                    return
                logger.info(f"🔔 Stream ended event: host={host_id}, chat={chat_id}")
                asyncio.create_task(_on_track_end(bot_client, host_id, chat_id))
            except Exception as e:
                logger.warning(f"on_update handler err: {e}")
    except Exception as e:
        logger.warning(f"Could not register stream-end handler for {host_id}: {e}")


# v11 NEW: Duration-based auto-leave fallback (silent, no message)
async def _schedule_music_autoleave(bot_client: Client, host_id: int, chat_id: int, duration: int):
    """
    Fallback watchdog: if stream-end event doesn't fire (PyTgCalls version mismatch),
    auto-trigger track-end check based on song duration + buffer.
    Silent — no message to chat on leave.
    """
    key = (host_id, chat_id)
    # Cancel any previous task for this key
    old = music_autoleave_tasks.pop(key, None)
    if old and not old.done():
        try: old.cancel()
        except: pass

    if duration <= 0:
        duration = 300  # safe default 5min

    async def _waiter():
        try:
            # Wait duration + small buffer
            await asyncio.sleep(duration + 8)
            # Check if same song still playing
            cur = music_now_playing.get(key)
            if not cur:
                return
            # If queue empty and still in VC → silent leave
            q = music_queues.get(key)
            sk = (("music", host_id), chat_id)
            if sk in active_streams:
                logger.info(f"⏱ Duration-based track-end: host={host_id}, chat={chat_id}")
                await _on_track_end(bot_client, host_id, chat_id)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning(f"_schedule_music_autoleave err: {e}")

    task = asyncio.create_task(_waiter())
    music_autoleave_tasks[key] = task


def _cancel_music_autoleave(host_id: int, chat_id: int):
    key = (host_id, chat_id)
    task = music_autoleave_tasks.pop(key, None)
    if task and not task.done():
        try: task.cancel()
        except: pass

# ═══════════════════════════════════════════════════════════════════════════════
# 👋 AUTO-LEAVE (VC Fight non-queue)
# ═══════════════════════════════════════════════════════════════════════════════
async def auto_leave_after_playback(calls, account_key, chat_id, source, cleanup_path=None):
    sk = (account_key, chat_id)
    try:
        dur = await get_audio_duration(source)
        wait_time = (dur + 5) if dur > 0 else 300
        await asyncio.sleep(wait_time)
        if sk in active_streams:
            try: await calls.leave_group_call(chat_id)
            except: pass
            _clear_stream_state(account_key, chat_id)
        if cleanup_path and os.path.exists(cleanup_path):
            try: os.remove(cleanup_path)
            except: pass
        logger.info(f"👋 Auto-left VC ({account_key}, {chat_id})")
    except Exception as e:
        logger.warning(f"auto-leave err: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 📜 HELP TEXT
# ═══════════════════════════════════════════════════════════════════════════════
HELP_TEXT = """
╔══════════════════════════════════════╗
║   🔥  ZUDO USERBOT v11  —  /help  🔥  ║
╚══════════════════════════════════════╝

━━━━━━━ 🤖  BOT (DM)  ━━━━━━━
  /start              • Open main menu
  /help, /commands    • This help message
  /stop               • Stop YOUR active VC stream
  /status             • Your sessions / streams
  /restart            • Restart your music userbot
  /logout             • Logout your account
  /uptime  /ping      • Bot uptime / latency
  /owners  /info      • Owners list / account info

OTP/2FA INPUT:
  📨 OTP: digits with spaces  →  e.g. 1 2 3 4 5
  🔐 2FA: send password as text

━━━━━━━ 👑  OWNER ONLY  ━━━━━━━
  /sudo <id>          • Add sudo user
  /rmsudo <id>        • Remove sudo user
  /sudolist           • List sudo users
  /restartall         • Restart ALL music userbots
  /stats              • Bot statistics
  /broadcast <txt>    • Broadcast to all hosts
  /cleancache         • Clear download cache
  /killall            • Stop all active streams

━━━━━━━ 🎵 MUSIC USERBOT (in groups, .) ━━━━━━━
  .play <q>           • Play audio (queue if busy)
  .vplay <q>          • Play video (queue if busy)
  .playforce <q>      • Force play (skip queue)
  .vplayforce <q>     • Force video play
  .skip, .next        • Skip to next
  .stop, .end         • Stop & leave VC
  .pause / .resume    • Pause / Resume
  .mute / .unmute     • Mute / Unmute
  .volume <1-200>     • Set volume
  .queue, .q          • Show queue
  .nowplaying, .np    • Current song
  .shuffle            • Shuffle queue
  .clearqueue, .cq    • Clear queue
  .loop               • Toggle loop
  .replay             • Replay current
  .lyrics [song]      • Get lyrics
  .search <q>         • Search YT

  .auth <id>          • Grant sudo
  .dauth <id>         • Revoke sudo
  .authlist           • Sudo list
  .ping / .alive      • Liveness
  .uptime             • Userbot uptime
  .id                 • Chat/user ID
  .info / .stats      • Userbot info / stats
  .restart            • Restart this userbot
  .leaveall           • Leave all VCs
  .help / .commands   • This help

━━━━━━━ 🛠 UTILITY (.) ━━━━━━━
  .joinvc / .leavevc  • Manual VC join/leave
  .reload             • Reload handlers
  .clean              • Clean temp files
  .speedtest          • Network speed test
  .sysinfo            • System resources
  .logs               • Recent logs
  .ytdl <q>           • Test YT-DL resolve

━━━━━━━ ⚡ Powered by @zudo_userbot ⚡ ━━━━━━━
"""

# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 MUSIC QUEUE SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════
def _is_caller_allowed(host_id: int, user_id: int) -> bool:
    if user_id == host_id or user_id in OWNERS: return True
    if user_id in mongo_get_music_sudo(host_id): return True
    return False

def _is_group_chat(m: Message) -> bool:
    try:
        return m.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP)
    except: return False

def _q_get(host_id, chat_id) -> deque:
    key = (host_id, chat_id)
    if key not in music_queues:
        music_queues[key] = deque()
    return music_queues[key]

async def _on_track_end(c: Client, host_id: int, chat_id: int):
    key = (host_id, chat_id)
    _cancel_music_autoleave(host_id, chat_id)
    current = music_now_playing.get(key)
    if current and music_loop_mode.get(key):
        q = _q_get(host_id, chat_id)
        q.appendleft(current)
    if current:
        try:
            p = current.get("file_path")
            if p and os.path.exists(p) and not music_loop_mode.get(key):
                os.remove(p)
        except: pass
    music_now_playing.pop(key, None)
    await _play_next_from_queue(c, host_id, chat_id)

async def _play_next_from_queue(c: Client, host_id: int, chat_id: int):
    q = _q_get(host_id, chat_id)
    if not q:
        # v11 FIX: Silent leave when queue empty (no message sent)
        calls = music_calls.get(host_id)
        try:
            if calls: await calls.leave_group_call(chat_id)
        except: pass
        _clear_stream_state(("music", host_id), chat_id)
        music_now_playing.pop((host_id, chat_id), None)
        _cancel_music_autoleave(host_id, chat_id)
        logger.info(f"📭 Queue empty for ({host_id}, {chat_id}) — silently left VC")
        return None
    song = q.popleft()
    calls = music_calls.get(host_id)
    if not calls: return None
    music_now_playing[(host_id, chat_id)] = song
    try:
        ok, err = await join_vc_and_stream(
            c, calls, chat_id, song["file_path"], ("music", host_id),
            is_video=song.get("is_video", False), owner_uid=song.get("requested_by"))
    except Exception as e:
        logger.error(f"_play_next_from_queue stream err: {e}")
        ok, err = False, str(e)
    if not ok:
        logger.error(f"Queue next play failed: {err}")
        try:
            await c.send_message(chat_id, f"❌ Skipped (failed): {_safe_truncate(song.get('title','?'), 80)}",
                                 parse_mode=ParseMode.DISABLED)
        except: pass
        return await _play_next_from_queue(c, host_id, chat_id)
    try:
        title_safe = _escape_md(song.get('title', 'Unknown'))
        by_safe = song.get('requested_by_mention', '?')
        await c.send_message(
            chat_id,
            f"▶️ **Now Playing** {'🎬' if song.get('is_video') else '🎵'}\n"
            f"**{title_safe}**\n"
            f"💬 By: {by_safe}\n"
            f"📊 Queue: {len(q)} left")
    except Exception as e:
        logger.warning(f"send now-playing msg err: {e}")
    # v11: Schedule duration-based fallback auto-leave
    asyncio.create_task(_schedule_music_autoleave(c, host_id, chat_id, song.get("duration", 0)))
    return song

async def _enqueue_or_play(c: Client, m: Message, host_id: int, query: str,
                           want_video: bool = False, force: bool = False):
    chat_id = m.chat.id
    if not _is_caller_allowed(host_id, m.from_user.id): return
    lock = _get_play_lock(host_id, chat_id)
    async with lock:
        proc = await safe_reply(m, "⚡ Searching…", parse_mode=ParseMode.DISABLED)
        try:
            file_path, title, duration = await resolve_stream_instant(
                query, want_video=want_video, host_id=host_id)
        except Exception as e:
            await safe_edit(proc, f"❌ Download failed: {_safe_truncate(e, 150)}",
                            parse_mode=ParseMode.DISABLED)
            return
        if not file_path:
            await safe_edit(proc, "❌ Couldn't fetch audio", parse_mode=ParseMode.DISABLED); return
        requester = m.from_user
        song = {
            "file_path": file_path, "title": title, "duration": duration,
            "is_video": want_video, "requested_by": requester.id,
            "requested_by_mention": requester.mention if hasattr(requester,"mention")
                else (f"@{requester.username}" if requester.username else requester.first_name),
            "added_at": time.time(),
        }
        key = (host_id, chat_id)
        if force:
            q = _q_get(host_id, chat_id)
            for s in q:
                try:
                    p = s.get("file_path")
                    if p and os.path.exists(p): os.remove(p)
                except: pass
            music_queues[key] = deque()
            current = music_now_playing.pop(key, None)
            _cancel_music_autoleave(host_id, chat_id)
            if current:
                try:
                    p = current.get("file_path")
                    if p and os.path.exists(p): os.remove(p)
                except: pass
        if music_now_playing.get(key) and not force:
            q = _q_get(host_id, chat_id)
            q.append(song)
            title_safe = _escape_md(title)
            try:
                await proc.edit_text(
                    f"➕ **Added to queue** (#{len(q)})\n"
                    f"🎵 **{title_safe}**\n💬 By: {song['requested_by_mention']}")
            except Exception:
                await safe_edit(proc, f"➕ Added to queue (#{len(q)})\n🎵 {title}",
                                parse_mode=ParseMode.DISABLED)
            return
        music_now_playing[key] = song
        calls = music_calls.get(host_id)
        if not calls:
            await safe_edit(proc, "❌ Music userbot calls not initialized. Try .restart",
                            parse_mode=ParseMode.DISABLED)
            return
        try:
            ok, err = await join_vc_and_stream(
                c, calls, chat_id, file_path, ("music", host_id),
                is_video=want_video, owner_uid=requester.id)
        except Exception as e:
            logger.error(f"_enqueue_or_play stream err: {e}")
            ok, err = False, str(e)
        if ok:
            title_safe = _escape_md(title)
            try:
                await proc.edit_text(
                    f"▶️ **Now Playing** {'🎬' if want_video else '🎵'}\n"
                    f"**{title_safe}**\n💬 By: {song['requested_by_mention']}")
            except Exception:
                await safe_edit(proc, f"▶️ Now Playing\n{title}\nBy: {song['requested_by_mention']}",
                                parse_mode=ParseMode.DISABLED)
            # v11: schedule fallback auto-leave
            asyncio.create_task(_schedule_music_autoleave(c, host_id, chat_id, duration))
        else:
            music_now_playing.pop(key, None)
            await safe_edit(proc, f"❌ Play failed: {_safe_truncate(err, 150)}",
                            parse_mode=ParseMode.DISABLED)
            try: os.remove(file_path)
            except: pass

# ═══════════════════════════════════════════════════════════════════════════════
# 🎤 MUSIC HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════
def register_music_handlers(client: Client, host_id: int):
    if host_id in music_handler_refs: return
    handler_refs = []
    USERBOT_START_TIME = time.time()

    @safe_handler
    async def _play(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if len(m.command) < 2:
            await safe_reply(m, "❌ Usage: .play <song name or URL>", parse_mode=ParseMode.DISABLED); return
        await _enqueue_or_play(c, m, host_id, " ".join(m.command[1:]), False, False)

    @safe_handler
    async def _vplay(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if len(m.command) < 2:
            await safe_reply(m, "❌ Usage: .vplay <song name or URL>", parse_mode=ParseMode.DISABLED); return
        await _enqueue_or_play(c, m, host_id, " ".join(m.command[1:]), True, False)

    @safe_handler
    async def _playforce(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or len(m.command) < 2:
            await safe_reply(m, "❌ Usage: .playforce <song>", parse_mode=ParseMode.DISABLED); return
        await _enqueue_or_play(c, m, host_id, " ".join(m.command[1:]), False, True)

    @safe_handler
    async def _vplayforce(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or len(m.command) < 2:
            await safe_reply(m, "❌ Usage: .vplayforce <song>", parse_mode=ParseMode.DISABLED); return
        await _enqueue_or_play(c, m, host_id, " ".join(m.command[1:]), True, True)

    @safe_handler
    async def _stop_cmd(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        chat_id = m.chat.id
        calls = music_calls.get(host_id)
        _cancel_music_autoleave(host_id, chat_id)
        try:
            if calls: await calls.leave_group_call(chat_id)
        except: pass
        _clear_stream_state(("music", host_id), chat_id)
        q = music_queues.pop((host_id, chat_id), None)
        if q:
            for s in q:
                try:
                    p = s.get("file_path")
                    if p and os.path.exists(p): os.remove(p)
                except: pass
        cur = music_now_playing.pop((host_id, chat_id), None)
        if cur:
            try:
                p = cur.get("file_path")
                if p and os.path.exists(p): os.remove(p)
            except: pass
        await safe_reply(m, "⏹ Stopped & left VC.", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _skip(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        chat_id = m.chat.id
        _cancel_music_autoleave(host_id, chat_id)
        current = music_now_playing.pop((host_id, chat_id), None)
        if current:
            try:
                p = current.get("file_path")
                if p and os.path.exists(p): os.remove(p)
            except: pass
        nxt = await _play_next_from_queue(c, host_id, chat_id)
        if not nxt:
            await safe_reply(m, "⏭ Skipped — queue empty, left VC.", parse_mode=ParseMode.DISABLED)
        else:
            await safe_reply(m, f"⏭ Skipped → ▶️ {_safe_truncate(nxt.get('title','?'), 80)}",
                             parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _pause(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        calls = music_calls.get(host_id)
        if not calls: return
        try:
            if hasattr(calls, "pause_stream"):
                await calls.pause_stream(m.chat.id)
            elif hasattr(calls, "pause"):
                await calls.pause(m.chat.id)
            await safe_reply(m, "⏸ Paused", parse_mode=ParseMode.DISABLED)
        except Exception as e:
            await safe_reply(m, f"❌ {_safe_truncate(e, 100)}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _resume(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        calls = music_calls.get(host_id)
        if not calls: return
        try:
            if hasattr(calls, "resume_stream"):
                await calls.resume_stream(m.chat.id)
            elif hasattr(calls, "resume"):
                await calls.resume(m.chat.id)
            await safe_reply(m, "▶️ Resumed", parse_mode=ParseMode.DISABLED)
        except Exception as e:
            await safe_reply(m, f"❌ {_safe_truncate(e, 100)}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _mute(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        calls = music_calls.get(host_id)
        try:
            if hasattr(calls, "mute_stream"):
                await calls.mute_stream(m.chat.id)
            await safe_reply(m, "🔇 Muted", parse_mode=ParseMode.DISABLED)
        except Exception as e:
            await safe_reply(m, f"❌ {_safe_truncate(e, 100)}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _unmute(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        calls = music_calls.get(host_id)
        try:
            if hasattr(calls, "unmute_stream"):
                await calls.unmute_stream(m.chat.id)
            await safe_reply(m, "🔊 Unmuted", parse_mode=ParseMode.DISABLED)
        except Exception as e:
            await safe_reply(m, f"❌ {_safe_truncate(e, 100)}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _volume(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        if len(m.command) < 2:
            await safe_reply(m, "❌ Usage: .volume <1-200>", parse_mode=ParseMode.DISABLED); return
        try:
            vol = max(1, min(200, int(m.command[1])))
        except ValueError:
            await safe_reply(m, "❌ Volume must be a number", parse_mode=ParseMode.DISABLED); return
        calls = music_calls[host_id]
        try:
            if hasattr(calls, "change_volume_call"):
                await calls.change_volume_call(m.chat.id, vol)
            elif hasattr(calls, "set_volume"):
                await calls.set_volume(m.chat.id, vol)
            else:
                await safe_reply(m, "❌ Volume API not supported.", parse_mode=ParseMode.DISABLED); return
            await safe_reply(m, f"🔊 Volume → {vol}%", parse_mode=ParseMode.DISABLED)
        except Exception as e:
            await safe_reply(m, f"❌ {_safe_truncate(e, 100)}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _queue_cmd(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        chat_id = m.chat.id
        current = music_now_playing.get((host_id, chat_id))
        q = _q_get(host_id, chat_id)
        if not current and not q:
            await safe_reply(m, "ℹ️ Queue empty.", parse_mode=ParseMode.DISABLED); return
        lines = ["🎶 Queue:\n"]
        if current:
            lines.append(f"▶️ Now: {_safe_truncate(current.get('title','?'), 80)} {'🎬' if current.get('is_video') else '🎵'}")
        for i, s in enumerate(list(q)[:20], 1):
            lines.append(f"{i}. {_safe_truncate(s.get('title','?'), 80)} {'🎬' if s.get('is_video') else '🎵'}")
        if len(q) > 20:
            lines.append(f"\n... and {len(q)-20} more")
        await safe_reply(m, "\n".join(lines), parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _np(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        current = music_now_playing.get((host_id, m.chat.id))
        if not current:
            await safe_reply(m, "ℹ️ Nothing playing.", parse_mode=ParseMode.DISABLED); return
        title_safe = _safe_truncate(current.get('title','?'), 150)
        await safe_reply(m,
            f"▶️ Now Playing {'🎬' if current.get('is_video') else '🎵'}\n"
            f"{title_safe}\n"
            f"💬 By: {current.get('requested_by_mention','?')}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _shuffle(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        import random
        q = _q_get(host_id, m.chat.id)
        if len(q) < 2:
            await safe_reply(m, "ℹ️ Need at least 2 songs.", parse_mode=ParseMode.DISABLED); return
        items = list(q); random.shuffle(items)
        music_queues[(host_id, m.chat.id)] = deque(items)
        await safe_reply(m, f"🔀 Shuffled {len(items)} songs.", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _clearqueue(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        q = music_queues.pop((host_id, m.chat.id), None)
        n = 0
        if q:
            n = len(q)
            for s in q:
                try:
                    p = s.get("file_path")
                    if p and os.path.exists(p): os.remove(p)
                except: pass
        await safe_reply(m, f"🗑 Cleared {n} from queue.", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _loop(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        key = (host_id, m.chat.id)
        music_loop_mode[key] = not music_loop_mode.get(key, False)
        await safe_reply(m, f"🔁 Loop: {'ON' if music_loop_mode[key] else 'OFF'}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _replay(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        current = music_now_playing.get((host_id, m.chat.id))
        if not current:
            await safe_reply(m, "ℹ️ Nothing playing.", parse_mode=ParseMode.DISABLED); return
        calls = music_calls.get(host_id)
        ok, err = await join_vc_and_stream(
            c, calls, m.chat.id, current["file_path"], ("music", host_id),
            is_video=current.get("is_video", False), owner_uid=current.get("requested_by"))
        if ok:
            await safe_reply(m, f"🔁 Replaying: {_safe_truncate(current.get('title','?'), 100)}",
                             parse_mode=ParseMode.DISABLED)
            asyncio.create_task(_schedule_music_autoleave(c, host_id, m.chat.id, current.get("duration", 0)))
        else:
            await safe_reply(m, f"❌ Replay failed: {_safe_truncate(err, 100)}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _lyrics(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        if len(m.command) < 2:
            current = music_now_playing.get((host_id, m.chat.id))
            if not current:
                await safe_reply(m, "❌ Usage: .lyrics <song>", parse_mode=ParseMode.DISABLED); return
            query = current.get("title", "")
            if not query:
                await safe_reply(m, "❌ Usage: .lyrics <song>", parse_mode=ParseMode.DISABLED); return
        else:
            query = " ".join(m.command[1:])
        proc = await safe_reply(m, "🔎 Fetching lyrics…", parse_mode=ParseMode.DISABLED)
        try:
            r = await asyncio.get_event_loop().run_in_executor(
                GLOBAL_DOWNLOAD_POOL,
                lambda: requests.get(f"https://api.lyrics.ovh/v1/{quote(query)}", timeout=15))
            if r.status_code == 200:
                data = r.json()
                lyrics = data.get("lyrics", "")[:3500]
                if lyrics:
                    await safe_edit(proc, f"📜 {query}\n\n{lyrics}", parse_mode=ParseMode.DISABLED); return
            await safe_edit(proc, "❌ Lyrics not found.", parse_mode=ParseMode.DISABLED)
        except Exception as e:
            await safe_edit(proc, f"❌ {_safe_truncate(e, 100)}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _search(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        if len(m.command) < 2:
            await safe_reply(m, "❌ Usage: .search <query>", parse_mode=ParseMode.DISABLED); return
        query = " ".join(m.command[1:])
        proc = await safe_reply(m, f"🔎 Searching: {_safe_truncate(query, 80)}", parse_mode=ParseMode.DISABLED)
        def _do_search():
            opts = {"quiet": True, "no_warnings": True, "skip_download": True,
                    "extract_flat": True, "default_search": "ytsearch5",
                    "socket_timeout": 15}
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(f"ytsearch5:{query}", download=False)
        try:
            info = await asyncio.get_event_loop().run_in_executor(GLOBAL_DOWNLOAD_POOL, _do_search)
            entries = info.get("entries", []) if info else []
            if not entries:
                await safe_edit(proc, "❌ No results.", parse_mode=ParseMode.DISABLED); return
            lines = [f"🔎 Top results for: {_safe_truncate(query, 60)}\n"]
            for i, e in enumerate(entries[:5], 1):
                lines.append(f"{i}. {_safe_truncate(e.get('title', '?'), 60)}")
            await safe_edit(proc, "\n".join(lines), parse_mode=ParseMode.DISABLED)
        except Exception as e:
            await safe_edit(proc, f"❌ Search failed: {_safe_truncate(e, 100)}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _auth(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS: return
        target = None
        if m.reply_to_message and m.reply_to_message.from_user:
            target = m.reply_to_message.from_user.id
        elif len(m.command) > 1:
            try:
                u = await c.get_users(m.command[1].replace("@", ""))
                target = u.id
            except: pass
        if not target:
            await safe_reply(m, "❌ Reply to user or give user_id/@username", parse_mode=ParseMode.DISABLED); return
        mongo_add_music_sudo(host_id, target)
        await safe_reply(m, f"✅ Auth granted: {target}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _dauth(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS: return
        target = None
        if m.reply_to_message and m.reply_to_message.from_user:
            target = m.reply_to_message.from_user.id
        elif len(m.command) > 1:
            try:
                u = await c.get_users(m.command[1].replace("@", ""))
                target = u.id
            except: pass
        if not target:
            await safe_reply(m, "❌ Reply or give user_id", parse_mode=ParseMode.DISABLED); return
        mongo_remove_music_sudo(host_id, target)
        await safe_reply(m, f"✅ Auth removed: {target}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _authlist(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS: return
        ids = mongo_get_music_sudo(host_id)
        if not ids:
            await safe_reply(m, "ℹ️ No sudo users.", parse_mode=ParseMode.DISABLED); return
        txt = "👥 Sudo list:\n" + "\n".join(f"• {i}" for i in ids)
        await safe_reply(m, txt, parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _ping(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if not _is_caller_allowed(host_id, m.from_user.id): return
        t0 = time.time()
        msg = await safe_reply(m, "🏓 Pong!", parse_mode=ParseMode.DISABLED)
        ms = (time.time() - t0) * 1000
        await safe_edit(msg, f"🏓 Pong! {ms:.0f}ms\n🎵 Music Userbot Alive 🔥", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _alive(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if not _is_caller_allowed(host_id, m.from_user.id): return
        up = time.time() - USERBOT_START_TIME
        await safe_reply(m,
            f"✅ Alive!\n⏱ Uptime: {int(up//3600)}h {int((up%3600)//60)}m\n🎵 ZUDO v11",
            parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _uptime(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if not _is_caller_allowed(host_id, m.from_user.id): return
        up = time.time() - USERBOT_START_TIME
        h = int(up // 3600); mins = int((up % 3600) // 60); s = int(up % 60)
        await safe_reply(m, f"⏱ Uptime: {h}h {mins}m {s}s", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _id_cmd(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if not _is_caller_allowed(host_id, m.from_user.id): return
        txt = f"💬 Chat ID: {m.chat.id}\n👤 Your ID: {m.from_user.id}"
        if m.reply_to_message and m.reply_to_message.from_user:
            txt += f"\n🔁 Reply ID: {m.reply_to_message.from_user.id}"
        await safe_reply(m, txt, parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _info(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if not _is_caller_allowed(host_id, m.from_user.id): return
        await safe_reply(m,
            f"🎵 Music Userbot Info\n\n"
            f"👤 Host: {host_id}\n"
            f"📊 Active Streams: {len([k for k in active_streams if k[0]==('music',host_id)])}\n"
            f"📜 Queues: {len([k for k in music_queues if k[0]==host_id])}\n"
            f"🔥 ZUDO v11", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _stats(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if not _is_caller_allowed(host_id, m.from_user.id): return
        my_streams = [k for k in active_streams if k[0]==("music",host_id)]
        my_queues = [k for k in music_queues if k[0]==host_id]
        total_q = sum(len(music_queues[k]) for k in my_queues)
        await safe_reply(m,
            f"📊 Userbot Stats\n\n"
            f"• Active VCs: {len(my_streams)}\n"
            f"• Active Queues: {len(my_queues)}\n"
            f"• Total Queued: {total_q}\n"
            f"• Sudo Users: {len(mongo_get_music_sudo(host_id))}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _leaveall(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS: return
        calls = music_calls.get(host_id)
        count = 0
        for sk in list(active_streams.keys()):
            if sk[0] == ("music", host_id):
                _cancel_music_autoleave(host_id, sk[1])
                try:
                    if calls: await calls.leave_group_call(sk[1])
                except: pass
                _clear_stream_state(("music", host_id), sk[1])
                count += 1
        for key in list(music_queues.keys()):
            if key[0] == host_id:
                q = music_queues.pop(key, None)
                if q:
                    for s in q:
                        try:
                            p = s.get("file_path")
                            if p and os.path.exists(p): os.remove(p)
                        except: pass
        for key in list(music_now_playing.keys()):
            if key[0] == host_id:
                cur = music_now_playing.pop(key, None)
                if cur:
                    try:
                        p = cur.get("file_path")
                        if p and os.path.exists(p): os.remove(p)
                    except: pass
        await safe_reply(m, f"👋 Left {count} voice chats.", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _joinvc(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        await safe_reply(m, "ℹ️ Use .play <song> to join VC with audio.", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _leavevc(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id): return
        await _stop_cmd(c, m)

    @safe_handler
    async def _help_cmd(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if not _is_caller_allowed(host_id, m.from_user.id): return
        await safe_reply(m, f"```\n{HELP_TEXT}\n```")

    @safe_handler
    async def _restart_inline(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS: return
        msg = await safe_reply(m, "♻️ Restarting your music userbot…", parse_mode=ParseMode.DISABLED)
        ok = await restart_music_userbot(host_id)
        if ok:
            await safe_edit(msg, "✅ Music userbot restarted!", parse_mode=ParseMode.DISABLED)
        else:
            await safe_edit(msg, "❌ Restart failed. Try /start to re-host.", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _clean(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS: return
        cleaned = _clean_downloads_dir(force=True)
        gc.collect()
        await safe_reply(m, f"🧹 Cleaned {cleaned} temp files + GC.", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _sysinfo(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if not _is_caller_allowed(host_id, m.from_user.id): return
        try:
            import psutil
            mem = psutil.virtual_memory()
            txt = (f"💻 System Info\n"
                   f"• CPU: {psutil.cpu_percent()}%\n"
                   f"• RAM: {mem.percent}% ({mem.used//1024//1024}M / {mem.total//1024//1024}M)\n"
                   f"• Disk: {psutil.disk_usage('/').percent}%\n"
                   f"• Threads (DL Pool): {GLOBAL_DOWNLOAD_POOL._max_workers}\n"
                   f"• Active Userbots: {len(music_accounts)}")
        except ImportError:
            txt = "⚠️ psutil not installed"
        await safe_reply(m, txt, parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _speedtest(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if not _is_caller_allowed(host_id, m.from_user.id): return
        proc = await safe_reply(m, "📡 Testing download speed…", parse_mode=ParseMode.DISABLED)
        try:
            url = "https://speed.cloudflare.com/__down?bytes=10000000"
            t0 = time.time()
            def _dl():
                r = requests.get(url, timeout=30, stream=True)
                size = 0
                for chunk in r.iter_content(64*1024):
                    size += len(chunk)
                return size
            size = await asyncio.get_event_loop().run_in_executor(GLOBAL_DOWNLOAD_POOL, _dl)
            elapsed = time.time() - t0
            mbps = (size * 8) / (elapsed * 1_000_000)
            await safe_edit(proc,
                f"📡 Speed Test\n"
                f"• Downloaded: {size/1024/1024:.1f} MB\n"
                f"• Time: {elapsed:.2f}s\n"
                f"• Speed: {mbps:.2f} Mbps", parse_mode=ParseMode.DISABLED)
        except Exception as e:
            await safe_edit(proc, f"❌ {_safe_truncate(e, 100)}", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _logs(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS: return
        if not LOG_BUFFER:
            await safe_reply(m, "ℹ️ No logs.", parse_mode=ParseMode.DISABLED); return
        recent = list(LOG_BUFFER)[-30:]
        txt = "\n".join(recent)
        if len(txt) > 3800: txt = txt[-3800:]
        # Sanitize for code block — strip backticks
        txt = txt.replace('`', "'")
        await safe_reply(m, f"📜 Recent logs:\n\n```\n{txt}\n```")

    @safe_handler
    async def _reload(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if m.from_user.id != host_id and m.from_user.id not in OWNERS: return
        unregister_music_handlers(client, host_id)
        register_music_handlers(client, host_id)
        await safe_reply(m, "✅ Handlers reloaded.", parse_mode=ParseMode.DISABLED)

    @safe_handler
    async def _ytdl(c, m: Message):
        if not _is_group_chat(m) or not m.from_user: return
        if not _is_caller_allowed(host_id, m.from_user.id): return
        if len(m.command) < 2:
            await safe_reply(m, "❌ Usage: .ytdl <query>", parse_mode=ParseMode.DISABLED); return
        q = " ".join(m.command[1:])
        proc = await safe_reply(m, "⏳ Resolving…", parse_mode=ParseMode.DISABLED)
        t0 = time.time()
        try:
            fp, title, dur = await resolve_stream_instant(q, host_id=host_id)
            el = time.time() - t0
            await safe_edit(proc,
                f"✅ {_safe_truncate(title, 100)}\n"
                f"Duration: {dur}s\nTime: {el:.2f}s\n"
                f"File: {os.path.basename(fp)}", parse_mode=ParseMode.DISABLED)
        except Exception as e:
            await safe_edit(proc, f"❌ {_safe_truncate(e, 150)}", parse_mode=ParseMode.DISABLED)

    handlers_to_add = [
        (_play,            ["play", "p"]),
        (_vplay,           ["vplay", "vp"]),
        (_playforce,       ["playforce", "pf", "fplay"]),
        (_vplayforce,      ["vplayforce", "vpf", "fvplay"]),
        (_stop_cmd,        ["stop", "end"]),
        (_skip,            ["skip", "next"]),
        (_pause,           ["pause"]),
        (_resume,          ["resume"]),
        (_mute,            ["mute"]),
        (_unmute,          ["unmute"]),
        (_volume,          ["volume", "vol"]),
        (_queue_cmd,       ["queue", "q"]),
        (_np,              ["nowplaying", "np", "current"]),
        (_shuffle,         ["shuffle"]),
        (_clearqueue,      ["clearqueue", "cq"]),
        (_loop,            ["loop"]),
        (_replay,          ["replay"]),
        (_lyrics,          ["lyrics"]),
        (_search,          ["search"]),
        (_auth,            ["auth"]),
        (_dauth,           ["dauth"]),
        (_authlist,        ["authlist"]),
        (_ping,            ["ping"]),
        (_alive,           ["alive"]),
        (_uptime,          ["uptime"]),
        (_id_cmd,          ["id"]),
        (_info,            ["info"]),
        (_stats,           ["stats"]),
        (_leaveall,        ["leaveall"]),
        (_joinvc,          ["joinvc"]),
        (_leavevc,         ["leavevc"]),
        (_help_cmd,        ["help", "commands"]),
        (_restart_inline,  ["restart"]),
        (_clean,           ["clean"]),
        (_sysinfo,         ["sysinfo"]),
        (_speedtest,       ["speedtest"]),
        (_logs,            ["logs"]),
        (_reload,          ["reload"]),
        (_ytdl,            ["ytdl"]),
    ]
    for func, cmds in handlers_to_add:
        h = MessageHandler(func, filters.command(cmds, prefixes="."))
        client.add_handler(h)
        handler_refs.append(h)
    music_handler_refs[host_id] = handler_refs
    calls = music_calls.get(host_id)
    if calls:
        _register_stream_end_handler(calls, host_id, client)
    logger.info(f"✅ Music handlers attached for host {host_id} ({len(handler_refs)} cmds)")

def unregister_music_handlers(client: Client, host_id: int):
    refs = music_handler_refs.pop(host_id, [])
    for h in refs:
        try: client.remove_handler(h)
        except: pass
    if refs:
        logger.info(f"🧹 Removed {len(refs)} handlers for {host_id}")

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
        try: unregister_music_handlers(old_client, host_id)
        except: pass
        try:
            if old_client.is_connected:
                await old_client.stop()
        except Exception as e:
            logger.warning(f"old client stop err: {e}")
        music_accounts.pop(host_id, None)
    for sk in list(active_streams.keys()):
        if isinstance(sk, tuple) and sk[0] == ("music", host_id):
            _cancel_music_autoleave(host_id, sk[1])
            _clear_stream_state(("music", host_id), sk[1])
    for key in list(music_queues.keys()):
        if key[0] == host_id:
            q = music_queues.pop(key, None)
            if q:
                for s in q:
                    try:
                        p = s.get("file_path")
                        if p and os.path.exists(p): os.remove(p)
                    except: pass
    for key in list(music_now_playing.keys()):
        if key[0] == host_id:
            cur = music_now_playing.pop(key, None)
            if cur:
                try:
                    p = cur.get("file_path")
                    if p and os.path.exists(p): os.remove(p)
                except: pass
    sess_doc = mongo_get_session(host_id, "music")
    if not sess_doc or not sess_doc.get("session_string"):
        logger.error(f"No saved music session for {host_id}")
        return False
    try:
        c = Client(
            f"music_{host_id}_{int(time.time())}",
            api_id=API_ID, api_hash=API_HASH,
            session_string=sess_doc["session_string"], in_memory=True)
        await c.start()
        await warmup_peers(c, f"restart-{host_id}", force=True)
        calls = PyTgCalls(c)
        await calls.start()
        music_accounts[host_id] = c
        music_calls[host_id] = calls
        register_music_handlers(c, host_id)
        logger.info(f"✅ Music userbot for {host_id} restarted")
        return True
    except (AuthKeyUnregistered, AuthKeyDuplicated,
            UserDeactivated, UserDeactivatedBan) as e:
        logger.error(f"❌ Session dead for {host_id}: {e}. Removing.")
        mongo_delete_session(host_id, "music")
        mongo_remove_music_host(host_id)
        return False
    except Exception as e:
        logger.error(f"Restart fail for {host_id}: {e}")
        return False

# ═══════════════════════════════════════════════════════════════════════════════
# 🔄 RESTORE SESSIONS (v11 FIX — robust, comprehensive load from MongoDB)
# ═══════════════════════════════════════════════════════════════════════════════
async def restore_sessions():
    global default_account, default_calls, sudo_users

    if not MONGO_OK:
        logger.warning("⚠️ Mongo not connected, can't restore sessions")
        return

    # v11 FIX: Reload sudo_users fresh from MongoDB (in case of stale state)
    try:
        fresh_sudo = mongo_get_sudo()
        sudo_users.clear()
        sudo_users.update(fresh_sudo)
        logger.info(f"🔑 Restored {len(sudo_users)} sudo users from MongoDB")
    except Exception as e:
        logger.error(f"sudo restore err: {e}")

    # v11 FIX: Restore default account
    try:
        d = col_default.find_one({"_id": "default"})
    except Exception as e:
        logger.error(f"default fetch err: {e}")
        d = None

    if d and d.get("session_string"):
        try:
            c = Client("default_session_restored",
                       api_id=API_ID, api_hash=API_HASH,
                       session_string=d["session_string"], in_memory=True)
            await c.start()
            await warmup_peers(c, "default", force=True)
            default_account = c
            default_calls = PyTgCalls(c)
            await default_calls.start()
            logger.info("✅ Default account restored from MongoDB")
        except (AuthKeyUnregistered, AuthKeyDuplicated,
                UserDeactivated, UserDeactivatedBan) as e:
            logger.error(f"❌ Default session DEAD ({e}). Removing.")
            try: col_default.delete_one({"_id": "default"})
            except: pass
        except Exception as e:
            logger.error(f"Default restore fail: {e}")
    else:
        logger.info("ℹ️ No default account session found in MongoDB")

    # v11 FIX: Restore ALL user sessions (vc + music)
    try:
        sessions = list(col_sessions.find({}))
        logger.info(f"🔍 Found {len(sessions)} user sessions in MongoDB")
    except Exception as e:
        logger.error(f"sessions fetch err: {e}")
        sessions = []

    async def _restore_one(s):
        uid = s.get("user_id")
        kind = s.get("kind", "vc")
        sess = s.get("session_string")
        if not uid or not sess:
            logger.warning(f"Skipping session — missing uid/sess: {s.get('_id')}")
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
                # Ensure host is registered
                if not mongo_is_music_host(uid):
                    mongo_save_music_host(uid)
                logger.info(f"✅ Music userbot restored for {uid}")
            else:
                user_accounts[uid] = c
                user_calls[uid] = calls
                logger.info(f"✅ VC user restored for {uid}")
        except (AuthKeyUnregistered, AuthKeyDuplicated,
                UserDeactivated, UserDeactivatedBan) as e:
            logger.error(f"❌ Session DEAD for {uid} ({kind}): {e}. Removing.")
            try:
                mongo_delete_session(uid, kind)
                if kind == "music": mongo_remove_music_host(uid)
            except: pass
        except Exception as e:
            logger.error(f"⚠️ Restore fail for {uid} ({kind}): {e}")

    if sessions:
        sem = asyncio.Semaphore(5)
        async def _bounded(s):
            async with sem:
                try:
                    await _restore_one(s)
                except Exception as e:
                    logger.error(f"bounded restore err: {e}")
        await asyncio.gather(*[_bounded(s) for s in sessions], return_exceptions=True)
        logger.info(f"✅ Session restore complete: {len(music_accounts)} music, {len(user_accounts)} vc")
    else:
        logger.info("ℹ️ No user sessions to restore")

    # v11 FIX: Also restore music hosts that may have lost their session reference
    try:
        all_hosts = mongo_get_all_music_hosts()
        logger.info(f"🎤 Total music hosts registered in MongoDB: {len(all_hosts)}")
        # Check for hosts without active client
        for hid in all_hosts:
            if hid not in music_accounts:
                logger.warning(f"⚠️ Host {hid} registered but no session loaded — may need re-host")
    except Exception as e:
        logger.error(f"music_hosts check err: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 🧹 JANITOR + WATCHDOG
# ═══════════════════════════════════════════════════════════════════════════════
def _clean_downloads_dir(force: bool = False):
    cleaned = 0
    try:
        all_dirs = [DOWNLOADS_DIR] + list(USERBOT_DOWNLOAD_DIRS.values())
        total_size = 0
        files = []
        for d in all_dirs:
            if not os.path.isdir(d): continue
            for f in os.listdir(d):
                fp = os.path.join(d, f)
                if not os.path.isfile(fp): continue
                try:
                    st = os.stat(fp)
                    total_size += st.st_size
                    files.append((fp, st.st_mtime, st.st_size))
                except: pass
        if not force and total_size < MAX_DOWNLOAD_DIR_MB * 1024 * 1024:
            now = time.time()
            for fp, mtime, _ in files:
                if (now - mtime) > RECENT_DL_TTL:
                    in_use = False
                    for song in list(music_now_playing.values()):
                        if song.get("file_path") == fp: in_use = True; break
                    if in_use: continue
                    for q in list(music_queues.values()):
                        for s in q:
                            if s.get("file_path") == fp: in_use = True; break
                        if in_use: break
                    if in_use: continue
                    try: os.remove(fp); cleaned += 1
                    except: pass
        else:
            files.sort(key=lambda x: x[1])
            for fp, _, _ in files:
                in_use = False
                for song in list(music_now_playing.values()):
                    if song.get("file_path") == fp: in_use = True; break
                if in_use: continue
                for q in list(music_queues.values()):
                    for s in q:
                        if s.get("file_path") == fp: in_use = True; break
                    if in_use: break
                if in_use: continue
                try: os.remove(fp); cleaned += 1
                except: pass
    except Exception as e:
        logger.warning(f"clean_downloads_dir err: {e}")
    return cleaned

async def background_janitor():
    while True:
        try:
            await asyncio.sleep(GC_INTERVAL_SEC)
            cleaned = _clean_downloads_dir(force=False)
            now = time.time()
            for k in list(RECENT_DL_CACHE.keys()):
                if now - RECENT_DL_CACHE[k].get("ts", 0) > RECENT_DL_TTL:
                    RECENT_DL_CACHE.pop(k, None)
            for uid in list(user_states.keys()):
                if now - user_states[uid].created_at > 1800 and not user_states[uid].step:
                    user_states.pop(uid, None)
            collected = gc.collect()
            if cleaned or collected:
                logger.info(f"🧹 Janitor: cleaned={cleaned}, gc={collected}, cache={len(RECENT_DL_CACHE)}, userbots={len(music_accounts)}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Janitor error: {e}")

async def watchdog():
    while True:
        try:
            await asyncio.sleep(WATCHDOG_INTERVAL)
            now = time.time()
            for sk in list(stream_start_time.keys()):
                start = stream_start_time.get(sk, now)
                if now - start > 7200:
                    logger.warning(f"⚠️ Stream {sk} running >2h — force cleanup")
                    account_key, chat_id = sk
                    calls = None
                    if account_key == "default": calls = default_calls
                    elif isinstance(account_key, tuple):
                        if account_key[0] == "vc":
                            calls = user_calls.get(account_key[1])
                        elif account_key[0] == "music":
                            calls = music_calls.get(account_key[1])
                            _cancel_music_autoleave(account_key[1], chat_id)
                    try:
                        if calls: await calls.leave_group_call(chat_id)
                    except: pass
                    _clear_stream_state(account_key, chat_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"watchdog err: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 📨 BOT COMMANDS (DM)
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_message(filters.command("start") & filters.private)
@safe_handler
async def start_command(client, m: Message):
    if not is_authorized(m.from_user.id):
        await safe_reply(m, "❌ You don't have permission to use this bot!",
                         parse_mode=ParseMode.DISABLED); return
    txt = (
        "╔══════════════════════════════╗\n"
        "║   🎵  **ZUDO USERBOT v11**  🎵   ║\n"
        "╚══════════════════════════════╝\n\n"
        "👋 Welcome. Choose your weapon:\n\n"
        "⚔️  **VC Fight** — Stream audio in any voice chat\n"
        "🎵 **Music Userbot** — Personal music bot (.play .skip…)\n\n"
        "Type /help for full command list.\n\n"
        "🔥 Powered by @zudo_userbot")
    try:
        await m.reply_text(txt, reply_markup=build_welcome_keyboard())
    except Exception:
        await m.reply_text(txt.replace('*', ''), reply_markup=build_welcome_keyboard(),
                           parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command(["help","commands"]) & filters.private)
@safe_handler
async def help_cmd(c, m: Message):
    if not is_authorized(m.from_user.id): return
    await safe_reply(m, f"```\n{HELP_TEXT}\n```")

@bot.on_message(filters.command("owners") & filters.private)
@safe_handler
async def owners_cmd(c, m: Message):
    if not is_authorized(m.from_user.id): return
    lines = ["👑 Bot Owners:"]
    for oid in OWNERS:
        try:
            u = await c.get_users(oid)
            lines.append(f"• {u.first_name} ({oid})")
        except:
            lines.append(f"• {oid}")
    await safe_reply(m, "\n".join(lines), parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("status") & filters.private)
@safe_handler
async def status_cmd(c, m: Message):
    if not is_authorized(m.from_user.id): return
    uid = m.from_user.id
    has_vc    = uid in user_accounts
    has_music = uid in music_accounts
    has_def   = default_account is not None
    my_streams = [sk for sk, owner in stream_owners.items() if owner == uid]
    txt = (f"📊 Status\n\n"
           f"• Default Account : {'✅' if has_def else '❌'}\n"
           f"• VC Fight (you)  : {'✅' if has_vc else '❌'}\n"
           f"• Music Userbot   : {'✅' if has_music else '❌'}\n"
           f"• Your Streams    : {len(my_streams)}\n"
           f"• Total Streams   : {len(active_streams)}\n"
           f"• Sudo users      : {len(sudo_users)}\n"
           f"• Owners          : {len(OWNERS)}\n"
           f"• MongoDB         : {'✅' if MONGO_OK else '❌'}")
    await safe_reply(m, txt, parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("uptime") & filters.private)
@safe_handler
async def uptime_cmd(c, m: Message):
    if not is_authorized(m.from_user.id): return
    up = time.time() - BOT_START_TIME
    h = int(up // 3600); mins = int((up % 3600) // 60); s = int(up % 60)
    await safe_reply(m, f"⏱ Bot Uptime: {h}h {mins}m {s}s", parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("ping") & filters.private)
@safe_handler
async def ping_cmd(c, m: Message):
    if not is_authorized(m.from_user.id): return
    t0 = time.time()
    msg = await safe_reply(m, "🏓 Pong!", parse_mode=ParseMode.DISABLED)
    ms = (time.time() - t0) * 1000
    await safe_edit(msg, f"🏓 Pong! {ms:.0f}ms", parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("info") & filters.private)
@safe_handler
async def info_cmd(c, m: Message):
    if not is_authorized(m.from_user.id): return
    uid = m.from_user.id
    await safe_reply(m,
        f"👤 Account Info\n\n"
        f"• Your ID: {uid}\n"
        f"• Owner: {'✅' if is_owner(uid) else '❌'}\n"
        f"• Sudo: {'✅' if uid in sudo_users else '❌'}\n"
        f"• Music Host: {'✅' if mongo_is_music_host(uid) else '❌'}", parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("restart") & filters.private)
@safe_handler
async def restart_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_authorized(uid): return
    if not (mongo_is_music_host(uid) or uid in music_accounts):
        await safe_reply(m, "❌ Aap kisi music userbot ke host nahi ho. /start se host karo.",
                         parse_mode=ParseMode.DISABLED); return
    proc = await safe_reply(m, "♻️ Restarting your music userbot…", parse_mode=ParseMode.DISABLED)
    ok = await restart_music_userbot(uid)
    if ok: await safe_edit(proc, "✅ Music userbot restarted!", parse_mode=ParseMode.DISABLED)
    else:  await safe_edit(proc, "❌ Restart fail. Session expire — /start se re-host karo.",
                           parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("restartall") & filters.private)
@safe_handler
async def restartall_cmd(c, m: Message):
    if not is_owner(m.from_user.id):
        await safe_reply(m, "❌ Owner only.", parse_mode=ParseMode.DISABLED); return
    hosts = mongo_get_all_music_hosts()
    if not hosts:
        await safe_reply(m, "ℹ️ No music userbots hosted.", parse_mode=ParseMode.DISABLED); return
    proc = await safe_reply(m, f"♻️ Restarting {len(hosts)} music userbots…", parse_mode=ParseMode.DISABLED)
    sem = asyncio.Semaphore(5)
    results = {"ok": 0, "fail": 0}
    async def _do(h):
        async with sem:
            try:
                r = await restart_music_userbot(h)
                if r: results["ok"] += 1
                else: results["fail"] += 1
            except: results["fail"] += 1
    await asyncio.gather(*[_do(h) for h in hosts], return_exceptions=True)
    await safe_edit(proc,
        f"✅ Restart Complete\n\n"
        f"• Successful: {results['ok']}\n"
        f"• Failed: {results['fail']}\n"
        f"• Total: {len(hosts)}", parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("stats") & filters.private)
@safe_handler
async def stats_cmd(c, m: Message):
    if not is_owner(m.from_user.id): return
    hosts = mongo_get_all_music_hosts()
    await safe_reply(m,
        f"📊 Bot Statistics\n\n"
        f"• Music Hosts (DB): {len(hosts)}\n"
        f"• Active Music: {len(music_accounts)}\n"
        f"• Active VC Users: {len(user_accounts)}\n"
        f"• Active Streams: {len(active_streams)}\n"
        f"• Queues: {len(music_queues)}\n"
        f"• Cache Size: {len(RECENT_DL_CACHE)}\n"
        f"• Sudo: {len(sudo_users)}\n"
        f"• DL Pool: {GLOBAL_DOWNLOAD_POOL._max_workers} workers", parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("broadcast") & filters.private)
@safe_handler
async def broadcast_cmd(c, m: Message):
    if not is_owner(m.from_user.id): return
    if len(m.command) < 2:
        await safe_reply(m, "❌ Usage: /broadcast <text>", parse_mode=ParseMode.DISABLED); return
    text = m.text.split(None, 1)[1]
    hosts = mongo_get_all_music_hosts()
    sent = 0
    for h in hosts:
        try:
            await c.send_message(h, f"📢 Broadcast from Owner:\n\n{text}",
                                 parse_mode=ParseMode.DISABLED)
            sent += 1
        except: pass
    await safe_reply(m, f"📢 Sent to {sent}/{len(hosts)} hosts.", parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("cleancache") & filters.private)
@safe_handler
async def cleancache_cmd(c, m: Message):
    if not is_owner(m.from_user.id): return
    n = _clean_downloads_dir(force=True)
    RECENT_DL_CACHE.clear()
    gc.collect()
    await safe_reply(m, f"🧹 Cleaned {n} files + cache cleared + GC.", parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("killall") & filters.private)
@safe_handler
async def killall_cmd(c, m: Message):
    if not is_owner(m.from_user.id): return
    count = 0
    for sk in list(active_streams.keys()):
        account_key, chat_id = sk
        calls = None
        if account_key == "default": calls = default_calls
        elif isinstance(account_key, tuple):
            if account_key[0] == "vc":
                calls = user_calls.get(account_key[1])
            elif account_key[0] == "music":
                calls = music_calls.get(account_key[1])
                _cancel_music_autoleave(account_key[1], chat_id)
        try:
            if calls: await calls.leave_group_call(chat_id)
        except: pass
        _clear_stream_state(account_key, chat_id)
        count += 1
    music_now_playing.clear()
    for q in music_queues.values():
        for s in q:
            try:
                p = s.get("file_path")
                if p and os.path.exists(p): os.remove(p)
            except: pass
    music_queues.clear()
    await safe_reply(m, f"🛑 Killed {count} active streams.", parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("sudo") & filters.private)
@safe_handler
async def add_sudo(client, m: Message):
    if not is_owner(m.from_user.id): return
    if len(m.command) < 2:
        await safe_reply(m, "❌ Usage: /sudo <user_id>", parse_mode=ParseMode.DISABLED); return
    try:
        uid = int(m.command[1])
    except ValueError:
        await safe_reply(m, "❌ Invalid user_id", parse_mode=ParseMode.DISABLED); return
    sudo_users.add(uid)
    mongo_save_sudo(uid)
    await safe_reply(m, f"✅ Added sudo: {uid}", parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("rmsudo") & filters.private)
@safe_handler
async def rm_sudo(client, m: Message):
    if not is_owner(m.from_user.id): return
    if len(m.command) < 2:
        await safe_reply(m, "❌ Usage: /rmsudo <user_id>", parse_mode=ParseMode.DISABLED); return
    try:
        uid = int(m.command[1])
    except ValueError:
        await safe_reply(m, "❌ Invalid user_id", parse_mode=ParseMode.DISABLED); return
    sudo_users.discard(uid)
    mongo_remove_sudo(uid)
    await safe_reply(m, f"✅ Removed sudo: {uid}", parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("sudolist") & filters.private)
@safe_handler
async def sudo_list(client, m: Message):
    if not is_owner(m.from_user.id): return
    if not sudo_users:
        await safe_reply(m, "ℹ️ No sudo users.", parse_mode=ParseMode.DISABLED); return
    txt = "👥 Sudo users:\n" + "\n".join(f"• {u}" for u in sudo_users)
    await safe_reply(m, txt, parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("stop") & filters.private)
@safe_handler
async def stop_cmd(c, m: Message):
    if not is_authorized(m.from_user.id): return
    uid = m.from_user.id
    my_streams = [sk for sk, owner in stream_owners.items() if owner == uid]
    if not my_streams:
        await safe_reply(m, "ℹ️ You have no active streams.", parse_mode=ParseMode.DISABLED); return
    count = 0
    for sk in my_streams:
        account_key, chat_id = sk
        calls = None
        if account_key == "default": calls = default_calls
        elif isinstance(account_key, tuple):
            if account_key[0] == "vc":
                calls = user_calls.get(account_key[1])
            elif account_key[0] == "music":
                calls = music_calls.get(account_key[1])
                _cancel_music_autoleave(account_key[1], chat_id)
        try:
            if calls: await calls.leave_group_call(chat_id)
        except: pass
        _clear_stream_state(account_key, chat_id)
        count += 1
    await safe_reply(m, f"⏹ Stopped {count} of your streams.", parse_mode=ParseMode.DISABLED)

@bot.on_message(filters.command("logout") & filters.private)
@safe_handler
async def logout_cmd(c, m: Message):
    if not is_authorized(m.from_user.id): return
    uid = m.from_user.id
    logged_out = []
    if uid in user_accounts:
        try:
            await safe_stop_calls(user_calls.get(uid))
            await user_accounts[uid].stop()
        except: pass
        user_accounts.pop(uid, None); user_calls.pop(uid, None)
        mongo_delete_session(uid, "vc")
        logged_out.append("VC Fight")
    if uid in music_accounts:
        try:
            unregister_music_handlers(music_accounts[uid], uid)
            await safe_stop_calls(music_calls.get(uid))
            await music_accounts[uid].stop()
        except: pass
        music_accounts.pop(uid, None); music_calls.pop(uid, None)
        mongo_delete_session(uid, "music")
        mongo_remove_music_host(uid)
        logged_out.append("Music Userbot")
    if logged_out:
        await safe_reply(m, f"✅ Logged out: {', '.join(logged_out)}", parse_mode=ParseMode.DISABLED)
    else:
        await safe_reply(m, "ℹ️ Nothing to logout.", parse_mode=ParseMode.DISABLED)

# ═══════════════════════════════════════════════════════════════════════════════
# 🎹 CALLBACK QUERIES
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_callback_query()
async def cb_handler(c, q: CallbackQuery):
    uid = q.from_user.id
    if not is_authorized(uid):
        try: await q.answer("❌ Not authorized", show_alert=True)
        except: pass
        return
    data = q.data
    state = get_user_state(uid)
    try:
        if data == "back_main":
            try:
                await q.message.edit_text("👋 Choose what to do:", reply_markup=build_welcome_keyboard())
            except:
                await q.message.edit_text("Choose what to do:", reply_markup=build_welcome_keyboard(),
                                          parse_mode=ParseMode.DISABLED)
            await q.answer(); return
        if data == "show_help":
            try:
                await q.message.edit_text(f"```\n{HELP_TEXT}\n```",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="back_main")]]))
            except:
                await q.message.edit_text(HELP_TEXT,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="back_main")]]),
                    parse_mode=ParseMode.DISABLED)
            await q.answer(); return
        if data == "menu_vcfight":
            await q.message.edit_text("⚔️ VC Fight Menu\n\nChoose account:",
                reply_markup=build_vcfight_keyboard(), parse_mode=ParseMode.DISABLED)
            await q.answer(); return
        if data == "menu_music":
            await q.message.edit_text("🎵 Music Userbot Menu",
                reply_markup=build_music_keyboard(uid), parse_mode=ParseMode.DISABLED)
            await q.answer(); return
        if data == "use_default":
            if default_account:
                state.step = "default_group"
                state.data["mode"] = "default"
                await q.message.edit_text("📎 Send group info (@username or invite link or chat_id):",
                                          parse_mode=ParseMode.DISABLED)
                await q.answer(); return
            else:
                if not is_owner(uid):
                    await q.answer("❌ Default not set up yet.", show_alert=True); return
                state.step = "default_phone"
                state.data["mode"] = "default"
                await q.message.edit_text("📱 Send phone number for default account (e.g. +91…):",
                                          parse_mode=ParseMode.DISABLED)
                await q.answer(); return
        if data == "use_custom":
            if uid in user_accounts:
                state.step = "custom_group"
                state.data["mode"] = "custom"
                await q.message.edit_text("📎 Send group info (@username or invite link):",
                                          parse_mode=ParseMode.DISABLED)
                await q.answer(); return
            state.step = "custom_phone"
            state.data["mode"] = "custom"
            await q.message.edit_text("📱 Send your phone number (e.g. +91…):",
                                      parse_mode=ParseMode.DISABLED)
            await q.answer(); return
        if data == "music_host":
            if uid in music_accounts:
                await q.answer("✅ Already hosted!", show_alert=True); return
            state.step = "music_phone"
            state.data["mode"] = "music"
            await q.message.edit_text("📱 Send phone number to host music userbot (e.g. +91…):",
                                      parse_mode=ParseMode.DISABLED)
            await q.answer(); return
        if data == "music_commands":
            try:
                await q.message.edit_text(f"```\n{HELP_TEXT}\n```",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="menu_music")]]))
            except:
                await q.message.edit_text(HELP_TEXT,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="menu_music")]]),
                    parse_mode=ParseMode.DISABLED)
            await q.answer(); return
        if data == "music_restart":
            await q.answer("♻️ Restarting…", show_alert=False)
            ok = await restart_music_userbot(uid)
            if ok:
                await q.message.edit_text("✅ Music userbot restarted!",
                    reply_markup=build_music_keyboard(uid), parse_mode=ParseMode.DISABLED)
            else:
                await q.message.edit_text("❌ Restart failed. Re-host with /start",
                    reply_markup=build_music_keyboard(uid), parse_mode=ParseMode.DISABLED)
            return
        if data == "music_logout":
            if uid in music_accounts:
                try:
                    unregister_music_handlers(music_accounts[uid], uid)
                    await safe_stop_calls(music_calls.get(uid))
                    await music_accounts[uid].stop()
                except: pass
                music_accounts.pop(uid, None); music_calls.pop(uid, None)
            mongo_delete_session(uid, "music")
            mongo_remove_music_host(uid)
            await q.message.edit_text("🚪 Logged out from music userbot.",
                reply_markup=build_welcome_keyboard(), parse_mode=ParseMode.DISABLED)
            await q.answer(); return
    except Exception as e:
        logger.error(f"cb_handler err: {e}")
        try: await q.answer(f"❌ {_safe_truncate(e, 100)}", show_alert=True)
        except: pass

# ═══════════════════════════════════════════════════════════════════════════════
# 🔐 OTP / 2FA FLOW
# ═══════════════════════════════════════════════════════════════════════════════
def _clean_otp_text(text: str) -> str:
    return re.sub(r'\D', '', text)

async def verify_otp_text(m: Message, mode: str, otp: str):
    uid = m.from_user.id
    state = get_user_state(uid)
    proc = await safe_reply(m, "⏳ Verifying OTP…", parse_mode=ParseMode.DISABLED)
    try:
        uc = state.data.get("client")
        if not uc:
            await safe_edit(proc, "❌ Session expired, /start again", parse_mode=ParseMode.DISABLED)
            state.step = None; return
        try:
            if not uc.is_connected:
                await uc.connect()
        except: pass
        await uc.sign_in(state.data["phone"], state.data["phone_code_hash"], otp)
        await _finalize_login(uid, uc, mode, proc, state)
    except SessionPasswordNeeded:
        state.step = {"default_otp": "default_2fa", "custom_otp": "custom_2fa",
                      "music_otp": "music_2fa"}[mode]
        await safe_edit(proc, "🔐 2FA Enabled\n\nSend your 2FA password as plain text.",
                        parse_mode=ParseMode.DISABLED)
    except PhoneCodeInvalid:
        await safe_edit(proc, "❌ Invalid OTP! Send digits again: 1 2 3 4 5",
                        parse_mode=ParseMode.DISABLED)
    except Exception as e:
        await safe_edit(proc, f"❌ {_safe_truncate(e, 150)}", parse_mode=ParseMode.DISABLED)
        state.step = None

async def _finalize_login(uid, uc: Client, mode: str, proc_msg, state):
    global default_account, default_calls
    logger.info(f"🔐 Login confirmed for {uid} (mode={mode})")
    await safe_edit(proc_msg, "✅ Verified! Setting up…", parse_mode=ParseMode.DISABLED)
    sess_str = await uc.export_session_string()
    asyncio.create_task(warmup_peers(uc, f"login-{mode}", force=True))

    if mode in ("default_otp", "default_2fa"):
        default_account = uc
        default_calls = PyTgCalls(uc)
        await default_calls.start()
        mongo_save_session(uid, state.data.get("phone", ""), sess_str, "default")
        await safe_edit(proc_msg, "✅ Default account configured!", parse_mode=ParseMode.DISABLED)
    elif mode in ("custom_otp", "custom_2fa"):
        user_accounts[uid] = uc
        user_calls[uid] = PyTgCalls(uc)
        await user_calls[uid].start()
        mongo_save_session(uid, state.data.get("phone", ""), sess_str, "vc")
        state.step = "custom_group"
        await safe_edit(proc_msg, "✅ Logged in!\n\n📎 Now send group info:", parse_mode=ParseMode.DISABLED)
        return
    elif mode in ("music_otp", "music_2fa"):
        music_accounts[uid] = uc
        music_calls[uid] = PyTgCalls(uc)
        await music_calls[uid].start()
        mongo_save_session(uid, state.data.get("phone", ""), sess_str, "music")
        mongo_save_music_host(uid)
        register_music_handlers(uc, uid)
        await safe_edit(proc_msg,
            "✅ Music Userbot Hosted! 🎵\n\n"
            "Add to any group → .play <song>  (instant!)\n"
            "Send /help for full command list.", parse_mode=ParseMode.DISABLED)
    state.step = None
    state.data.pop("client", None)
    logger.info(f"✅ Finalize complete for {uid}")

# ═══════════════════════════════════════════════════════════════════════════════
# 💬 TEXT MESSAGE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_message(filters.private & filters.text & ~filters.command([
    "start","help","commands","sudo","rmsudo","sudolist","stop","status",
    "logout","owners","restart","restartall","stats","broadcast","cleancache",
    "killall","uptime","ping","info"]))
@safe_handler
async def msg_handler(client, m: Message):
    uid = m.from_user.id
    if not is_authorized(uid): return
    state = get_user_state(uid)
    text = m.text
    if not state.step: return

    if state.step in ("default_phone", "custom_phone", "music_phone"):
        phone = text.strip().replace(" ", "")
        state.data["phone"] = phone
        proc = await safe_reply(m, "⏳ Sending OTP…", parse_mode=ParseMode.DISABLED)
        try:
            name = {"default_phone": f"default_tmp_{uid}",
                    "custom_phone":  f"user_{uid}_tmp",
                    "music_phone":   f"music_{uid}_tmp"}[state.step]
            uc = Client(name, api_id=API_ID, api_hash=API_HASH, in_memory=True)
            await uc.connect()
            sent = await uc.send_code(phone)
            state.data["phone_code_hash"] = sent.phone_code_hash
            state.data["client"] = uc
            next_mode = {"default_phone": "default_otp", "custom_phone": "custom_otp",
                         "music_phone": "music_otp"}[state.step]
            state.step = next_mode
            await safe_edit(proc,
                "📨 OTP Sent!\n\n"
                "Send the OTP code with spaces between digits.\n"
                "Example:  1 2 3 4 5", parse_mode=ParseMode.DISABLED)
        except FloodWait as e:
            await safe_edit(proc, f"⏳ Flood wait: {e.value}s", parse_mode=ParseMode.DISABLED)
            state.step = None
        except Exception as e:
            await safe_edit(proc, f"❌ {_safe_truncate(e, 150)}", parse_mode=ParseMode.DISABLED)
            state.step = None
        return

    if state.step in ("default_otp", "custom_otp", "music_otp"):
        otp = _clean_otp_text(text)
        if len(otp) < 4:
            await safe_reply(m, "❌ Send digits: 1 2 3 4 5 (min 4 digits)",
                             parse_mode=ParseMode.DISABLED)
            return
        await verify_otp_text(m, state.step, otp)
        return

    if state.step in ("default_2fa", "custom_2fa", "music_2fa"):
        proc = await safe_reply(m, "⏳ Verifying 2FA…", parse_mode=ParseMode.DISABLED)
        try:
            uc = state.data.get("client")
            if not uc:
                await safe_edit(proc, "❌ Session expired, /start again", parse_mode=ParseMode.DISABLED)
                state.step = None; return
            try:
                if not uc.is_connected:
                    await uc.connect()
            except: pass
            await uc.check_password(text.strip())
            await _finalize_login(uid, uc, state.step, proc, state)
        except PasswordHashInvalid:
            await safe_edit(proc, "❌ Wrong 2FA password! /start again.", parse_mode=ParseMode.DISABLED)
            state.step = None
        except Exception as e:
            await safe_edit(proc, f"❌ {_safe_truncate(e, 150)}", parse_mode=ParseMode.DISABLED)
            state.step = None
        return

    if state.step == "waiting_chat_id":
        try:
            cid = int(text.strip())
            state.data["actual_chat_id"] = cid
            state.step = "audio_input"
            await safe_reply(m, f"✅ Chat ID set: {cid}\n\n🎵 Send Audio / URL / Song name",
                             parse_mode=ParseMode.DISABLED)
        except ValueError:
            await safe_reply(m, "❌ Invalid! Send like: -100123456789",
                             parse_mode=ParseMode.DISABLED)
        return

    if state.step in ("default_group", "custom_group"):
        if text.strip().lstrip("-").isdigit():
            try:
                cid = int(text.strip())
                state.data["actual_chat_id"] = cid
                state.step = "audio_input"
                await safe_reply(m, f"✅ Chat ID set: {cid}\n\n🎵 Send Audio / URL / Song name",
                                 parse_mode=ParseMode.DISABLED)
                return
            except: pass
        ci = extract_chat_info(text)
        if not ci:
            await safe_reply(m, "❌ Invalid input. Send @username, t.me/+xxx, or chat_id",
                             parse_mode=ParseMode.DISABLED)
            return
        state.data["chat_info"] = ci
        mode = state.data.get("mode")
        client_to_use = default_account if mode == "default" else user_accounts.get(uid)
        user_key_for_cache = "default" if mode == "default" else uid
        if not client_to_use:
            await safe_reply(m, "❌ Session expired, /start again", parse_mode=ParseMode.DISABLED)
            state.step = None; return
        if ci["type"] == "username":
            proc = await safe_reply(m, "⏳ Resolving group…", parse_mode=ParseMode.DISABLED)
            ok, cid, title, err, need_id = await get_chat_id_smart(client_to_use, ci, user_key_for_cache)
            if ok:
                state.data["actual_chat_id"] = cid
                state.data["chat_title"] = title
                state.step = "audio_input"
                asyncio.create_task(ensure_peer(client_to_use, cid))
                await safe_edit(proc, f"✅ Group: {title}\n\n🎵 Send Audio / URL / Song name",
                                parse_mode=ParseMode.DISABLED)
            else:
                await safe_edit(proc, err, parse_mode=ParseMode.DISABLED); state.step = None
        else:
            proc = await safe_reply(m, "⏳ Joining private group…", parse_mode=ParseMode.DISABLED)
            try: await client_to_use.join_chat(ci["value"])
            except UserAlreadyParticipant: pass
            except InviteHashExpired:
                await safe_edit(proc, "❌ Invite expired!", parse_mode=ParseMode.DISABLED)
                state.step = None; return
            except Exception: pass
            state.step = "waiting_chat_id"
            await safe_edit(proc, "🔒 Private Group\n\nSend the Chat ID (e.g. -100123456789)",
                            parse_mode=ParseMode.DISABLED)
        return

    if state.step == "audio_input":
        mode = state.data.get("mode")
        ci = state.data.get("chat_info")
        if mode == "default":
            client_to_use, calls_to_use = default_account, default_calls
            account_key = "default"
        else:
            client_to_use = user_accounts.get(uid)
            calls_to_use = user_calls.get(uid)
            account_key = ("vc", uid)
        if not client_to_use or not calls_to_use:
            await safe_reply(m, "❌ Session expired, /start", parse_mode=ParseMode.DISABLED)
            state.step = None; return
        proc = await safe_reply(m, "⚡ Downloading…", parse_mode=ParseMode.DISABLED)
        actual_id = state.data.get("actual_chat_id")
        title = state.data.get("chat_title", "Group")
        if not actual_id and ci:
            ok, actual_id, title, err, _ = await get_chat_id_smart(
                client_to_use, ci, "default" if mode == "default" else uid)
            if not ok:
                await safe_edit(proc, err, parse_mode=ParseMode.DISABLED)
                state.step = None; return
        t0 = time.time()
        try:
            file_path, song_title, _dur = await resolve_stream_instant(text, host_id=uid)
        except Exception as e:
            await safe_edit(proc, f"❌ Download error: {_safe_truncate(e, 150)}",
                            parse_mode=ParseMode.DISABLED)
            state.step = None; return
        if not file_path:
            await safe_edit(proc, "❌ Couldn't fetch audio", parse_mode=ParseMode.DISABLED)
            state.step = None; return
        asyncio.create_task(ensure_peer(client_to_use, actual_id))
        await safe_edit(proc, "🔌 Joining VC…", parse_mode=ParseMode.DISABLED)
        ok, err = await join_vc_and_stream(
            client_to_use, calls_to_use, actual_id, file_path, account_key,
            is_video=False, owner_uid=uid)
        elapsed = time.time() - t0
        if ok:
            asyncio.create_task(auto_leave_after_playback(
                calls_to_use, account_key, actual_id, file_path, cleanup_path=file_path))
            await safe_edit(proc,
                f"✅ NOW STREAMING 🔥\n\n"
                f"📻 Group: {_safe_truncate(title, 80)}\n"
                f"🎵 {_safe_truncate(song_title or 'Audio', 100)}\n"
                f"⚡ Started in {elapsed:.1f}s\n\n"
                f"Use /stop to stop YOUR stream", parse_mode=ParseMode.DISABLED)
        else:
            await safe_edit(proc, f"❌ Play failed: {_safe_truncate(err, 150)}",
                            parse_mode=ParseMode.DISABLED)
            try: os.remove(file_path)
            except: pass
        state.step = None
        return

# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 AUDIO / VOICE FILE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_message(filters.private & (filters.audio | filters.voice))
@safe_handler
async def audio_handler(client, m: Message):
    uid = m.from_user.id
    if not is_authorized(uid): return
    state = get_user_state(uid)
    if state.step != "audio_input": return
    mode = state.data.get("mode")
    if mode == "default":
        cu, calls = default_account, default_calls
        account_key = "default"
    else:
        cu, calls = user_accounts.get(uid), user_calls.get(uid)
        account_key = ("vc", uid)
    if not cu or not calls:
        await safe_reply(m, "❌ Session expired", parse_mode=ParseMode.DISABLED)
        state.step = None; return
    proc = await safe_reply(m, "⏳ Downloading audio…", parse_mode=ParseMode.DISABLED)
    dl_dir = _get_userbot_dl_dir(uid)
    raw = await m.download(file_name=f"{dl_dir}/{m.id}.mp3")
    audio_file = await boost_audio(raw)
    actual = state.data.get("actual_chat_id")
    title = state.data.get("chat_title", "Group")
    await safe_edit(proc, "🔌 Joining VC…", parse_mode=ParseMode.DISABLED)
    ok, err = await join_vc_and_stream(
        cu, calls, actual, audio_file, account_key,
        is_video=False, owner_uid=uid)
    if ok:
        asyncio.create_task(auto_leave_after_playback(
            calls, account_key, actual, audio_file, cleanup_path=audio_file))
        boost_txt = f"🔊 BOOST {VOLUME_BOOST}x" if ENABLE_AUDIO_BOOST else ""
        await safe_edit(proc, f"✅ NOW PLAYING 🔥\n📻 {_safe_truncate(title, 80)}\n{boost_txt}",
                        parse_mode=ParseMode.DISABLED)
    else:
        await safe_edit(proc, f"❌ {_safe_truncate(err, 150)}", parse_mode=ParseMode.DISABLED)
    state.step = None

# ═══════════════════════════════════════════════════════════════════════════════
# 🚀 STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
async def on_startup():
    _clean_downloads_dir(force=True)
    await restore_sessions()
    asyncio.create_task(background_janitor())
    asyncio.create_task(watchdog())
    logger.info("✅ Background janitor + watchdog started")
    logger.info(f"✅ All sessions restored: {len(music_accounts)} music userbots, {len(user_accounts)} vc users, {len(sudo_users)} sudo")

def _banner():
    print("\n" + "="*70)
    print("🔥  ZUDO USERBOT — v11 (FIXED MONGO RESTORE + ENTITY BOUNDS + AUTO-LEAVE)")
    print("="*70)
    print(f"   👑 Primary Owner : {OWNER_ID}")
    print(f"   👑 Co-Owner      : {CO_OWNER_ID}")
    print(f"   🗄️  MongoDB       : {'OK' if MONGO_OK else 'FAIL'}")
    print(f"   🔊 Volume Boost  : {VOLUME_BOOST}x ({'ON' if ENABLE_AUDIO_BOOST else 'OFF (saves RAM)'})")
    print(f"   🔑 Bot Token     : {'*'*8}{BOT_TOKEN[-4:] if BOT_TOKEN else '????'}")
    print(f"   ⚡ DL Pool       : {GLOBAL_DOWNLOAD_POOL._max_workers} workers (non-blocking)")
    print(f"   🔒 Per-Userbot   : isolated sem(2) + isolated dl_dir")
    print(f"   📥 Queue System  : per-(host, chat) deque + Lock")
    print(f"   🛑 Stop Mode     : per-user (stream_owners)")
    print(f"   🎬 .vplay        : capped at 480p (no OOM)")
    print(f"   🧹 Janitor       : every {GC_INTERVAL_SEC}s")
    print(f"   🐕 Watchdog      : every {WATCHDOG_INTERVAL}s")
    print(f"   🛡 safe_handler  : ALL handlers wrapped + entity-safe")
    print(f"   📡 Auto-Leave    : silent leave when queue empty (dual mechanism)")
    print(f"   🧩 Stream Types  : "
          f"MediaStream={'✅' if _MediaStream else '❌'} "
          f"AudioPiped={'✅' if _AudioPiped else '❌'} "
          f"VideoPiped={'✅' if _VideoPiped else '❌'}")
    print("="*70 + "\n")

if __name__ == "__main__":
    _banner()
    logger.info("🚀 Starting ZUDO USERBOT v11 (mongo restore + entity fix + auto-leave)…")

    async def _main():
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(_global_exception_handler)
        await bot.start()
        await on_startup()
        logger.info("✅ Bot is running. Ctrl+C to stop.")
        await idle()
        await bot.stop()
        GLOBAL_DOWNLOAD_POOL.shutdown(wait=False)
        GLOBAL_FFPROBE_POOL.shutdown(wait=False)

    try:
        asyncio.get_event_loop().run_until_complete(_main())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception as e:
        logger.error(f"Crash: {e}\n{traceback.format_exc()}")
