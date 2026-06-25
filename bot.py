"""
🔥 ZUDO USERBOT — GOD-LEVEL EDITION v5 (INSTANT STREAMING + TEXT-OTP + 403-FIX)

✦ FIXED: OTP keypad HATA DIYA — ab direct text se OTP enter karo
       (e.g. "1 2 3 4 5" ya "12345" ya "1-2-3-4-5" — sab chalega)
✦ FIXED: Music ab INSTANT STREAM hota hai (no full download wait!)
       → .play command turant VC join + play within ~3-5 sec
✦ FIXED: .play command message turant delete ho jata hai
✦ FIXED: 403 Forbidden errors — multi-source fallback chain:
       nubcoder API → invidious → yt-dlp (with android client) → piped
✦ FIXED: PyTgCalls.stop() AttributeError — removed (attr doesn't exist)
✦ FIXED: Songs jo play nahi hote the — ab sab play honge (fallback chain)
✦ FIXED: 12 old handler duplicate registration — proper cleanup
"""

import os
import re
import sys
import json
import asyncio
import logging
import random
import requests
from datetime import datetime
from typing import Optional, Tuple

# ─── Pyrogram ────────────────────────────────────────────────────────────────
from pyrogram import Client, filters, idle
from pyrogram.types import (
    Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
)
from pyrogram.handlers import MessageHandler
from pyrogram.enums import ChatType
from pyrogram.errors import (
    FloodWait, PhoneCodeInvalid, SessionPasswordNeeded, PasswordHashInvalid,
    PeerIdInvalid, UserAlreadyParticipant, InviteHashExpired,
    AuthKeyUnregistered, AuthKeyDuplicated, UserDeactivated, UserDeactivatedBan,
)

# ─── PyTgCalls ───────────────────────────────────────────────────────────────
from pytgcalls import PyTgCalls, StreamType
from pytgcalls.types.input_stream import AudioPiped
from pytgcalls.types.input_stream.quality import HighQualityAudio

# ─── yt-dlp & Mongo ──────────────────────────────────────────────────────────
import yt_dlp
from pymongo import MongoClient

# ═══════════════════════════════════════════════════════════════════════════════
# ⚙️  CONFIG  (same defaults as old script — env vars override)
# ═══════════════════════════════════════════════════════════════════════════════
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
YT_COOKIES   = _get_env("YT_COOKIES", default="/app/cookies/cookies.txt")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

Path("/tmp/downloads").mkdir(exist_ok=True, parents=True)
Path("/app/sessions").mkdir(exist_ok=True, parents=True)
Path("/app/data").mkdir(exist_ok=True, parents=True)
Path("/app/cookies").mkdir(exist_ok=True, parents=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 🪵 LOGGING
# ═══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)-7s] [ZUDO] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("zudo")

# ═══════════════════════════════════════════════════════════════════════════════
# 🗄️  MONGO INIT
# ═══════════════════════════════════════════════════════════════════════════════
MONGO_OK = False
mongo = None
col_sudo = col_cache = col_sessions = col_default = None
col_music_hosts = col_music_sudo = None

if MONGO_URL:
    try:
        mongo = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
        mongo.admin.command("ping")
        db = mongo["zudo_userbot"]
        col_sudo        = db["sudo_users"]
        col_cache       = db["chat_cache"]
        col_sessions    = db["sessions"]
        col_default     = db["default_session"]
        col_music_hosts = db["music_hosts"]
        col_music_sudo  = db["music_sudo"]
        MONGO_OK = True
        logger.info("✅ MongoDB connected")
    except Exception as e:
        logger.error(f"❌ Mongo fail: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 🤖 BOT CLIENT
# ═══════════════════════════════════════════════════════════════════════════════
bot = Client(
    "zudo_bot",
    api_id=API_ID, api_hash=API_HASH,
    bot_token=BOT_TOKEN, in_memory=True
)

# ═══════════════════════════════════════════════════════════════════════════════
# 🌐 GLOBAL STATE
# ═══════════════════════════════════════════════════════════════════════════════
default_account: Optional[Client]   = None
default_calls:   Optional[PyTgCalls] = None

user_accounts:  dict = {}   # uid -> Pyrogram Client (VC fight)
user_calls:     dict = {}   # uid -> PyTgCalls
music_accounts: dict = {}   # uid -> Pyrogram Client (music host)
music_calls:    dict = {}   # uid -> PyTgCalls

user_states:    dict = {}   # uid -> UserState
active_streams: dict = {}   # key -> chat_id currently streaming
play_locks:     dict = {}   # (host_id, chat_id) -> asyncio.Lock

# per-host list of registered MessageHandler instances (for restart cleanup)
music_handler_refs: dict = {}


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
# 🚑 PEER WARMUP
# ═══════════════════════════════════════════════════════════════════════════════
async def warmup_peers(client: Client, label: str = ""):
    try:
        count = 0
        async for _ in client.get_dialogs():
            count += 1
            if count > 500:
                break
        logger.info(f"🔥 [{label}] Warmed up {count} peers — Peer-id invalid FIX done")
    except Exception as e:
        logger.error(f"warmup_peers [{label}] err: {e}")

async def safe_resolve(client: Client, chat_id):
    try:
        return await client.get_chat(chat_id)
    except (PeerIdInvalid, KeyError, ValueError):
        logger.warning(f"⚠️ Peer cache miss for {chat_id} — re-warming dialogs")
        await warmup_peers(client, label=f"retry-{chat_id}")
        return await client.get_chat(chat_id)


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
            return False, None, None, (
                "🔒 Private Group Detected\n\nPlease send the Chat ID now (e.g. -100123456789)"
            ), True

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
                return False, None, None, "🔒 Already member, please send Chat ID (-100…)", True
            except InviteHashExpired:
                return False, None, None, "❌ Invite link expired!", False
            except Exception as e:
                return False, None, None, f"❌ {e}", False
    except Exception as e:
        return False, None, None, f"❌ Unexpected: {e}", False


# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 INSTANT STREAMING ENGINE — multi-source fallback (NO download wait!)
#     Chain: nubcoder → invidious → yt-dlp(android) → yt-dlp(web) → piped
# ═══════════════════════════════════════════════════════════════════════════════
def _ytdlp_opts_base(client_name="android"):
    """yt-dlp opts. 'android' client bypasses most 403s."""
    opts = {
        'quiet': True,
        'no_warnings': True,
        'nocheckcertificate': True,
        'default_search': 'ytsearch',
        'noplaylist': True,
        'skip_download': True,
        'geo_bypass': True,
        'extractor_retries': 2,
        'socket_timeout': 12,
        'http_headers': {
            'User-Agent': USER_AGENT,
            'Accept-Language': 'en-US,en;q=0.9',
        },
        # 🔥 KEY FIX for 403: use android/ios player client
        'extractor_args': {
            'youtube': {
                'player_client': [client_name, 'web'],
                'player_skip': ['webpage', 'configs'],
            }
        },
    }
    if YT_COOKIES and os.path.exists(YT_COOKIES):
        opts['cookiefile'] = YT_COOKIES
    return opts


async def nubcoder_get_stream(query_or_url: str):
    """Try nubcoder API first — gives direct stream URL fast."""
    try:
        url = NUBCODER_API.format(requests.utils.quote(query_or_url))
        loop = asyncio.get_event_loop()
        r = await loop.run_in_executor(None, lambda: requests.get(url, timeout=8))
        if r.status_code == 200:
            data = r.json()
            for key in ("url", "audio_url", "stream", "stream_url", "direct", "link"):
                if key in data and data[key]:
                    return data[key], data.get("title", "Unknown")
    except Exception as e:
        logger.warning(f"nubcoder err (non-fatal): {e}")
    return None, None


async def invidious_get_stream(query_or_url: str):
    """Invidious API — instance-based, works when YT direct blocks."""
    try:
        loop = asyncio.get_event_loop()

        # Extract video ID if URL given
        video_id = None
        m = re.search(r'(?:v=|/)([0-9A-Za-z_-]{11})', query_or_url)
        if m:
            video_id = m.group(1)
        else:
            # search
            search_url = f"{INVIDIOUS_URL}/api/v1/search?q={requests.utils.quote(query_or_url)}&type=video"
            r = await loop.run_in_executor(None, lambda: requests.get(
                search_url, timeout=8, headers={"User-Agent": USER_AGENT}
            ))
            if r.status_code == 200:
                results = r.json()
                for item in results:
                    if item.get("type") == "video" and item.get("videoId"):
                        video_id = item["videoId"]
                        break
        if not video_id:
            return None, None

        # Get video details (has audio adaptive formats)
        detail_url = f"{INVIDIOUS_URL}/api/v1/videos/{video_id}"
        r = await loop.run_in_executor(None, lambda: requests.get(
            detail_url, timeout=8, headers={"User-Agent": USER_AGENT}
        ))
        if r.status_code != 200:
            return None, None
        data = r.json()
        title = data.get("title", "Unknown")

        # Pick best audio-only format
        adaptive = data.get("adaptiveFormats", [])
        audio_formats = [f for f in adaptive if f.get("type", "").startswith("audio")]
        if audio_formats:
            # highest bitrate
            best = max(audio_formats, key=lambda f: int(f.get("bitrate", 0) or 0))
            return best.get("url"), title
    except Exception as e:
        logger.warning(f"invidious err (non-fatal): {e}")
    return None, None


async def piped_get_stream(query_or_url: str):
    """Piped API as another fallback."""
    try:
        loop = asyncio.get_event_loop()

        video_id = None
        m = re.search(r'(?:v=|/)([0-9A-Za-z_-]{11})', query_or_url)
        if m:
            video_id = m.group(1)
        else:
            search_url = f"{PIPED_URL}/search?q={requests.utils.quote(query_or_url)}&filter=videos"
            r = await loop.run_in_executor(None, lambda: requests.get(
                search_url, timeout=8, headers={"User-Agent": USER_AGENT}
            ))
            if r.status_code == 200:
                results = r.json().get("items", [])
                for item in results:
                    if item.get("url"):
                        vm = re.search(r'(?:v=|/)([0-9A-Za-z_-]{11})', item["url"])
                        if vm:
                            video_id = vm.group(1)
                            break
        if not video_id:
            return None, None

        stream_url = f"{PIPED_URL}/streams/{video_id}"
        r = await loop.run_in_executor(None, lambda: requests.get(
            stream_url, timeout=8, headers={"User-Agent": USER_AGENT}
        ))
        if r.status_code != 200:
            return None, None
        data = r.json()
        title = data.get("title", "Unknown")
        audio_streams = data.get("audioStreams", [])
        if audio_streams:
            best = max(audio_streams, key=lambda f: int(f.get("bitrate", 0) or 0))
            return best.get("url"), title
    except Exception as e:
        logger.warning(f"piped err (non-fatal): {e}")
    return None, None


def _ytdlp_extract_sync(query_or_url: str, client_name: str, fmt: str):
    """Sync yt-dlp extraction (run in executor)."""
    opts = _ytdlp_opts_base(client_name=client_name)
    opts['format'] = fmt
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(query_or_url, download=False)
            if 'entries' in info and info['entries']:
                info = info['entries'][0]
            # Direct URL
            if info.get('url'):
                return info['url'], info.get('title', 'Unknown')
            # Formats list
            if 'formats' in info:
                for f in reversed(info['formats']):
                    if f.get('url') and f.get('acodec') and f.get('acodec') != 'none':
                        return f['url'], info.get('title', 'Unknown')
    except Exception as e:
        logger.warning(f"yt-dlp [{client_name}/{fmt}] err: {e}")
    return None, None


async def ytdlp_get_stream(query_or_url: str):
    """yt-dlp with multiple client fallbacks (android first to bypass 403)."""
    loop = asyncio.get_event_loop()
    # Try android first (bypasses most 403), then ios, then web
    for client_name in ("android", "ios", "web"):
        for fmt in ("bestaudio[ext=m4a]/bestaudio", "bestaudio/best", "140/251/250/249/best"):
            try:
                url, title = await loop.run_in_executor(
                    None, _ytdlp_extract_sync, query_or_url, client_name, fmt
                )
                if url:
                    logger.info(f"✅ yt-dlp stream OK ({client_name}/{fmt}): {title}")
                    return url, title
            except Exception as e:
                logger.warning(f"yt-dlp outer err: {e}")
    return None, None


async def get_stream_url(query_or_url: str):
    """
    Get DIRECT streamable audio URL (NO download). Returns (stream_url, title).
    Tries multiple sources in parallel for SPEED.
    """
    logger.info(f"🔎 Stream search: {query_or_url}")

    # 🚀 Run nubcoder + invidious in PARALLEL (fastest first wins)
    tasks = [
        asyncio.create_task(nubcoder_get_stream(query_or_url)),
        asyncio.create_task(invidious_get_stream(query_or_url)),
    ]

    try:
        # Wait for any to succeed (up to 6s)
        done, pending = await asyncio.wait(
            tasks, timeout=6.0, return_when=asyncio.FIRST_COMPLETED
        )
        for t in done:
            try:
                url, title = t.result()
                if url:
                    # cancel pending
                    for p in pending:
                        p.cancel()
                    logger.info(f"✅ FAST stream OK: {title}")
                    return url, title
            except Exception:
                continue

        # If first finished but failed, wait a bit more for the other one
        if pending:
            done2, _ = await asyncio.wait(pending, timeout=4.0)
            for t in done2:
                try:
                    url, title = t.result()
                    if url:
                        logger.info(f"✅ FAST stream OK (2nd): {title}")
                        return url, title
                except Exception:
                    continue
    except Exception as e:
        logger.warning(f"parallel fetch err: {e}")
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()

    # Fallback: piped
    url, title = await piped_get_stream(query_or_url)
    if url:
        logger.info(f"✅ piped stream OK: {title}")
        return url, title

    # Last resort: yt-dlp with android client (slow but reliable)
    url, title = await ytdlp_get_stream(query_or_url)
    if url:
        return url, title

    logger.error(f"❌ All stream sources failed for: {query_or_url}")
    return None, None


def build_audio_piped(stream_url: str) -> AudioPiped:
    """Build AudioPiped with volume boost & loudnorm — streams direct from URL."""
    try:
        ffmpeg_params = (
            f'-user_agent "{USER_AGENT}" '
            f'-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 '
            f'-af volume={VOLUME_BOOST},loudnorm=I=-12:LRA=11:TP=-1'
        )
        return AudioPiped(
            stream_url,
            HighQualityAudio(),
            additional_ffmpeg_parameters=ffmpeg_params,
        )
    except TypeError:
        return AudioPiped(stream_url, HighQualityAudio())


async def cleanup_file(file_path):
    try:
        await asyncio.sleep(60)
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
    except:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# 🎯 CORE: join VC + play  (instant, streaming-based)
# ═══════════════════════════════════════════════════════════════════════════════
async def join_vc_and_play(client: Client, calls: PyTgCalls,
                           chat_id: int, stream_url: str, stream_key):
    """Join VC and play a DIRECT URL stream — no download wait."""
    async def _try_join():
        await calls.join_group_call(
            chat_id,
            build_audio_piped(stream_url),
            stream_type=StreamType().pulse_stream
        )

    try:
        await _try_join()
        active_streams[stream_key] = chat_id
        logger.info(f"✅ Joined VC {chat_id} (key={stream_key})")
        return True, None
    except (PeerIdInvalid, KeyError, ValueError) as e:
        logger.warning(f"⚠️ peer issue ({e}); warming up & retrying")
        await warmup_peers(client, label=f"retry-{stream_key}")
        try:
            await _try_join()
            active_streams[stream_key] = chat_id
            logger.info(f"✅ Joined VC after warmup {chat_id}")
            return True, None
        except Exception as e2:
            return False, str(e2)
    except Exception as e:
        emsg = str(e)
        if "already" in emsg.lower() or "GROUPCALL_JOIN_MISSING" in emsg:
            try:
                await calls.change_stream(chat_id, build_audio_piped(stream_url))
                active_streams[stream_key] = chat_id
                logger.info(f"🔄 Changed stream in VC {chat_id}")
                return True, None
            except Exception as e2:
                return False, f"change_stream fail: {e2}"
        if any(x in emsg for x in [
            "No active group call", "GROUP_CALL_INVALID",
            "not found", "GROUPCALL_FORBIDDEN"
        ]):
            return False, f"⚠️ No active VC in group. Start a voice chat first. ({emsg})"
        return False, emsg


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

━━━━━━━ 👑  OWNER ONLY  ━━━━━━━
  /sudo <id>          • Add sudo user
  /rmsudo <id>        • Remove sudo user
  /sudolist           • List sudo users
  /owners             • Show owners

━━━━━━━ 🎵  MUSIC USERBOT  (in groups, dot-prefix)  ━━━━━━━
  .play <query/url>           • Play track INSTANTLY (streaming, no download)
  .vplay <query/url>          • Same as .play (alias)
  .pause                      • Pause stream
  .resume                     • Resume stream
  .skip                       • Skip current track
  .stop / .end                • Stop & leave VC
  .mute  / .unmute            • Stream mute
  .ping  / .alive             • Liveness check
  .auth <reply/id>            • Grant sudo on userbot
  .dauth <reply/id>           • Revoke sudo
  .authlist                   • Sudo list
  .restart                    • Restart this userbot
  .help / .commands           • This help

━━━━━━━ ⚡ Powered by @zudo_userbot ⚡ ━━━━━━━
"""


# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 MUSIC USERBOT HANDLERS  (per-host, attached via add_handler)
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


async def _safe_delete(msg: Message):
    try:
        await msg.delete()
    except Exception:
        pass

async def _auto_delete(msg: Message, after: int = 5):
    try:
        await asyncio.sleep(after)
        await msg.delete()
    except Exception:
        pass


def register_music_handlers(client: Client, host_id: int):
    """Attach all .play etc handlers using add_handler."""
    unregister_music_handlers(client, host_id)
    handler_refs = []

    async def _play(c, m: Message):
        try:
            if not _is_group_chat(m):
                return
            if not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
                return
            if len(m.command) < 2:
                tmp = await c.send_message(m.chat.id, "❌ Usage: `.play <song name / url>`")
                asyncio.create_task(_auto_delete(tmp, 5))
                return

            query = " ".join(m.command[1:])
            chat_id = m.chat.id
            calls = music_calls.get(host_id)
            if not calls:
                await m.reply_text("❌ Music backend not ready. Re-host with /start.")
                return

            # 🗑️ Delete the .play command message IMMEDIATELY
            asyncio.create_task(_safe_delete(m))

            lock = _get_play_lock(host_id, chat_id)
            if lock.locked():
                tmp = await c.send_message(chat_id, "⏳ Already preparing a track, please wait…")
                asyncio.create_task(_auto_delete(tmp, 5))
                return

            async with lock:
                # Single status message that we edit
                proc = await c.send_message(chat_id, f"🔎 Searching: **{query}**")

                # PARALLEL: get stream URL + warm peers
                url_task = asyncio.create_task(get_stream_url(query))
                warm_task = asyncio.create_task(warmup_peers(c, f"play-{chat_id}"))

                stream_url, title = await url_task
                try:
                    await asyncio.wait_for(warm_task, timeout=0.1)
                except asyncio.TimeoutError:
                    pass

                if not stream_url:
                    await proc.edit_text("❌ Couldn't fetch audio. Try another query.")
                    return

                try:
                    await proc.edit_text(f"🎵 **{title}**\n🔌 Joining voice chat…")
                except: pass

                ok, err = await join_vc_and_play(c, calls, chat_id, stream_url, f"music_{host_id}")
                if not ok:
                    await proc.edit_text(f"❌ Play failed: {err}")
                    return

                await proc.edit_text(
                    f"▶️ **Now Playing**\n"
                    f"🎵 {title}\n"
                    f"🔊 Boost: {VOLUME_BOOST}x\n"
                    f"⚡ Streaming live (no download)\n"
                    f"💬 Requested by: {m.from_user.mention}"
                )
                logger.info(f"🎵 Streaming '{title}' in chat {chat_id} via host {host_id}")
        except Exception as e:
            logger.error(f".play err: {e}")
            try: await c.send_message(m.chat.id, f"❌ {e}")
            except: pass

    async def _stop(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
            return
        asyncio.create_task(_safe_delete(m))
        try:
            calls = music_calls.get(host_id)
            await calls.leave_group_call(m.chat.id)
            active_streams.pop(f"music_{host_id}", None)
            await c.send_message(m.chat.id, "⏹ Stopped.")
        except Exception as e:
            await c.send_message(m.chat.id, f"❌ {e}")

    async def _pause(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
            return
        asyncio.create_task(_safe_delete(m))
        try:
            await music_calls[host_id].pause_stream(m.chat.id)
            await c.send_message(m.chat.id, "⏸ Paused.")
        except Exception as e:
            await c.send_message(m.chat.id, f"❌ {e}")

    async def _resume(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
            return
        asyncio.create_task(_safe_delete(m))
        try:
            await music_calls[host_id].resume_stream(m.chat.id)
            await c.send_message(m.chat.id, "▶️ Resumed.")
        except Exception as e:
            await c.send_message(m.chat.id, f"❌ {e}")

    async def _mute(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
            return
        asyncio.create_task(_safe_delete(m))
        try:
            await music_calls[host_id].mute_stream(m.chat.id)
            await c.send_message(m.chat.id, "🔇 Muted.")
        except Exception as e:
            await c.send_message(m.chat.id, f"❌ {e}")

    async def _unmute(c, m: Message):
        if not _is_group_chat(m) or not m.from_user or not _is_caller_allowed(host_id, m.from_user.id):
            return
        asyncio.create_task(_safe_delete(m))
        try:
            await music_calls[host_id].unmute_stream(m.chat.id)
            await c.send_message(m.chat.id, "🔊 Unmuted.")
        except Exception as e:
            await c.send_message(m.chat.id, f"❌ {e}")

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
        await m.reply_text(f"✅ Auth granted to {target}")

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
        await m.reply_text(f"✅ Auth removed for {target}")

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
        await m.reply_text("🏓 Pong! Music Userbot Alive 🔥")

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
# ♻️ RESTART MUSIC USERBOT  (no PyTgCalls.stop() — that attr doesn't exist)
# ═══════════════════════════════════════════════════════════════════════════════
async def restart_music_userbot(host_id: int) -> bool:
    logger.info(f"♻️ Restart requested for host {host_id}")

    # 1) Leave any active VC for this host (best effort)
    if host_id in music_calls:
        calls = music_calls[host_id]
        active_key = f"music_{host_id}"
        if active_key in active_streams:
            try:
                await calls.leave_group_call(active_streams[active_key])
            except Exception as e:
                logger.warning(f"leave_group_call err: {e}")
            active_streams.pop(active_key, None)
        # PyTgCalls has no .stop() — just drop the reference.
        # The underlying client.stop() (below) cleans up the call instance.
        music_calls.pop(host_id, None)

    # 2) Remove handlers and stop the underlying Pyrogram client
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

    # 3) Reload session from Mongo
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
        await warmup_peers(c, f"restart-{host_id}")
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
            await warmup_peers(c, "default")
            default_account = c
            default_calls = PyTgCalls(c)
            await default_calls.start()
            logger.info("✅ Default account restored")
        except (AuthKeyUnregistered, AuthKeyDuplicated,
                UserDeactivated, UserDeactivatedBan) as e:
            logger.error(f"❌ Default session DEAD ({e}). Removing from DB.")
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
            await warmup_peers(c, f"{kind}-{uid}")
            calls = PyTgCalls(c)
            await calls.start()
            if kind == "music":
                music_accounts[uid] = c
                music_calls[uid] = calls
                register_music_handlers(c, uid)
                logger.info(f"✅ Music userbot restored & handlers attached for {uid}")
            else:
                user_accounts[uid] = c
                user_calls[uid] = calls
                logger.info(f"✅ VC user {uid} restored")
        except (AuthKeyUnregistered, AuthKeyDuplicated,
                UserDeactivated, UserDeactivatedBan) as e:
            logger.error(f"❌ Session DEAD for {uid} ({kind}): {e}. Removing from DB & skipping.")
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
        "👋 Welcome! Choose what you want to do:\n\n"
        "⚔️  **VC Fight** — Stream audio loudly in any voice chat\n"
        "🎵 **Music Userbot** — Personal music bot (.play, .skip...)\n\n"
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
        await proc.edit_text("✅ Music userbot restarted! All commands ab fir se kaam karenge.")
    else:
        await proc.edit_text("❌ Restart fail ho gaya. Session expire ho sakti hai — /start se re-host karo.")


@bot.on_message(filters.command("sudo") & filters.private)
async def add_sudo(client, m: Message):
    if not is_owner(m.from_user.id):
        return
    if len(m.command) < 2:
        await m.reply_text("Usage: `/sudo <user_id>`")
        return
    try:
        uid = int(m.command[1])
        sudo_users.add(uid)
        mongo_save_sudo(uid)
        await m.reply_text(f"✅ Added sudo: `{uid}`")
    except ValueError:
        await m.reply_text("❌ Invalid user id")


@bot.on_message(filters.command("rmsudo") & filters.private)
async def rm_sudo(client, m: Message):
    if not is_owner(m.from_user.id):
        return
    if len(m.command) < 2:
        await m.reply_text("Usage: `/rmsudo <user_id>`")
        return
    try:
        uid = int(m.command[1])
        sudo_users.discard(uid)
        mongo_remove_sudo(uid)
        await m.reply_text(f"✅ Removed sudo: `{uid}`")
    except ValueError:
        await m.reply_text("❌ Invalid user id")


@bot.on_message(filters.command("sudolist") & filters.private)
async def sudo_list(client, m: Message):
    if not is_owner(m.from_user.id):
        return
    if not sudo_users:
        await m.reply_text("ℹ️ No sudo users.")
        return
    txt = "👥 **Sudo Users:**\n" + "\n".join(f"• `{u}`" for u in sudo_users)
    await m.reply_text(txt)


@bot.on_message(filters.command("stop") & filters.private)
async def stop_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_authorized(uid):
        return
    stopped = []
    # Default
    if default_calls and "default" in active_streams:
        try:
            await default_calls.leave_group_call(active_streams["default"])
            active_streams.pop("default", None)
            stopped.append("default")
        except: pass
    # User VC
    if uid in user_calls and uid in active_streams:
        try:
            await user_calls[uid].leave_group_call(active_streams[uid])
            active_streams.pop(uid, None)
            stopped.append("vc-fight")
        except: pass
    # Music
    key = f"music_{uid}"
    if uid in music_calls and key in active_streams:
        try:
            await music_calls[uid].leave_group_call(active_streams[key])
            active_streams.pop(key, None)
            stopped.append("music")
        except: pass
    if stopped:
        await m.reply_text(f"⏹ Stopped: {', '.join(stopped)}")
    else:
        await m.reply_text("ℹ️ No active streams.")


@bot.on_message(filters.command("logout") & filters.private)
async def logout_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_authorized(uid):
        return
    cleaned = []
    if uid in user_accounts:
        try: await user_accounts[uid].stop()
        except: pass
        user_accounts.pop(uid, None)
        user_calls.pop(uid, None)
        mongo_delete_session(uid, "vc")
        cleaned.append("VC")
    if uid in music_accounts:
        try:
            unregister_music_handlers(music_accounts[uid], uid)
            await music_accounts[uid].stop()
        except: pass
        music_accounts.pop(uid, None)
        music_calls.pop(uid, None)
        mongo_delete_session(uid, "music")
        mongo_remove_music_host(uid)
        cleaned.append("Music")
    if cleaned:
        await m.reply_text(f"✅ Logged out: {', '.join(cleaned)}")
    else:
        await m.reply_text("ℹ️ No active sessions.")


# ═══════════════════════════════════════════════════════════════════════════════
# 🔘 CALLBACK BUTTONS
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_callback_query()
async def cb_handler(c, q: CallbackQuery):
    if not is_authorized(q.from_user.id):
        await q.answer("❌ Not authorized", show_alert=True)
        return
    uid = q.from_user.id
    state = get_user_state(uid)
    data = q.data

    if data == "back_main":
        await q.message.edit_text(
            "👋 Main Menu\n\nChoose option:",
            reply_markup=build_welcome_keyboard()
        )
        return

    if data == "show_help":
        await q.message.edit_text(f"```{HELP_TEXT}```", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("« Back", callback_data="back_main")]
        ]))
        return

    if data == "menu_vcfight":
        await q.message.edit_text(
            "⚔️ **VC Fight Mode**\n\n"
            "• Default Account: instantly stream using global default session\n"
            "• Login My Account: login your own number to stream from it",
            reply_markup=build_vcfight_keyboard()
        )
        return

    if data == "menu_music":
        await q.message.edit_text(
            "🎵 **Music Userbot**\n\n"
            "Host your personal music userbot. Add to groups and use `.play`.",
            reply_markup=build_music_keyboard(uid)
        )
        return

    if data == "use_default":
        if not default_account:
            if is_owner(uid):
                state.step = "default_phone"
                state.data = {"mode": "default"}
                await q.message.edit_text(
                    "🌟 **Setup Default Account**\n\n"
                    "Send phone number with country code (e.g. +91xxxxxxxxxx)"
                )
            else:
                await q.message.edit_text("❌ Default not configured. Ask owner.")
            return
        state.step = "default_group"
        state.data = {"mode": "default"}
        await q.message.edit_text(
            "✅ Default account ready!\n\n"
            "📎 Send group info (@username or invite link):"
        )
        return

    if data == "use_custom":
        state.step = "custom_phone"
        state.data = {"mode": "custom"}
        await q.message.edit_text(
            "🔐 **Login Your Account**\n\n"
            "Send phone number with country code (e.g. +91xxxxxxxxxx)"
        )
        return

    if data == "music_host":
        state.step = "music_phone"
        state.data = {"mode": "music"}
        await q.message.edit_text(
            "🎤 **Host Music Userbot**\n\n"
            "Send phone number with country code (e.g. +91xxxxxxxxxx)"
        )
        return

    if data == "music_commands":
        await q.message.edit_text(f"```{HELP_TEXT}```", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("« Back", callback_data="menu_music")]
        ]))
        return

    if data == "music_restart":
        await q.answer("Restarting…")
        ok = await restart_music_userbot(uid)
        if ok:
            await q.message.edit_text(
                "✅ Music userbot restarted!",
                reply_markup=build_music_keyboard(uid)
            )
        else:
            await q.message.edit_text(
                "❌ Restart failed. Re-host with /start.",
                reply_markup=build_welcome_keyboard()
            )
        return

    if data == "music_logout":
        if uid in music_accounts:
            try:
                unregister_music_handlers(music_accounts[uid], uid)
                await music_accounts[uid].stop()
            except: pass
            music_accounts.pop(uid, None)
            music_calls.pop(uid, None)
        mongo_delete_session(uid, "music")
        mongo_remove_music_host(uid)
        await q.message.edit_text(
            "✅ Logged out from music userbot.",
            reply_markup=build_welcome_keyboard()
        )
        return


# ═══════════════════════════════════════════════════════════════════════════════
# 🔐 TEXT-BASED OTP HANDLING  (NO KEYPAD!)
# ═══════════════════════════════════════════════════════════════════════════════
def _clean_otp(text: str) -> str:
    """Accept '1 2 3 4 5', '1-2-3-4-5', '12345' etc — strip everything non-digit."""
    return re.sub(r'\D', '', text or '')


async def verify_otp_text(message: Message, mode: str, otp: str):
    uid = message.from_user.id
    state = get_user_state(uid)
    proc = await message.reply_text("⏳ Verifying OTP…")

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
            "Send your 2FA password as text message now."
        )
    except PhoneCodeInvalid:
        await proc.edit_text("❌ Invalid OTP! Run /start again.")
        state.step = None
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
    asyncio.create_task(warmup_peers(uc, f"login-{mode}"))

    if mode in ("default_otp", "default_2fa"):
        default_account = uc
        default_calls = PyTgCalls(uc)
        await default_calls.start()
        mongo_save_session(uid, state.data.get("phone", ""), sess_str, "default")
        await proc_msg.edit_text("✅ **Default account configured!**\nSaved to MongoDB. 🎉")

    elif mode in ("custom_otp", "custom_2fa"):
        user_accounts[uid] = uc
        user_calls[uid] = PyTgCalls(uc)
        await user_calls[uid].start()
        mongo_save_session(uid, state.data.get("phone", ""), sess_str, "vc")
        state.step = "custom_group"
        await proc_msg.edit_text(
            "✅ Logged in successfully!\n\n"
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
            "Your userbot is now live. Add it to any group and use `.play <song>`.\n"
            "Send /help for full command list."
        )

    state.step = None
    logger.info(f"✅ Finalize complete for {uid} (mode={mode})")


# ═══════════════════════════════════════════════════════════════════════════════
# 💬 TEXT MESSAGE HANDLER  (handles OTP as text now)
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
        # ── PHONE STEP ─────────────────────────────────────────────
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
                    "Telegram pe jo OTP code aaya hai, usse **text me likh ke send karo**.\n\n"
                    "✅ Examples (sab chalega):\n"
                    "   • `1 2 3 4 5`\n"
                    "   • `12345`\n"
                    "   • `1-2-3-4-5`\n\n"
                    "(Spaces / dashes apne aap remove ho jayenge.)"
                )
            except FloodWait as e:
                await proc.edit_text(f"⏳ Flood wait: {e.value}s"); state.step = None
            except Exception as e:
                await proc.edit_text(f"❌ {e}"); state.step = None
            return

        # ── OTP STEP (text-based) ──────────────────────────────────
        if state.step in ("default_otp", "custom_otp", "music_otp"):
            otp = _clean_otp(text)
            if len(otp) < 4:
                await m.reply_text(
                    "❌ OTP me kam se kam 4 digits hone chahiye.\n"
                    "Format: `1 2 3 4 5` ya `12345`"
                )
                return
            await verify_otp_text(m, state.step, otp)
            return

        # ── 2FA STEP ───────────────────────────────────────────────
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

        # ── CHAT ID STEP ───────────────────────────────────────────
        if state.step == "waiting_chat_id":
            try:
                cid = int(text.strip())
                state.data["actual_chat_id"] = cid
                state.step = "audio_input"
                await m.reply_text(f"✅ Chat ID set: `{cid}`\n\n🎵 Send Audio / YouTube URL / Song name")
            except ValueError:
                await m.reply_text("❌ Invalid! Send like: `-100123456789`")
            return

        # ── GROUP INFO STEP ────────────────────────────────────────
        if state.step in ("default_group", "custom_group"):
            ci = extract_chat_info(text)
            if not ci:
                await m.reply_text("❌ Invalid input. Send @username or https://t.me/+xxx")
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
                    await proc.edit_text(f"✅ Group: **{title}**\n\n🎵 Send Audio / YouTube URL / Song name")
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
                    "🔒 Private Group\n\nPlease send the Chat ID (e.g. `-100123456789`)\n\n"
                    "👉 Forward any group msg to @username_to_id_bot to get it.")
            return

        # ── AUDIO INPUT STEP (streaming) ───────────────────────────
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
            proc = await m.reply_text("⏳ Processing...")
            actual_id = state.data.get("actual_chat_id")
            title = state.data.get("chat_title", "Group")
            if not actual_id:
                ok, actual_id, title, err, _ = await get_chat_id_smart(client_to_use, ci, stream_key)
                if not ok:
                    await proc.edit_text(err); state.step = None; return

            await proc.edit_text("⚡ Resolving stream URL...")

            # PARALLEL: stream URL + peer warmup
            url_task = asyncio.create_task(get_stream_url(text))
            warm_task = asyncio.create_task(warmup_peers(client_to_use, f"vcfight-{actual_id}"))

            stream_url, song_title = await url_task
            try:
                await asyncio.wait_for(warm_task, timeout=0.1)
            except asyncio.TimeoutError:
                pass

            if not stream_url:
                await proc.edit_text("❌ Couldn't fetch audio"); state.step = None; return

            await proc.edit_text("🔌 Joining voice chat...")
            ok, err = await join_vc_and_play(client_to_use, calls_to_use, actual_id, stream_url, stream_key)
            if ok:
                await proc.edit_text(
                    f"✅ **NOW PLAYING** 🔥\n\n📻 Group: **{title}**\n🎵 {song_title or 'Audio'}\n"
                    f"🔊 BOOST: **{VOLUME_BOOST}x** — speaker phaadne wala 😈\n"
                    f"⚡ Streaming live (no download)\n\nUse /stop to stop")
            else:
                await proc.edit_text(f"❌ Play failed: {err}")
            state.step = None
            return

    except Exception as e:
        logger.error(f"msg_handler err: {e}")
        await m.reply_text(f"❌ {e}")
        state.step = None


# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 AUDIO / VOICE FILE HANDLER (uploaded files still play — they're local already)
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
    os.makedirs("/tmp/downloads", exist_ok=True)
    raw = await m.download(file_name=f"/tmp/downloads/{m.id}.mp3")
    actual = state.data.get("actual_chat_id")
    title = state.data.get("chat_title", "Group")
    await proc.edit_text("🔌 Joining VC with 🔊 BOOST...")
    # Local file path works with same AudioPiped + ffmpeg filter
    ok, err = await join_vc_and_play(cu, calls, actual, raw, sk)
    if ok:
        await proc.edit_text(f"✅ NOW PLAYING 🔥\n📻 {title}\n🔊 BOOST {VOLUME_BOOST}x")
    else:
        await proc.edit_text(f"❌ {err}")
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
    print("🔥  ZUDO USERBOT — GOD-LEVEL EDITION v5 (INSTANT STREAMING)")
    print("="*68)
    print(f"   👑 Primary Owner : {OWNER_ID}")
    print(f"   👑 Co-Owner      : {CO_OWNER_ID}")
    print(f"   🗄️  MongoDB       : {'OK' if MONGO_OK else 'FAIL'}")
    print(f"   🔊 Volume Boost  : {VOLUME_BOOST}x")
    print(f"   🍪 YT Cookies    : {'YES' if os.path.exists(YT_COOKIES) else 'NO  (recommended)'}")
    print(f"   🔑 Bot Token     : {'*'*8}{BOT_TOKEN[-4:] if BOT_TOKEN else '????'}")
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
