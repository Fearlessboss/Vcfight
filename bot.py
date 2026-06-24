"""
🔥 ZUDO USERBOT - VC FIGHT + MUSIC USERBOT (FULL UPGRADED)
- Welcome with 2 inline boxes: VC Fight & Music Userbot
- VC Fight: Default Account + Login My Account (Telephone-style OTP keypad)
- MongoDB persistent memory (sessions, sudo, cache, hosts)
- Loud audio boost
- Music Userbot: .play .skip ... 20+ commands via /commands
- .auth / .dauth for sudo control
- yt-dlp + nubcoder API for fast YouTube playback
Powered by @zudo_userbot
"""

import os
import asyncio
import re
import json
import logging
import requests
from pathlib import Path
from datetime import datetime

from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Message,
    CallbackQuery
)
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid,
    PasswordHashInvalid, FloodWait, UserAlreadyParticipant,
    InviteHashExpired
)
from pytgcalls import PyTgCalls, StreamType
from pytgcalls.types.input_stream import AudioPiped
from pytgcalls.types.input_stream.quality import HighQualityAudio
import yt_dlp
from pymongo import MongoClient

# ═══════════════════════════════════════════════════════════════════════════════
# 🔧 CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
OWNER_ID = 7661825494
BOT_TOKEN = "7845373810:AAFQlhmmDstNi48ZJAbruihSrM4YWIzOt2U"
API_ID = 33628258
API_HASH = "0850762925b9c1715b9b122f7b753128"

# MongoDB - persistent memory across restarts
MONGO_URL = "mongodb+srv://moderatorhelperorg_db_user:nze86usap2dYthZN@cluster0.uokrixs.mongodb.net/mydatabase?retryWrites=true&w=majority"

# Nubcoder API for fast YT extraction
NUBCODER_TOKEN = "4HBcMS072p"
NUBCODER_API = f"http://api.nubcoder.com/info?token={NUBCODER_TOKEN}&q={{}}"

# Audio Volume Boost (1.0 = normal, 4.0 = LOUD AF 🔥)
VOLUME_BOOST = 4.0

# Directories
Path("/tmp/downloads").mkdir(exist_ok=True, parents=True)
Path("/app/sessions").mkdir(exist_ok=True, parents=True)
Path("/app/data").mkdir(exist_ok=True, parents=True)
Path("/app/cookies").mkdir(exist_ok=True, parents=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# 🗄️  MONGODB - PERSISTENT MEMORY
# ═══════════════════════════════════════════════════════════════════════════════
try:
    mongo = MongoClient(MONGO_URL, serverSelectionTimeoutMS=10000)
    mongo.server_info()  # force connection test
    db = mongo["zudo_userbot"]
    col_sudo = db["sudo_users"]
    col_cache = db["chat_cache"]
    col_sessions = db["sessions"]      # phone+session_string for users
    col_default = db["default_acc"]    # default account session
    col_music_hosts = db["music_hosts"]  # music userbot hosts
    col_music_sudo = db["music_sudo"]    # per-host sudo list
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

# In-memory runtime state
user_states = {}
default_account = None
default_calls = None
user_accounts = {}     # {user_id: Client}        VC Fight
user_calls = {}        # {user_id: PyTgCalls}     VC Fight
music_accounts = {}    # {user_id: Client}        Music Userbot
music_calls = {}       # {user_id: PyTgCalls}     Music Userbot
active_streams = {}    # stream_key -> chat_id

# ═══════════════════════════════════════════════════════════════════════════════
# 💾 MONGO HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def mongo_save_sudo(user_id):
    if MONGO_OK:
        col_sudo.update_one({"_id": user_id}, {"$set": {"_id": user_id}}, upsert=True)

def mongo_remove_sudo(user_id):
    if MONGO_OK:
        col_sudo.delete_one({"_id": user_id})

def mongo_get_sudo():
    if not MONGO_OK:
        return set()
    return {d["_id"] for d in col_sudo.find({})}

def mongo_save_cache(key, chat_id, title):
    if MONGO_OK:
        col_cache.update_one(
            {"_id": key},
            {"$set": {"chat_id": chat_id, "title": title}},
            upsert=True
        )

def mongo_get_cache(key):
    if not MONGO_OK:
        return None
    d = col_cache.find_one({"_id": key})
    return (d["chat_id"], d["title"]) if d else None

def mongo_save_session(user_id, phone, session_string, kind="vc"):
    """kind: 'vc' (VC Fight) | 'music' (Music Userbot) | 'default'"""
    if not MONGO_OK:
        return
    collection = col_sessions if kind != "default" else col_default
    doc_id = user_id if kind != "default" else "default"
    collection.update_one(
        {"_id": doc_id},
        {"$set": {
            "user_id": user_id,
            "phone": phone,
            "session_string": session_string,
            "kind": kind,
            "saved_at": datetime.utcnow()
        }},
        upsert=True
    )

def mongo_get_session(user_id, kind="vc"):
    if not MONGO_OK:
        return None
    collection = col_sessions if kind != "default" else col_default
    doc_id = user_id if kind != "default" else "default"
    return collection.find_one({"_id": doc_id})

def mongo_delete_session(user_id, kind="vc"):
    if not MONGO_OK:
        return
    collection = col_sessions if kind != "default" else col_default
    doc_id = user_id if kind != "default" else "default"
    collection.delete_one({"_id": doc_id})

def mongo_save_music_host(user_id):
    if MONGO_OK:
        col_music_hosts.update_one({"_id": user_id}, {"$set": {"_id": user_id}}, upsert=True)

def mongo_remove_music_host(user_id):
    if MONGO_OK:
        col_music_hosts.delete_one({"_id": user_id})

def mongo_is_music_host(user_id):
    if not MONGO_OK:
        return False
    return col_music_hosts.find_one({"_id": user_id}) is not None

def mongo_add_music_sudo(host_id, sudo_id):
    if MONGO_OK:
        col_music_sudo.update_one(
            {"_id": f"{host_id}:{sudo_id}"},
            {"$set": {"host_id": host_id, "sudo_id": sudo_id}},
            upsert=True
        )

def mongo_remove_music_sudo(host_id, sudo_id):
    if MONGO_OK:
        col_music_sudo.delete_one({"_id": f"{host_id}:{sudo_id}"})

def mongo_get_music_sudo(host_id):
    if not MONGO_OK:
        return set()
    return {d["sudo_id"] for d in col_music_sudo.find({"host_id": host_id})}

# Load sudo into memory
sudo_users = mongo_get_sudo()

# ═══════════════════════════════════════════════════════════════════════════════
# 🧠 STATE
# ═══════════════════════════════════════════════════════════════════════════════
class UserState:
    def __init__(self):
        self.step = None
        self.data = {}
        self.otp_buffer = ""  # for keypad OTP

def get_user_state(user_id):
    if user_id not in user_states:
        user_states[user_id] = UserState()
    return user_states[user_id]

def is_authorized(user_id):
    return user_id == OWNER_ID or user_id in sudo_users

# ═══════════════════════════════════════════════════════════════════════════════
# 🎹 OTP KEYPAD UI (telephone style)
# ═══════════════════════════════════════════════════════════════════════════════
def build_otp_keypad(buffer: str, mode: str):
    """mode = 'default_otp' or 'custom_otp' or 'music_otp'"""
    display = " ".join(list(buffer)) if buffer else "—"
    rows = [
        [InlineKeyboardButton(f"📱  {display}", callback_data="otp_noop")],
        [
            InlineKeyboardButton("1", callback_data=f"otp_{mode}_1"),
            InlineKeyboardButton("2", callback_data=f"otp_{mode}_2"),
            InlineKeyboardButton("3", callback_data=f"otp_{mode}_3"),
        ],
        [
            InlineKeyboardButton("4", callback_data=f"otp_{mode}_4"),
            InlineKeyboardButton("5", callback_data=f"otp_{mode}_5"),
            InlineKeyboardButton("6", callback_data=f"otp_{mode}_6"),
        ],
        [
            InlineKeyboardButton("7", callback_data=f"otp_{mode}_7"),
            InlineKeyboardButton("8", callback_data=f"otp_{mode}_8"),
            InlineKeyboardButton("9", callback_data=f"otp_{mode}_9"),
        ],
        [
            InlineKeyboardButton("《 Back", callback_data=f"otp_{mode}_back"),
            InlineKeyboardButton("0", callback_data=f"otp_{mode}_0"),
            InlineKeyboardButton("✅ Submit", callback_data=f"otp_{mode}_submit"),
        ],
    ]
    return InlineKeyboardMarkup(rows)

def build_welcome_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⚔️  VC Fight", callback_data="menu_vcfight"),
            InlineKeyboardButton("🎵 Music Userbot", callback_data="menu_music"),
        ]
    ])

def build_vcfight_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🌟 Default Account", callback_data="use_default"),
            InlineKeyboardButton("🔐 Login My Account", callback_data="use_custom"),
        ],
        [InlineKeyboardButton("« Back", callback_data="back_main")]
    ])

def build_music_keyboard(user_id):
    if mongo_is_music_host(user_id) or user_id in music_accounts:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📜 /commands", callback_data="music_commands")],
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

async def is_public_group(client, chat_info):
    """Returns (is_public, chat_id, title) — used to decide whether to ask Chat ID."""
    if chat_info["type"] == "username":
        # username = public
        try:
            chat = await client.get_chat(chat_info["value"])
            return True, chat.id, chat.title
        except Exception:
            return True, None, None
    return False, None, None

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
        return None, None
    except Exception as e:
        logger.error(f"find_chat_in_dialogs err: {e}")
        return None, None

async def get_chat_id_smart(client, chat_info, user_key, force_ask_id=False):
    """Returns (success, chat_id, chat_title, error_msg, needs_chat_id)"""
    try:
        # For PRIVATE groups -> always ask for Chat ID (per user request)
        if chat_info["type"] == "invite" and force_ask_id:
            return False, None, None, (
                "🔒 Private Group Detected\n\n"
                "Please send the Chat ID now (e.g. -100123456789)\n\n"
                "How to get it:\n"
                "1. Forward any group message to @username_to_id_bot\n"
                "2. Copy the Chat ID it gives you\n"
                "3. Paste here"
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
                return False, None, None, (
                    "🔒 Already member but can't auto-detect group.\n"
                    "Please send the Chat ID (e.g. -100123456789)"
                ), True
            except InviteHashExpired:
                return False, None, None, "❌ Invite link expired!", False
            except Exception as e:
                return False, None, None, f"❌ {e}", False
    except Exception as e:
        return False, None, None, f"❌ Unexpected: {e}", False

# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 AUDIO HELPERS — LOUD BOOST + DOWNLOAD
# ═══════════════════════════════════════════════════════════════════════════════
async def boost_audio(input_path: str, gain: float = VOLUME_BOOST) -> str:
    """Boost audio volume using ffmpeg — speaker phaad de 😂"""
    try:
        out = input_path.rsplit(".", 1)[0] + "_BOOSTED.mp3"
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", input_path,
            "-filter:a", f"volume={gain},loudnorm=I=-12:LRA=11:TP=-1",
            "-b:a", "192k",
            out,
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

async def nubcoder_get_stream(query_or_url: str):
    """Try nubcoder API first — super fast direct stream URL."""
    try:
        url = NUBCODER_API.format(query_or_url)
        loop = asyncio.get_event_loop()
        r = await loop.run_in_executor(None, lambda: requests.get(url, timeout=15))
        if r.status_code == 200:
            data = r.json()
            # try common keys
            for key in ("url", "audio_url", "stream", "stream_url", "direct"):
                if key in data and data[key]:
                    return data[key], data.get("title", "Unknown")
        return None, None
    except Exception as e:
        logger.error(f"nubcoder err: {e}")
        return None, None

async def download_youtube_audio(url_or_query: str):
    # Try fast API first
    stream_url, title = await nubcoder_get_stream(url_or_query)
    if stream_url:
        try:
            output_path = f'/tmp/downloads/{int(asyncio.get_event_loop().time())}.mp3'
            loop = asyncio.get_event_loop()
            def _dl():
                with requests.get(stream_url, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    with open(output_path, "wb") as f:
                        for chunk in r.iter_content(8192):
                            f.write(chunk)
            await loop.run_in_executor(None, _dl)
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1000:
                return await boost_audio(output_path), title
        except Exception as e:
            logger.error(f"nubcoder dl fail: {e}")

    # Fallback yt-dlp
    try:
        output_path = f'/tmp/downloads/{int(asyncio.get_event_loop().time())}'
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': f'{output_path}.%(ext)s',
            'quiet': True,
            'no_warnings': True,
            'nocheckcertificate': True,
            'default_search': 'ytsearch',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
        }
        loop = asyncio.get_event_loop()
        info_holder = {}
        def _run():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url_or_query, download=True)
                if 'entries' in info:
                    info = info['entries'][0]
                info_holder['title'] = info.get('title', 'Unknown')
        await loop.run_in_executor(None, _run)
        final = f"{output_path}.mp3"
        if os.path.exists(final):
            return await boost_audio(final), info_holder.get('title', 'Unknown')
        return None, None
    except Exception as e:
        logger.error(f"yt-dlp err: {e}")
        return None, None

async def get_audio_duration(file_path: str) -> int:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        out, _ = await proc.communicate()
        s = out.decode().strip()
        return int(float(s)) if s else 0
    except:
        return 0

async def auto_leave_after_playback(calls, stream_key, chat_id, audio_path):
    try:
        dur = await get_audio_duration(audio_path)
        if dur > 0:
            await asyncio.sleep(dur + 3)
        try:
            await calls.leave_group_call(chat_id)
        except:
            pass
        active_streams.pop(stream_key, None)
    except Exception as e:
        logger.error(f"auto-leave err: {e}")

async def cleanup_file(file_path):
    try:
        await asyncio.sleep(300)
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
    except:
        pass

async def rejoin_and_play(client, calls, chat_id, audio_path, stream_key):
    try:
        try:
            await calls.leave_group_call(chat_id)
            await asyncio.sleep(2)
        except:
            pass
        try:
            await client.leave_chat(chat_id)
            await asyncio.sleep(3)
        except Exception as e:
            return False, f"Cannot leave: {e}"
        try:
            chat = await client.get_chat(chat_id)
            if getattr(chat, 'username', None):
                await client.join_chat(chat.username)
            else:
                return False, "Send invite link again"
            await asyncio.sleep(2)
        except Exception as e:
            return False, f"Cannot rejoin: {e}"
        try:
            await calls.join_group_call(
                chat_id,
                AudioPiped(audio_path, HighQualityAudio()),
                stream_type=StreamType().pulse_stream
            )
            active_streams[stream_key] = chat_id
            return True, None
        except Exception as e:
            return False, f"VC join failed: {e}"
    except Exception as e:
        return False, f"Rejoin err: {e}"

# ═══════════════════════════════════════════════════════════════════════════════
# 🔄 AUTO LOAD SAVED SESSIONS ON STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
async def restore_sessions():
    global default_account, default_calls
    if not MONGO_OK:
        return
    # Restore default account
    d = col_default.find_one({"_id": "default"})
    if d and d.get("session_string"):
        try:
            c = Client(
                "default_session_restored",
                api_id=API_ID, api_hash=API_HASH,
                session_string=d["session_string"],
                in_memory=True
            )
            await c.start()
            default_account = c
            default_calls = PyTgCalls(c)
            await default_calls.start()
            logger.info("✅ Default account restored from MongoDB")
        except Exception as e:
            logger.error(f"Default restore fail: {e}")

    # Restore VC fight + music users
    for s in col_sessions.find({}):
        uid = s.get("user_id")
        kind = s.get("kind", "vc")
        sess = s.get("session_string")
        if not uid or not sess:
            continue
        try:
            name = f"user_{uid}_{kind}_restored"
            c = Client(name, api_id=API_ID, api_hash=API_HASH,
                       session_string=sess, in_memory=True)
            await c.start()
            calls = PyTgCalls(c)
            await calls.start()
            if kind == "music":
                music_accounts[uid] = c
                music_calls[uid] = calls
                # register music handlers for this userbot
                register_music_handlers(c, uid)
                logger.info(f"✅ Music userbot restored for {uid}")
            else:
                user_accounts[uid] = c
                user_calls[uid] = calls
                logger.info(f"✅ VC user {uid} restored")
        except Exception as e:
            logger.error(f"Restore fail for {uid}: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 🎵 MUSIC USERBOT — Commands ( .play, .skip, .auth, etc. )
# ═══════════════════════════════════════════════════════════════════════════════
MUSIC_COMMANDS_TEXT = """
╔═══════════════════════════════════╗
║   🎵  MUSIC USERBOT COMMANDS  🎵   ║
╚═══════════════════════════════════╝

━━━━━━━ 🎧  PLAYBACK  ━━━━━━━
  .play  <song name / yt url>
  .vplay <song name / yt url>     • video
  .pause
  .resume
  .skip
  .stop
  .end
  .seek  <seconds>
  .replay

━━━━━━━ 📜  QUEUE  ━━━━━━━
  .queue
  .clear
  .shuffle
  .loop  <on/off>

━━━━━━━ 🔊  VOLUME  ━━━━━━━
  .volume <1-200>
  .boost           • turbo loud 🔥
  .mute
  .unmute

━━━━━━━ 👥  ADMIN  ━━━━━━━
  .auth  <reply/userid>
  .dauth <reply/userid>
  .authlist
  .ping
  .alive
  .stats

━━━━━━━ ℹ️  Use only by Host or Sudo ━━━━━━━
"""

def register_music_handlers(client: Client, host_id: int):
    """Attach .play .skip etc. handlers to the user's own userbot client."""
    @client.on_message(filters.command(["play"], prefixes=".") & filters.group)
    async def _play(c, m: Message):
        if m.from_user.id != host_id and m.from_user.id not in mongo_get_music_sudo(host_id) and m.from_user.id != OWNER_ID:
            return
        if len(m.command) < 2:
            await m.reply_text("❌ Usage: `.play <song name>`")
            return
        query = " ".join(m.command[1:])
        proc = await m.reply_text(f"🔎 Searching: `{query}`")
        audio_path, title = await download_youtube_audio(query)
        if not audio_path:
            await proc.edit_text("❌ Couldn't fetch audio")
            return
        try:
            calls = music_calls.get(host_id)
            await calls.join_group_call(
                m.chat.id,
                AudioPiped(audio_path, HighQualityAudio()),
                stream_type=StreamType().pulse_stream
            )
            active_streams[f"music_{host_id}"] = m.chat.id
            asyncio.create_task(auto_leave_after_playback(calls, f"music_{host_id}", m.chat.id, audio_path))
            await proc.edit_text(f"▶️ **Now Playing**\n🎵 `{title}`\n🔊 BOOST: {VOLUME_BOOST}x")
        except Exception as e:
            await proc.edit_text(f"❌ Play err: {e}")
        finally:
            asyncio.create_task(cleanup_file(audio_path))

    @client.on_message(filters.command(["skip", "stop", "end"], prefixes=".") & filters.group)
    async def _stop(c, m: Message):
        if m.from_user.id != host_id and m.from_user.id not in mongo_get_music_sudo(host_id) and m.from_user.id != OWNER_ID:
            return
        try:
            calls = music_calls.get(host_id)
            await calls.leave_group_call(m.chat.id)
            active_streams.pop(f"music_{host_id}", None)
            await m.reply_text("⏹ Stopped.")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @client.on_message(filters.command(["pause"], prefixes=".") & filters.group)
    async def _pause(c, m: Message):
        if m.from_user.id != host_id and m.from_user.id not in mongo_get_music_sudo(host_id):
            return
        try:
            await music_calls[host_id].pause_stream(m.chat.id)
            await m.reply_text("⏸ Paused.")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @client.on_message(filters.command(["resume"], prefixes=".") & filters.group)
    async def _resume(c, m: Message):
        if m.from_user.id != host_id and m.from_user.id not in mongo_get_music_sudo(host_id):
            return
        try:
            await music_calls[host_id].resume_stream(m.chat.id)
            await m.reply_text("▶️ Resumed.")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @client.on_message(filters.command(["mute"], prefixes=".") & filters.group)
    async def _mute(c, m: Message):
        if m.from_user.id != host_id and m.from_user.id not in mongo_get_music_sudo(host_id):
            return
        try:
            await music_calls[host_id].mute_stream(m.chat.id)
            await m.reply_text("🔇 Muted.")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @client.on_message(filters.command(["unmute"], prefixes=".") & filters.group)
    async def _unmute(c, m: Message):
        if m.from_user.id != host_id and m.from_user.id not in mongo_get_music_sudo(host_id):
            return
        try:
            await music_calls[host_id].unmute_stream(m.chat.id)
            await m.reply_text("🔊 Unmuted.")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @client.on_message(filters.command(["auth"], prefixes=".") & filters.group)
    async def _auth(c, m: Message):
        if m.from_user.id != host_id and m.from_user.id != OWNER_ID:
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

    @client.on_message(filters.command(["dauth"], prefixes=".") & filters.group)
    async def _dauth(c, m: Message):
        if m.from_user.id != host_id and m.from_user.id != OWNER_ID:
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

    @client.on_message(filters.command(["authlist"], prefixes=".") & filters.group)
    async def _authlist(c, m: Message):
        if m.from_user.id != host_id and m.from_user.id != OWNER_ID:
            return
        ids = mongo_get_music_sudo(host_id)
        if not ids:
            await m.reply_text("ℹ️ No sudo users.")
            return
        txt = "👥 **Sudo list:**\n" + "\n".join(f"• `{i}`" for i in ids)
        await m.reply_text(txt)

    @client.on_message(filters.command(["ping", "alive"], prefixes=".") & filters.group)
    async def _ping(c, m: Message):
        await m.reply_text("🏓 **Pong!** Music Userbot Alive 🔥")

# ═══════════════════════════════════════════════════════════════════════════════
# 📨 BOT START / SUDO / STOP
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_message(filters.command("start") & filters.private)
async def start_command(client, m: Message):
    if not is_authorized(m.from_user.id):
        await m.reply_text("❌ You don't have permission to use this bot!")
        return
    txt = (
        "╔══════════════════════════════╗\n"
        "║   🎵  ZUDO USERBOT  🎵     ║\n"
        "╚══════════════════════════════╝\n\n"
        "👋 **Welcome!** Choose what you want to do:\n\n"
        "⚔️  **VC Fight** — Stream audio loudly in any voice chat\n"
        "🎵 **Music Userbot** — Personal music bot (`.play`, `.skip`...)\n\n"
        "🔥 Powered by @zudo_userbot"
    )
    await m.reply_text(txt, reply_markup=build_welcome_keyboard())

@bot.on_message(filters.command("commands") & filters.private)
async def commands_cmd(c, m: Message):
    if not is_authorized(m.from_user.id):
        return
    await m.reply_text(f"```{MUSIC_COMMANDS_TEXT}```")

@bot.on_message(filters.command("sudo") & filters.private & filters.user(OWNER_ID))
async def add_sudo(client, m: Message):
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

@bot.on_message(filters.command("rmsudo") & filters.private & filters.user(OWNER_ID))
async def rm_sudo(client, m: Message):
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

@bot.on_message(filters.command("sudolist") & filters.private & filters.user(OWNER_ID))
async def list_sudo(client, m: Message):
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
                stopped = True
            except:
                pass
    await m.reply_text("✅ Stopped." if stopped else "❌ No active stream.")

# ═══════════════════════════════════════════════════════════════════════════════
# 🔘 CALLBACK ROUTER
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_callback_query()
async def cb_handler(client, cq: CallbackQuery):
    uid = cq.from_user.id
    data = cq.data
    state = get_user_state(uid)

    if not is_authorized(uid):
        await cq.answer("❌ No permission!", show_alert=True)
        return

    # ─── MAIN MENU ───
    if data == "back_main":
        await cq.message.edit_text(
            "👋 **Welcome!** Choose what you want to do:",
            reply_markup=build_welcome_keyboard()
        )
        return

    if data == "menu_vcfight":
        await cq.message.edit_text(
            "⚔️ **VC Fight Mode**\n\nPick an account to stream from:",
            reply_markup=build_vcfight_keyboard()
        )
        return

    if data == "menu_music":
        txt = (
            "🎵 **Music Userbot**\n\n"
            "Personal music bot — host once, then use `.play`, `.skip` etc. in any group.\n\n"
        )
        if mongo_is_music_host(uid) or uid in music_accounts:
            txt += "✅ Already hosted! Use `/commands` to see all commands."
        else:
            txt += "Tap below to host your userbot."
        await cq.message.edit_text(txt, reply_markup=build_music_keyboard(uid))
        return

    # ─── OTP KEYPAD ROUTER ───
    if data.startswith("otp_"):
        parts = data.split("_", 2)  # otp, mode, key
        # data like otp_default_otp_5 -> tricky, redo
        # use format otp:<mode>:<key>
        # rewrite: we used otp_<mode>_<key>
        # parts: ['otp', 'default', 'otp_5'] ← because mode has underscore
        # Simpler: rebuild
        raw = data[4:]  # strip "otp_"
        # last token is key, rest is mode
        last_us = raw.rfind("_")
        mode = raw[:last_us]
        key = raw[last_us+1:]

        if key == "noop":
            await cq.answer()
            return

        if key == "back":
            state.otp_buffer = state.otp_buffer[:-1]
        elif key == "submit":
            otp = state.otp_buffer
            if len(otp) < 4:
                await cq.answer("Enter full OTP first", show_alert=True)
                return
            await cq.answer("⏳ Verifying...")
            await verify_otp(cq, mode, otp)
            return
        else:
            if len(state.otp_buffer) < 6:
                state.otp_buffer += key
        # update keypad
        try:
            await cq.message.edit_reply_markup(build_otp_keypad(state.otp_buffer, mode))
        except:
            pass
        await cq.answer()
        return

    # ─── VC FIGHT — Default ───
    if data == "use_default":
        if default_account is None:
            if uid == OWNER_ID:
                state.step = "default_phone"
                state.data = {"mode": "default"}
                await cq.message.reply_text("📱 **Setup Default Account**\n\nSend phone number with country code:")
            else:
                await cq.answer("❌ Default not configured by owner yet", show_alert=True)
            return
        state.step = "default_group"
        state.data = {"mode": "default"}
        await cq.message.reply_text(
            "📎 **Send Group Info**\n\n"
            "🌐 Public:  `@groupusername`\n"
            "🔒 Private: `https://t.me/+xxxxx`\n\n"
            "_Private groups will ask for Chat ID each time._"
        )
        return

    if data == "use_custom":
        sess = mongo_get_session(uid, "vc")
        if sess or uid in user_accounts:
            state.step = "custom_group"
            state.data = {"mode": "custom"}
            await cq.message.reply_text(
                "✅ Already logged in!\n\n📎 Send group info (`@username` or invite link):"
            )
        else:
            state.step = "custom_phone"
            state.data = {"mode": "custom"}
            await cq.message.reply_text("📱 **Login My Account**\n\nSend your phone number with country code:")
        return

    # ─── MUSIC USERBOT ───
    if data == "music_host":
        if mongo_is_music_host(uid) or uid in music_accounts:
            await cq.answer("Already hosted!", show_alert=True)
            return
        state.step = "music_phone"
        state.data = {"mode": "music"}
        await cq.message.reply_text("🎤 **Host Music Userbot**\n\nSend phone number with country code:")
        return

    if data == "music_commands":
        await cq.message.reply_text(f"```{MUSIC_COMMANDS_TEXT}```")
        return

    if data == "music_logout":
        if uid in music_accounts:
            try:
                await music_calls[uid].stop()
            except:
                pass
            try:
                await music_accounts[uid].stop()
            except:
                pass
            music_accounts.pop(uid, None)
            music_calls.pop(uid, None)
        mongo_remove_music_host(uid)
        mongo_delete_session(uid, "music")
        await cq.message.reply_text("✅ Music userbot logged out.")
        return

    await cq.answer()

# ═══════════════════════════════════════════════════════════════════════════════
# 🔑 OTP VERIFY
# ═══════════════════════════════════════════════════════════════════════════════
async def verify_otp(cq: CallbackQuery, mode: str, otp: str):
    """mode = default_otp | custom_otp | music_otp"""
    global default_account, default_calls
    uid = cq.from_user.id
    state = get_user_state(uid)
    state.otp_buffer = ""
    proc = await cq.message.reply_text("⏳ Verifying OTP...")

    try:
        uc = state.data["client"]
        await uc.sign_in(state.data["phone"], state.data["phone_code_hash"], otp)
        await _finalize_login(uid, uc, mode, proc, state)
    except SessionPasswordNeeded:
        if mode == "default_otp":
            state.step = "default_2fa"
        elif mode == "custom_otp":
            state.step = "custom_2fa"
        else:
            state.step = "music_2fa"
        await proc.edit_text("🔐 **2FA Enabled**\n\nSend your 2FA password as text:")
    except PhoneCodeInvalid:
        await proc.edit_text("❌ Invalid OTP! Restart with /start")
        state.step = None
    except Exception as e:
        await proc.edit_text(f"❌ {e}")
        state.step = None

async def _finalize_login(uid, uc: Client, mode: str, proc_msg, state):
    global default_account, default_calls
    sess_str = await uc.export_session_string()

    if mode in ("default_otp", "default_2fa"):
        default_account = uc
        default_calls = PyTgCalls(uc)
        await default_calls.start()
        mongo_save_session(uid, state.data.get("phone", ""), sess_str, "default")
        await proc_msg.edit_text("✅ Default account configured & saved in MongoDB!")
    elif mode in ("custom_otp", "custom_2fa"):
        user_accounts[uid] = uc
        user_calls[uid] = PyTgCalls(uc)
        await user_calls[uid].start()
        mongo_save_session(uid, state.data.get("phone", ""), sess_str, "vc")
        state.step = "custom_group"
        await proc_msg.edit_text(
            "✅ Logged in & saved!\n\n📎 Send group info (`@username` or invite link):"
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
            "Add your userbot to any group and use `.play <song>` etc.\n"
            "Send `/commands` to view all 20+ commands."
        )
    state.step = None

# ═══════════════════════════════════════════════════════════════════════════════
# 💬 TEXT MESSAGE HANDLER — phone, 2FA password, group, chat_id, YT url
# ═══════════════════════════════════════════════════════════════════════════════
@bot.on_message(filters.private & filters.text & ~filters.command(["start","sudo","rmsudo","sudolist","stop","commands","logout"]))
async def msg_handler(client, m: Message):
    global default_account, default_calls
    uid = m.from_user.id
    if not is_authorized(uid):
        return
    state = get_user_state(uid)
    text = m.text
    if not state.step:
        return

    try:
        # ─── PHONE INPUTS ───
        if state.step in ("default_phone", "custom_phone", "music_phone"):
            phone = text.strip().replace(" ", "")
            state.data["phone"] = phone
            proc = await m.reply_text("⏳ Sending OTP...")
            try:
                name = {
                    "default_phone": "default_session_tmp",
                    "custom_phone": f"user_{uid}_tmp",
                    "music_phone": f"music_{uid}_tmp"
                }[state.step]
                uc = Client(name, api_id=API_ID, api_hash=API_HASH, in_memory=True)
                await uc.connect()
                sent = await uc.send_code(phone)
                state.data["phone_code_hash"] = sent.phone_code_hash
                state.data["client"] = uc
                next_mode = {
                    "default_phone": "default_otp",
                    "custom_phone": "custom_otp",
                    "music_phone": "music_otp"
                }[state.step]
                state.step = next_mode
                state.otp_buffer = ""
                await proc.delete()
                await m.reply_text(
                    "📨 **OTP Sent!**\n\nUse the keypad below to enter the OTP code:\n_(Tap digits, 《 to delete, ✅ to submit)_",
                    reply_markup=build_otp_keypad("", next_mode)
                )
            except FloodWait as e:
                await proc.edit_text(f"⏳ Flood wait: {e.value}s")
                state.step = None
            except Exception as e:
                await proc.edit_text(f"❌ {e}")
                state.step = None
            return

        # ─── 2FA PASSWORDS ───
        if state.step in ("default_2fa", "custom_2fa", "music_2fa"):
            proc = await m.reply_text("⏳ Verifying 2FA...")
            try:
                uc = state.data["client"]
                await uc.check_password(text.strip())
                await _finalize_login(uid, uc, state.step, proc, state)
            except PasswordHashInvalid:
                await proc.edit_text("❌ Wrong password! /start again")
                state.step = None
            except Exception as e:
                await proc.edit_text(f"❌ {e}")
                state.step = None
            return

        # ─── Chat ID input (private groups) ───
        if state.step == "waiting_chat_id":
            try:
                cid = int(text.strip())
                state.data["actual_chat_id"] = cid
                state.step = "audio_input"
                await m.reply_text(
                    f"✅ Chat ID set: `{cid}`\n\n🎵 **Now send:**\n• Audio / Voice file\n• YouTube URL\n• Song name"
                )
            except ValueError:
                await m.reply_text("❌ Invalid! Send like: `-100123456789`")
            return

        # ─── GROUP INPUT (VC Fight) ───
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
                await m.reply_text("❌ Session expired, /start again")
                state.step = None
                return

            if ci["type"] == "username":
                # PUBLIC group — no need to ask chat id
                proc = await m.reply_text("⏳ Resolving public group...")
                ok, cid, title, err, need_id = await get_chat_id_smart(client_to_use, ci, stream_key)
                if ok:
                    state.data["actual_chat_id"] = cid
                    state.data["chat_title"] = title
                    state.step = "audio_input"
                    await proc.edit_text(
                        f"✅ **Group:** {title}\n\n🎵 Send Audio / YouTube URL / Song name"
                    )
                else:
                    await proc.edit_text(err)
                    state.step = None
            else:
                # PRIVATE group — ALWAYS ask chat id per user request
                proc = await m.reply_text("⏳ Trying to join private group...")
                # join attempt only
                try:
                    await client_to_use.join_chat(ci["value"])
                except UserAlreadyParticipant:
                    pass
                except InviteHashExpired:
                    await proc.edit_text("❌ Invite link expired!")
                    state.step = None
                    return
                except Exception:
                    pass
                state.step = "waiting_chat_id"
                await proc.edit_text(
                    "🔒 **Private Group**\n\n"
                    "For accuracy, please send the Chat ID now (e.g. `-100123456789`)\n\n"
                    "👉 Forward any group msg to @username_to_id_bot to get it."
                )
            return

        # ─── AUDIO INPUT (YT URL / search) ───
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
                await m.reply_text("❌ Session expired, /start")
                state.step = None
                return
            proc = await m.reply_text("⏳ Processing...")
            actual_id = state.data.get("actual_chat_id")
            title = state.data.get("chat_title", "Group")
            if not actual_id:
                ok, actual_id, title, err, _ = await get_chat_id_smart(client_to_use, ci, stream_key)
                if not ok:
                    await proc.edit_text(err)
                    state.step = None
                    return

            await proc.edit_text("⏳ Downloading & boosting audio... 🔊")
            audio_path, song_title = await download_youtube_audio(text)
            if not audio_path:
                await proc.edit_text("❌ Couldn't fetch audio")
                state.step = None
                return

            try:
                await proc.edit_text("⏳ Joining voice chat...")
                await calls_to_use.join_group_call(
                    actual_id,
                    AudioPiped(audio_path, HighQualityAudio()),
                    stream_type=StreamType().pulse_stream
                )
                active_streams[stream_key] = actual_id
                asyncio.create_task(auto_leave_after_playback(calls_to_use, stream_key, actual_id, audio_path))
                await proc.edit_text(
                    f"✅ **NOW PLAYING** 🔥\n\n"
                    f"📻 Group: {title}\n"
                    f"🎵 {song_title or 'Audio'}\n"
                    f"🔊 BOOST: {VOLUME_BOOST}x — speaker phaadne wala 😈\n\n"
                    f"Use /stop to stop"
                )
                state.step = None
            except Exception as e:
                emsg = str(e)
                if any(x in emsg for x in ["No active group call","GROUP_CALL_INVALID","not found","GROUPCALL_FORBIDDEN"]):
                    await proc.edit_text("⏳ Rejoin strategy...")
                    ok, rerr = await rejoin_and_play(client_to_use, calls_to_use, actual_id, audio_path, stream_key)
                    if ok:
                        asyncio.create_task(auto_leave_after_playback(calls_to_use, stream_key, actual_id, audio_path))
                        await proc.edit_text(f"✅ Playing (rejoin)!\n{title}")
                    else:
                        await proc.edit_text(f"❌ Rejoin failed: {rerr}")
                    state.step = None
                else:
                    await proc.edit_text(f"❌ {emsg}")
                    state.step = None
            finally:
                asyncio.create_task(cleanup_file(audio_path))
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
        await m.reply_text("❌ Session expired")
        state.step = None
        return
    proc = await m.reply_text("⏳ Downloading audio...")
    raw = await m.download(file_name=f"/tmp/downloads/{m.id}.mp3")
    boosted = await boost_audio(raw)
    actual = state.data.get("actual_chat_id")
    title = state.data.get("chat_title", "Group")
    try:
        await proc.edit_text("⏳ Joining VC with 🔊 BOOST...")
        await calls.join_group_call(
            actual,
            AudioPiped(boosted, HighQualityAudio()),
            stream_type=StreamType().pulse_stream
        )
        active_streams[sk] = actual
        asyncio.create_task(auto_leave_after_playback(calls, sk, actual, boosted))
        await proc.edit_text(
            f"✅ **NOW PLAYING** 🔥\n📻 {title}\n🔊 BOOST {VOLUME_BOOST}x"
        )
        state.step = None
    except Exception as e:
        await proc.edit_text(f"❌ {e}")
        state.step = None
    finally:
        asyncio.create_task(cleanup_file(boosted))
        asyncio.create_task(cleanup_file(raw))

# ═══════════════════════════════════════════════════════════════════════════════
# 🚀 STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
async def on_startup():
    await restore_sessions()
    logger.info("✅ All sessions restored from MongoDB")

if __name__ == "__main__":
    logger.info("🚀 Starting ZUDO USERBOT...")
    logger.info(f"Owner: {OWNER_ID} | Mongo: {MONGO_OK}")
    logger.info(f"🔊 Volume boost: {VOLUME_BOOST}x")
    logger.info("Powered by @zudo_userbot")

    # Schedule restore on startup
    async def _main():
        await bot.start()
        await on_startup()
        logger.info("✅ Bot is running...")
        # idle
        from pyrogram import idle
        await idle()
        await bot.stop()

    try:
        asyncio.get_event_loop().run_until_complete(_main())
    except KeyboardInterrupt:
        logger.info("Stopped")
    except Exception as e:
        logger.error(f"Crash: {e}")
