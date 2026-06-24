import os
import asyncio
import re
import aiohttp
import logging
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

from motor.motor_asyncio import AsyncIOMotorClient

# ============================================================
# 🔧 CONFIGURATION
# ============================================================
OWNER_ID = 7661825494
BOT_TOKEN = "7845373810:AAH5buiFgaqEluB2nqv6zB2J371QNMtGago"
API_ID = 33628258
API_HASH = "0850762925b9c1715b9b122f7b753128"

# ✅ MongoDB
MONGO_URL = "mongodb+srv://moderatorhelperorg_db_user:nze86usap2dYthZN@cluster0.uokrixs.mongodb.net/mydatabase?retryWrites=true&w=majority"

# ✅ YT-DLP API
YTDLP_API = "http://api.nubcoder.com/info?token=4HBcMS072p&q={url}"

# 🔊 LOUD BOOST (FFmpeg volume multiplier)
AUDIO_BOOST = 4.0  # 4x louder (super loud)

# ============================================================
# 📁 DIRECTORIES
# ============================================================
Path("/tmp/downloads").mkdir(exist_ok=True, parents=True)
Path("/app/sessions").mkdir(exist_ok=True, parents=True)
Path("/app/data").mkdir(exist_ok=True, parents=True)
Path("/app/cookies").mkdir(exist_ok=True, parents=True)

# ============================================================
# 📝 LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("main")

# ============================================================
# 🤖 BOT INITIALIZATION
# ============================================================
bot = Client(
    "vc_music_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir="/app/sessions"
)

# ============================================================
# 🗄️ MONGODB SETUP
# ============================================================
mongo_client = AsyncIOMotorClient(MONGO_URL)
db = mongo_client["vcmusic_bot"]
sessions_col = db["sessions"]
sudo_col = db["sudo_users"]
music_sudo_col = db["music_sudo"]
chat_cache_col = db["chat_cache"]
default_account_col = db["default_account"]
music_hosts_col = db["music_hosts"]

# ============================================================
# 🧠 STATE STORAGE
# ============================================================
user_states = {}
default_account = None
user_accounts = {}
default_calls = None
user_calls = {}
music_userbots = {}
music_calls = {}
active_streams = {}
otp_buffer = {}
COOKIES_FILE = "/app/cookies/youtube_cookies.txt"

# ============================================================
# 🗄️ MONGO HELPERS
# ============================================================
async def save_session_to_db(user_id, session_string, account_type="custom"):
    await sessions_col.update_one(
        {"user_id": user_id, "type": account_type},
        {"$set": {
            "user_id": user_id,
            "type": account_type,
            "session_string": session_string,
            "updated_at": datetime.utcnow()
        }},
        upsert=True
    )


async def get_session_from_db(user_id, account_type="custom"):
    doc = await sessions_col.find_one({"user_id": user_id, "type": account_type})
    return doc["session_string"] if doc else None


async def delete_session_from_db(user_id, account_type="custom"):
    await sessions_col.delete_one({"user_id": user_id, "type": account_type})


async def save_default_account_db(session_string):
    await default_account_col.update_one(
        {"_id": "default"},
        {"$set": {"_id": "default", "session_string": session_string, "updated_at": datetime.utcnow()}},
        upsert=True
    )


async def get_default_account_db():
    doc = await default_account_col.find_one({"_id": "default"})
    return doc["session_string"] if doc else None


async def add_sudo_db(user_id, collection):
    await collection.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id}},
        upsert=True
    )


async def remove_sudo_db(user_id, collection):
    await collection.delete_one({"user_id": user_id})


async def get_sudo_list_db(collection):
    docs = collection.find({})
    return [doc["user_id"] async for doc in docs]


async def is_sudo_db(user_id, collection):
    doc = await collection.find_one({"user_id": user_id})
    return doc is not None


async def cache_chat_id(key, chat_id, chat_title):
    await chat_cache_col.update_one(
        {"_id": key},
        {"$set": {"_id": key, "chat_id": chat_id, "chat_title": chat_title}},
        upsert=True
    )


async def get_cached_chat(key):
    doc = await chat_cache_col.find_one({"_id": key})
    return (doc["chat_id"], doc["chat_title"]) if doc else None


async def save_music_host(user_id, session_string):
    await music_hosts_col.update_one(
        {"user_id": user_id},
        {"$set": {
            "user_id": user_id,
            "session_string": session_string,
            "updated_at": datetime.utcnow()
        }},
        upsert=True
    )


async def get_music_host(user_id):
    doc = await music_hosts_col.find_one({"user_id": user_id})
    return doc["session_string"] if doc else None


async def delete_music_host(user_id):
    await music_hosts_col.delete_one({"user_id": user_id})


# ============================================================
# 👤 STATE CLASS
# ============================================================
class UserState:
    def __init__(self):
        self.step = None
        self.data = {}


def get_user_state(user_id):
    if user_id not in user_states:
        user_states[user_id] = UserState()
    return user_states[user_id]


async def is_authorized(user_id):
    if user_id == OWNER_ID:
        return True
    if await is_sudo_db(user_id, sudo_col):
        return True
    return False


async def is_music_authorized(user_id):
    if user_id == OWNER_ID:
        return True
    if await is_sudo_db(user_id, music_sudo_col):
        return True
    return False


# ============================================================
# 🔍 CHAT EXTRACT
# ============================================================
def extract_chat_info(text):
    text = text.strip()
    invite_patterns = [
        r'(https?://)?t\.me/\+([a-zA-Z0-9_-]+)',
        r'(https?://)?t\.me/joinchat/([a-zA-Z0-9_-]+)',
    ]
    for pattern in invite_patterns:
        match = re.search(pattern, text)
        if match:
            return {"type": "invite", "value": text, "hash": match.group(2)}

    username_patterns = [
        r'(https?://)?t\.me/([a-zA-Z0-9_]+)',
        r'(https?://)?telegram\.me/([a-zA-Z0-9_]+)',
        r'@([a-zA-Z0-9_]+)',
    ]
    for pattern in username_patterns:
        match = re.search(pattern, text)
        if match:
            username = match.group(2) if ('t.me' in pattern or 'telegram' in pattern) else match.group(1)
            return {"type": "username", "value": username}

    # Direct chat ID
    if text.lstrip('-').isdigit():
        return {"type": "chatid", "value": int(text)}

    if text and not text.startswith('http'):
        return {"type": "username", "value": text.replace('@', '')}
    return None


async def find_chat_in_dialogs(client, invite_hash=None, username=None):
    try:
        async for dialog in client.get_dialogs():
            chat = dialog.chat
            try:
                if invite_hash:
                    try:
                        full_chat = await client.get_chat(chat.id)
                        if hasattr(full_chat, 'invite_link') and full_chat.invite_link:
                            if invite_hash in full_chat.invite_link:
                                return chat.id, chat.title
                    except Exception:
                        pass
                if username and hasattr(chat, 'username') and chat.username:
                    if chat.username.lower() == username.lower():
                        return chat.id, chat.title
            except Exception:
                continue
        return None, None
    except Exception as e:
        logger.error(f"find_chat_in_dialogs error: {e}")
        return None, None


async def get_chat_id_smart(client, chat_info, user_key, ask_chat_id_if_private=True):
    """
    Returns: (success, chat_id, chat_title, error_msg, needs_chat_id, is_public)
    """
    try:
        if chat_info["type"] == "chatid":
            cid = chat_info["value"]
            try:
                chat = await client.get_chat(cid)
                return True, chat.id, chat.title, None, False, False
            except Exception as e:
                return False, None, None, f"❌ Cannot access chat ID {cid}: {e}", False, False

        if chat_info["type"] == "username":
            username = chat_info["value"]
            cache_key = f"{user_key}_user_{username}"

            cached = await get_cached_chat(cache_key)
            if cached:
                return True, cached[0], cached[1], None, False, True

            try:
                chat = await client.get_chat(username)
                await cache_chat_id(cache_key, chat.id, chat.title)
                return True, chat.id, chat.title, None, False, True
            except Exception:
                pass

            try:
                await client.join_chat(username)
            except UserAlreadyParticipant:
                pass
            except Exception:
                pass

            try:
                chat = await client.get_chat(username)
                await cache_chat_id(cache_key, chat.id, chat.title)
                return True, chat.id, chat.title, None, False, True
            except Exception:
                pass

            cid, ctitle = await find_chat_in_dialogs(client, username=username)
            if cid:
                await cache_chat_id(cache_key, cid, ctitle)
                return True, cid, ctitle, None, False, True

            return False, None, None, f"❌ Cannot find group @{username}", False, True

        else:
            # private invite — ALWAYS ask for chat ID for private groups
            invite_hash = chat_info.get("hash", "")
            try:
                chat = await client.join_chat(chat_info["value"])
                if ask_chat_id_if_private:
                    return False, None, None, (
                        f"✅ Joined private group: {chat.title}\n\n"
                        f"⚠️ Private group detected!\n\n"
                        f"Please send the Chat ID (like -100123456789)\n\n"
                        f"How to get Chat ID:\n"
                        f"1. Forward any message from the group to @username_to_id_bot\n"
                        f"2. The bot will reply with the Chat ID\n"
                        f"3. Send me that Chat ID"
                    ), True, False
                return True, chat.id, chat.title, None, False, False

            except UserAlreadyParticipant:
                if ask_chat_id_if_private:
                    return False, None, None, (
                        "⚠️ Private group detected!\n\n"
                        "Please send the Chat ID (like -100123456789)\n\n"
                        "How to get Chat ID:\n"
                        "1. Forward any message from the group to @username_to_id_bot\n"
                        "2. The bot will reply with the Chat ID\n"
                        "3. Send me that Chat ID"
                    ), True, False
                cid, ctitle = await find_chat_in_dialogs(client, invite_hash=invite_hash)
                if cid:
                    return True, cid, ctitle, None, False, False
                return False, None, None, "⚠️ Please send Chat ID", True, False

            except InviteHashExpired:
                return False, None, None, "❌ Invite link expired!", False, False
            except Exception as e:
                return False, None, None, f"❌ Error: {e}", False, False

    except Exception as e:
        logger.error(f"get_chat_id_smart error: {e}")
        return False, None, None, f"❌ Unexpected error: {e}", False, False


# ============================================================
# 🎵 AUDIO DOWNLOAD via YT-DLP API
# ============================================================
async def fetch_ytdlp_api(url):
    """Get direct stream URL from yt-dlp API (fast)"""
    try:
        api_url = YTDLP_API.format(url=url)
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, timeout=30) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data
        return None
    except Exception as e:
        logger.error(f"yt-dlp API error: {e}")
        return None


async def download_audio_fast(url):
    """Download audio using yt-dlp API + FFmpeg with LOUD boost"""
    try:
        output_path = f'/tmp/downloads/{int(asyncio.get_event_loop().time() * 1000)}.mp3'

        data = await fetch_ytdlp_api(url)
        direct_url = None

        if data:
            if isinstance(data, dict):
                direct_url = (
                    data.get("url")
                    or data.get("audio_url")
                    or data.get("stream_url")
                    or data.get("link")
                )
                if not direct_url and "formats" in data:
                    formats = data["formats"]
                    if isinstance(formats, list) and formats:
                        for f in formats:
                            if "audio" in str(f.get("format", "")).lower():
                                direct_url = f.get("url")
                                break
                        if not direct_url:
                            direct_url = formats[0].get("url")

        if direct_url:
            # FFmpeg: download + boost volume
            cmd = [
                "ffmpeg", "-y",
                "-i", direct_url,
                "-vn",
                "-af", f"volume={AUDIO_BOOST}",
                "-acodec", "libmp3lame",
                "-b:a", "192k",
                output_path
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            await proc.communicate()
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1000:
                return output_path

        # Fallback: yt-dlp local
        return await download_youtube_local(url, output_path)

    except Exception as e:
        logger.error(f"download_audio_fast error: {e}")
        return None


async def download_youtube_local(url, output_path):
    """Fallback local yt-dlp download with loud boost"""
    try:
        import yt_dlp
        base_path = output_path.replace(".mp3", "")
        cookies_content = (
            "# Netscape HTTP Cookie File\n"
            ".youtube.com\tTRUE\t/\tTRUE\t0\tCONSENT\tYES+\n"
            ".youtube.com\tTRUE\t/\tFALSE\t0\tPREF\ttz=Asia.Kolkata\n"
        )
        with open(COOKIES_FILE, 'w', encoding="utf-8") as f:
            f.write(cookies_content)

        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': f'{base_path}.%(ext)s',
            'quiet': True,
            'no_warnings': True,
            'nocheckcertificate': True,
            'cookiefile': COOKIES_FILE,
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
        }
        loop = asyncio.get_event_loop()
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            await loop.run_in_executor(None, lambda: ydl.extract_info(url, download=True))

        raw_file = f"{base_path}.mp3"
        if not os.path.exists(raw_file):
            return None

        # Boost volume
        boosted = f"{base_path}_boost.mp3"
        cmd = [
            "ffmpeg", "-y",
            "-i", raw_file,
            "-af", f"volume={AUDIO_BOOST}",
            "-acodec", "libmp3lame",
            "-b:a", "192k",
            boosted
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        await proc.communicate()

        if os.path.exists(boosted):
            try:
                os.remove(raw_file)
            except Exception:
                pass
            return boosted
        return raw_file
    except Exception as e:
        logger.error(f"yt-dlp local error: {e}")
        return None


async def boost_audio_file(input_path):
    """Boost any local audio file"""
    try:
        boosted = input_path.replace(".mp3", "_boost.mp3").replace(".ogg", "_boost.mp3").replace(".m4a", "_boost.mp3")
        if boosted == input_path:
            boosted = input_path + "_boost.mp3"
        cmd = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-af", f"volume={AUDIO_BOOST}",
            "-acodec", "libmp3lame",
            "-b:a", "192k",
            boosted
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        await proc.communicate()
        if os.path.exists(boosted) and os.path.getsize(boosted) > 1000:
            return boosted
        return input_path
    except Exception as e:
        logger.error(f"boost error: {e}")
        return input_path


async def cleanup_file(path):
    try:
        await asyncio.sleep(600)  # cleanup after 10 minutes
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


# ============================================================
# 🔁 REJOIN STRATEGY
# ============================================================
async def rejoin_and_play(client, calls, chat_id, audio_path, stream_key):
    try:
        try:
            await calls.leave_group_call(chat_id)
            await asyncio.sleep(2)
        except Exception:
            pass
        try:
            await client.leave_chat(chat_id)
            await asyncio.sleep(3)
        except Exception as e:
            return False, f"Cannot leave group: {e}"
        try:
            chat = await client.get_chat(chat_id)
            if hasattr(chat, 'username') and chat.username:
                await client.join_chat(chat.username)
            else:
                return False, "Cannot rejoin private group automatically."
            await asyncio.sleep(2)
        except Exception as e:
            return False, f"Cannot rejoin group: {e}"
        try:
            await calls.join_group_call(
                chat_id,
                AudioPiped(audio_path, HighQualityAudio()),
                stream_type=StreamType().pulse_stream
            )
            active_streams[stream_key] = chat_id
            return True, None
        except Exception as e:
            return False, f"Still cannot join VC: {e}"
    except Exception as e:
        return False, f"Rejoin failed: {e}"


# ============================================================
# ⏱️ AUTO LEAVE
# ============================================================
async def get_audio_duration(file_path):
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
        if proc.returncode != 0:
            return 0
        s = out.decode().strip()
        return max(0, int(float(s))) if s else 0
    except Exception:
        return 0


async def auto_leave_after_playback(calls_to_use, stream_key, chat_id, audio_path):
    try:
        dur = await get_audio_duration(audio_path)
        if dur <= 0:
            return
        await asyncio.sleep(dur + 3)
        if active_streams.get(stream_key) == chat_id:
            try:
                await calls_to_use.leave_group_call(chat_id)
            except Exception:
                pass
            active_streams.pop(stream_key, None)
    except Exception as e:
        logger.error(f"auto_leave error: {e}")


# ============================================================
# 🔢 OTP KEYPAD BUILDER
# ============================================================
def build_otp_keypad(buf, prefix):
    display = buf if buf else "—"
    rows = [
        [
            InlineKeyboardButton("1", callback_data=f"{prefix}otp1"),
            InlineKeyboardButton("2", callback_data=f"{prefix}otp2"),
            InlineKeyboardButton("3", callback_data=f"{prefix}otp3"),
        ],
        [
            InlineKeyboardButton("4", callback_data=f"{prefix}otp4"),
            InlineKeyboardButton("5", callback_data=f"{prefix}otp5"),
            InlineKeyboardButton("6", callback_data=f"{prefix}otp6"),
        ],
        [
            InlineKeyboardButton("7", callback_data=f"{prefix}otp7"),
            InlineKeyboardButton("8", callback_data=f"{prefix}otp8"),
            InlineKeyboardButton("9", callback_data=f"{prefix}otp9"),
        ],
        [
            InlineKeyboardButton("⬅️", callback_data=f"{prefix}otpback"),
            InlineKeyboardButton("0", callback_data=f"{prefix}otp0"),
            InlineKeyboardButton("🧹", callback_data=f"{prefix}otpclear"),
        ],
        [
            InlineKeyboardButton("❌ Cancel", callback_data=f"{prefix}otpcancel"),
            InlineKeyboardButton("✅ Submit", callback_data=f"{prefix}otpsubmit"),
        ],
    ]
    return display, InlineKeyboardMarkup(rows)


# ============================================================
# 🔁 RESTORE SESSIONS
# ============================================================
async def restore_default_account():
    global default_account, default_calls
    session_str = await get_default_account_db()
    if not session_str:
        return
    try:
        default_account = Client(
            "default_account",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_str,
            in_memory=True
        )
        await default_account.start()
        default_calls = PyTgCalls(default_account)
        await default_calls.start()
        logger.info("✅ Default account restored")
    except Exception as e:
        logger.error(f"restore_default_account error: {e}")
        default_account = None
        default_calls = None


async def restore_user_account(user_id):
    session_str = await get_session_from_db(user_id, "custom")
    if not session_str:
        return
    try:
        c = Client(
            f"user_{user_id}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_str,
            in_memory=True
        )
        await c.start()
        user_accounts[user_id] = c
        user_calls[user_id] = PyTgCalls(c)
        await user_calls[user_id].start()
        logger.info(f"✅ User {user_id} restored")
    except Exception as e:
        logger.error(f"restore_user_account {user_id} error: {e}")


async def restore_music_host(user_id):
    session_str = await get_music_host(user_id)
    if not session_str:
        return
    try:
        c = Client(
            f"music_{user_id}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_str,
            in_memory=True
        )
        await c.start()
        music_userbots[user_id] = c
        music_calls[user_id] = PyTgCalls(c)
        await music_calls[user_id].start()
        await register_music_handlers(c, user_id)
        logger.info(f"✅ Music host {user_id} restored")
    except Exception as e:
        logger.error(f"restore_music_host {user_id} error: {e}")


# ============================================================
# 🏠 START COMMAND
# ============================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message: Message):
    user_id = message.from_user.id
    if not (await is_authorized(user_id) or await is_music_authorized(user_id)):
        await message.reply_text("❌ You are not authorized to use this bot!")
        return

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⚔️ VC Fight", callback_data="menu_vcfight"),
            InlineKeyboardButton("🎵 Music Userbot", callback_data="menu_music"),
        ]
    ])
    await message.reply_text(
        "✨ Welcome to VC + Music Mega Bot! ✨\n\n"
        "🎯 Choose what you want to do:\n\n"
        "⚔️ VC Fight — VC fighting / loud audio play\n"
        "🎵 Music Userbot — Play music using userbot in groups\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "💪 Powerful · Fast · Loud\n"
        "🔥 Powered by @zudo_userbot",
        reply_markup=keyboard
    )


# ============================================================
# 📋 MENU CALLBACKS
# ============================================================
@bot.on_callback_query(filters.regex("^menu_"))
async def menu_callback(client, cq: CallbackQuery):
    user_id = cq.from_user.id
    data = cq.data

    if data == "menu_vcfight":
        if not await is_authorized(user_id):
            await cq.answer("❌ Not authorized for VC Fight!", show_alert=True)
            return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ Use Default Account", callback_data="vc_default")] if user_id == OWNER_ID else [],
            [InlineKeyboardButton("🔐 Login with Your Account", callback_data="vc_custom_login")],
            [InlineKeyboardButton("▶️ Play (Custom Account)", callback_data="vc_custom_play")],
            [InlineKeyboardButton("🚪 Logout", callback_data="vc_logout")],
            [InlineKeyboardButton("« Back", callback_data="menu_back")],
        ])
        # filter out empty rows
        kb = InlineKeyboardMarkup([row for row in kb.inline_keyboard if row])
        await cq.message.edit_text(
            "⚔️ VC Fight Menu\n\n"
            "Choose an option below 👇",
            reply_markup=kb
        )

    elif data == "menu_music":
        if not await is_music_authorized(user_id):
            await cq.answer("❌ Not authorized for Music!", show_alert=True)
            return
        if user_id in music_userbots:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Status", callback_data="music_status")],
                [InlineKeyboardButton("📜 Commands List", callback_data="music_commands")],
                [InlineKeyboardButton("🚪 Logout Music", callback_data="music_logout")],
                [InlineKeyboardButton("« Back", callback_data="menu_back")],
            ])
            await cq.message.edit_text(
                "🎵 Music Userbot Menu\n\n"
                "✅ You have hosted a music userbot.\n"
                "Use `.play <song>` to play music.",
                reply_markup=kb
            )
        else:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Host Music Userbot", callback_data="music_host")],
                [InlineKeyboardButton("📜 Commands List", callback_data="music_commands")],
                [InlineKeyboardButton("« Back", callback_data="menu_back")],
            ])
            await cq.message.edit_text(
                "🎵 Music Userbot Menu\n\n"
                "⚠️ You haven't hosted your music userbot yet.\n\n"
                "🚀 Click Host Music Userbot to login your account as music userbot.\n\n"
                "Once hosted, you can use commands like:\n"
                "`.play <song>` `.skip` `.pause` etc. in any group.",
                reply_markup=kb
            )

    elif data == "menu_back":
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⚔️ VC Fight", callback_data="menu_vcfight"),
                InlineKeyboardButton("🎵 Music Userbot", callback_data="menu_music"),
            ]
        ])
        await cq.message.edit_text(
            "✨ Welcome to VC + Music Mega Bot! ✨\n\n"
            "🎯 Choose what you want to do:\n\n"
            "⚔️ VC Fight — VC fighting / loud audio play\n"
            "🎵 Music Userbot — Play music using userbot in groups\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "💪 Powerful · Fast · Loud\n"
            "🔥 Powered by @zudo_userbot",
            reply_markup=keyboard
        )

    await cq.answer()


# ============================================================
# ⚔️ VC FIGHT CALLBACKS
# ============================================================
@bot.on_callback_query(filters.regex("^vc_"))
async def vc_callback(client, cq: CallbackQuery):
    global default_account, default_calls
    user_id = cq.from_user.id
    data = cq.data
    state = get_user_state(user_id)

    if not await is_authorized(user_id):
        await cq.answer("❌ Not authorized!", show_alert=True)
        return

    if data == "vc_default":
        if default_account is None:
            await restore_default_account()
        if default_account is None:
            await cq.answer("❌ Default account not configured!", show_alert=True)
            if user_id == OWNER_ID:
                await cq.message.reply_text("⚠️ Use /setdefault to configure default account first.")
            return

        state.step = "default_group"
        state.data = {"mode": "default"}
        await cq.message.reply_text(
            "📎 Send Group Info\n\n"
            "For Public Groups:\n"
            "Send username: @groupusername\n\n"
            "For Private Groups:\n"
            "Send invite link: https://t.me/+xxxxx\n\n"
            "ℹ️ For private groups, you'll be asked for Chat ID."
        )

    elif data == "vc_custom_login":
        state.step = "custom_phone"
        state.data = {"mode": "custom"}
        await cq.message.reply_text(
            "📱 Login to Your Account\n\n"
            "Send your phone number with country code:\n"
            "Example: +919876543210"
        )

    elif data == "vc_custom_play":
        if user_id not in user_accounts:
            await restore_user_account(user_id)
        if user_id not in user_accounts:
            await cq.answer("❌ Please login first!", show_alert=True)
            return
        state.step = "custom_group"
        state.data = {"mode": "custom"}
        await cq.message.reply_text(
            "📎 Send Group Info\n\n"
            "For Public Groups:\n"
            "Send username: @groupusername\n\n"
            "For Private Groups:\n"
            "Send invite link: https://t.me/+xxxxx\n\n"
            "ℹ️ For private groups, you'll be asked for Chat ID."
        )

    elif data == "vc_logout":
        await force_logout_user(user_id)
        await cq.message.reply_text("✅ Logged out successfully! Use /start again.")

    await cq.answer()


# ============================================================
# 🎵 MUSIC CALLBACKS
# ============================================================
@bot.on_callback_query(filters.regex("^music_"))
async def music_callback(client, cq: CallbackQuery):
    user_id = cq.from_user.id
    data = cq.data
    state = get_user_state(user_id)

    if not await is_music_authorized(user_id):
        await cq.answer("❌ Not authorized!", show_alert=True)
        return

    if data == "music_host":
        state.step = "music_phone"
        state.data = {"mode": "music"}
        await cq.message.reply_text(
            "🚀 Host Music Userbot\n\n"
            "Send your phone number with country code:\n"
            "Example: +919876543210"
        )

    elif data == "music_status":
        if user_id in music_userbots:
            me = await music_userbots[user_id].get_me()
            await cq.answer(f"✅ Hosted as {me.first_name}", show_alert=True)
        else:
            await cq.answer("❌ Not hosted", show_alert=True)

    elif data == "music_commands":
        await cq.message.reply_text(get_music_commands_text())

    elif data == "music_logout":
        await delete_music_host(user_id)
        if user_id in music_userbots:
            try:
                await music_userbots[user_id].stop()
            except Exception:
                pass
            del music_userbots[user_id]
        if user_id in music_calls:
            try:
                await music_calls[user_id].stop()
            except Exception:
                pass
            del music_calls[user_id]
        await cq.message.reply_text("✅ Music userbot logged out!")

    await cq.answer()


# ============================================================
# 🔢 OTP KEYPAD CALLBACKS
# ============================================================
@bot.on_callback_query(filters.regex("^(vclogin|musichost|setdef)otp"))
async def otp_callback(client, cq: CallbackQuery):
    user_id = cq.from_user.id
    state = get_user_state(user_id)
    data = cq.data

    # data format: <prefix>otp<action>
    m = re.match(r"^(vclogin|musichost|setdef)otp(.+)$", data)
    if not m:
        await cq.answer()
        return
    prefix = m.group(1)
    action = m.group(2)

    buf = otp_buffer.get(user_id, "")

    if action == "back":
        buf = buf[:-1]
    elif action == "clear":
        buf = ""
    elif action == "cancel":
        otp_buffer.pop(user_id, None)
        state.step = None
        await cq.message.edit_text("❌ OTP entry cancelled.")
        await cq.answer()
        return
    elif action == "submit":
        if not buf:
            await cq.answer("❌ Enter OTP first!", show_alert=True)
            return
        otp = buf
        otp_buffer.pop(user_id, None)
        # Process OTP
        await process_otp(client, cq, prefix, otp)
        return
    elif action.isdigit():
        if len(buf) < 5:
            buf += action

    otp_buffer[user_id] = buf
    display, kb = build_otp_keypad(buf, prefix)
    try:
        await cq.message.edit_text(
            f"📨 OTP Sent!\n\n"
            f"OTP: {display}\n\n"
            f"Use the keypad below to enter OTP 👇",
            reply_markup=kb
        )
    except Exception:
        pass
    await cq.answer()


async def process_otp(client, cq, prefix, otp):
    global default_account, default_calls
    user_id = cq.from_user.id
    state = get_user_state(user_id)
    processing_msg = await cq.message.edit_text("⏳ Verifying OTP...")

    try:
        user_client = state.data["client"]
        phone = state.data["phone"]
        phone_code_hash = state.data["phone_code_hash"]
        await user_client.sign_in(phone, phone_code_hash, otp)
        session_str = await user_client.export_session_string()

        if prefix == "setdef":
            await save_default_account_db(session_str)
            default_account = user_client
            default_calls = PyTgCalls(default_account)
            await default_calls.start()
            await processing_msg.edit_text("✅ Default account configured!")
            state.step = None
        elif prefix == "vclogin":
            await save_session_to_db(user_id, session_str, "custom")
            user_accounts[user_id] = user_client
            user_calls[user_id] = PyTgCalls(user_client)
            await user_calls[user_id].start()
            state.step = "custom_group"
            await processing_msg.edit_text(
                "✅ Logged in!\n\n"
                "📎 Send Group Info:\n"
                "Public: @groupusername\n"
                "Private: https://t.me/+xxxxx"
            )
        elif prefix == "musichost":
            await save_music_host(user_id, session_str)
            music_userbots[user_id] = user_client
            music_calls[user_id] = PyTgCalls(user_client)
            await music_calls[user_id].start()
            await register_music_handlers(user_client, user_id)
            await processing_msg.edit_text(
                "✅ Music userbot hosted!\n\n"
                "🎵 Use `.play <song>` `.skip` `.pause` etc.\n\n"
                "Type /commands to see all music commands."
            )
            state.step = None

    except SessionPasswordNeeded:
        state.step = f"{prefix}_2fa"
        await processing_msg.edit_text(
            "🔐 2FA Enabled\n\n"
            "Please send your 2FA password as a text message:"
        )
    except PhoneCodeInvalid:
        await processing_msg.edit_text("❌ Invalid OTP! Please /start again.")
        state.step = None
    except Exception as e:
        await processing_msg.edit_text(f"❌ Error: {e}")
        state.step = None


# ============================================================
# 🔧 ADMIN COMMANDS
# ============================================================
@bot.on_message(filters.command("setdefault") & filters.private & filters.user(OWNER_ID))
async def set_default(client, message: Message):
    state = get_user_state(message.from_user.id)
    state.step = "setdef_phone"
    state.data = {}
    await message.reply_text("📱 Setup Default Account\n\nSend phone number with country code:")


@bot.on_message(filters.command("sudo") & filters.private & filters.user(OWNER_ID))
async def add_sudo_cmd(client, message: Message):
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/sudo <user_id or @username>`")
            return
        user_input = message.command[1]
        if user_input.startswith('@'):
            user = await client.get_users(user_input[1:])
        else:
            user = await client.get_users(int(user_input))
        await add_sudo_db(user.id, sudo_col)
        await message.reply_text(f"✅ {user.first_name} ({user.id}) added to VC Fight sudo!")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@bot.on_message(filters.command("rmsudo") & filters.private & filters.user(OWNER_ID))
async def rm_sudo_cmd(client, message: Message):
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/rmsudo <user_id or @username>`")
            return
        user_input = message.command[1]
        if user_input.startswith('@'):
            user = await client.get_users(user_input[1:])
        else:
            user = await client.get_users(int(user_input))
        await remove_sudo_db(user.id, sudo_col)
        await message.reply_text(f"✅ {user.first_name} removed from VC Fight sudo!")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@bot.on_message(filters.command("sudolist") & filters.private & filters.user(OWNER_ID))
async def sudo_list_cmd(client, message: Message):
    try:
        sudos = await get_sudo_list_db(sudo_col)
        if not sudos:
            await message.reply_text("ℹ️ No sudo users!")
            return
        text = "👥 VC Fight Sudo Users:\n\n"
        for uid in sudos:
            try:
                u = await client.get_users(uid)
                text += f"• {u.first_name} - {u.id}\n"
            except Exception:
                text += f"• Unknown - {uid}\n"
        text += f"\nTotal: {len(sudos)}"
        await message.reply_text(text)
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@bot.on_message(filters.command(["msudo", "musicsudo"]) & filters.private & filters.user(OWNER_ID))
async def add_music_sudo(client, message: Message):
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/msudo <user_id or @username>`")
            return
        user_input = message.command[1]
        if user_input.startswith('@'):
            user = await client.get_users(user_input[1:])
        else:
            user = await client.get_users(int(user_input))
        await add_sudo_db(user.id, music_sudo_col)
        await message.reply_text(f"✅ {user.first_name} added to Music sudo!")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@bot.on_message(filters.command(["rmmsudo", "rmmusicsudo"]) & filters.private & filters.user(OWNER_ID))
async def rm_music_sudo(client, message: Message):
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/rmmsudo <user_id or @username>`")
            return
        user_input = message.command[1]
        if user_input.startswith('@'):
            user = await client.get_users(user_input[1:])
        else:
            user = await client.get_users(int(user_input))
        await remove_sudo_db(user.id, music_sudo_col)
        await message.reply_text(f"✅ {user.first_name} removed from Music sudo!")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


# ============================================================
# 🚪 LOGOUT
# ============================================================
async def force_logout_user(user_id):
    try:
        if user_id in active_streams and user_id in user_calls:
            cid = active_streams.get(user_id)
            try:
                await user_calls[user_id].leave_group_call(cid)
            except Exception:
                pass
            active_streams.pop(user_id, None)
    except Exception:
        pass
    try:
        if user_id in user_calls:
            try:
                await user_calls[user_id].stop()
            except Exception:
                pass
            del user_calls[user_id]
    except Exception:
        pass
    try:
        if user_id in user_accounts:
            try:
                await user_accounts[user_id].stop()
            except Exception:
                pass
            del user_accounts[user_id]
    except Exception:
        pass
    await delete_session_from_db(user_id, "custom")


@bot.on_message(filters.command("logout") & filters.private)
async def logout_cmd(client, message: Message):
    user_id = message.from_user.id
    if not await is_authorized(user_id):
        await message.reply_text("❌ Not authorized!")
        return
    await force_logout_user(user_id)
    await message.reply_text("✅ Logged out & session deleted from DB!")


@bot.on_message(filters.command("stop") & filters.private)
async def stop_cmd(client, message: Message):
    user_id = message.from_user.id
    if not await is_authorized(user_id):
        return
    stopped = False
    if user_id in active_streams and user_id in user_calls:
        try:
            await user_calls[user_id].leave_group_call(active_streams[user_id])
            del active_streams[user_id]
            stopped = True
        except Exception:
            pass
    elif user_id == OWNER_ID and "default" in active_streams and default_calls:
        try:
            await default_calls.leave_group_call(active_streams["default"])
            del active_streams["default"]
            stopped = True
        except Exception:
            pass
    if stopped:
        await message.reply_text("✅ Stopped playing!")
    else:
        await message.reply_text("❌ No active stream!")


# ============================================================
# 💬 MAIN MESSAGE HANDLER
# ============================================================
@bot.on_message(filters.private & filters.text & ~filters.command([
    "start", "setdefault", "logout", "stop", "sudo", "rmsudo", "sudolist",
    "msudo", "rmmsudo", "rmmusicsudo", "musicsudo", "commands"
]))
async def message_handler(client, message: Message):
    global default_account, default_calls

    user_id = message.from_user.id
    state = get_user_state(user_id)
    text = message.text

    if not state.step:
        return

    try:
        # ============= PHONE STEP =============
        if state.step in ["setdef_phone", "custom_phone", "music_phone"]:
            phone = text.strip().replace(" ", "")
            state.data["phone"] = phone
            processing_msg = await message.reply_text("⏳ Sending OTP...")

            try:
                if state.step == "setdef_phone":
                    session_name = "default_session_tmp"
                    prefix = "setdef"
                elif state.step == "custom_phone":
                    session_name = f"user_{user_id}_tmp"
                    prefix = "vclogin"
                else:
                    session_name = f"music_{user_id}_tmp"
                    prefix = "musichost"

                user_client = Client(
                    session_name,
                    api_id=API_ID,
                    api_hash=API_HASH,
                    in_memory=True
                )
                await user_client.connect()
                sent_code = await user_client.send_code(phone)
                state.data["phone_code_hash"] = sent_code.phone_code_hash
                state.data["client"] = user_client
                state.data["prefix"] = prefix
                state.step = f"{prefix}_otp_keypad"
                otp_buffer[user_id] = ""

                display, kb = build_otp_keypad("", prefix)
                await processing_msg.edit_text(
                    f"📨 OTP Sent!\n\n"
                    f"OTP: {display}\n\n"
                    f"Use the keypad below to enter OTP 👇",
                    reply_markup=kb
                )
            except FloodWait as e:
                await processing_msg.edit_text(f"⏳ FloodWait: {e.value}s")
                state.step = None
            except Exception as e:
                await processing_msg.edit_text(f"❌ Error: {e}")
                state.step = None

        # ============= 2FA STEP =============
        elif state.step in ["setdef_2fa", "vclogin_2fa", "musichost_2fa"]:
            password = text.strip()
            processing_msg = await message.reply_text("⏳ Verifying 2FA...")
            try:
                user_client = state.data["client"]
                await user_client.check_password(password)
                session_str = await user_client.export_session_string()
                prefix = state.step.split("_2fa")[0]

                if prefix == "setdef":
                    await save_default_account_db(session_str)
                    default_account = user_client
                    default_calls = PyTgCalls(default_account)
                    await default_calls.start()
                    await processing_msg.edit_text("✅ Default account configured!")
                    state.step = None
                elif prefix == "vclogin":
                    await save_session_to_db(user_id, session_str, "custom")
                    user_accounts[user_id] = user_client
                    user_calls[user_id] = PyTgCalls(user_client)
                    await user_calls[user_id].start()
                    state.step = "custom_group"
                    await processing_msg.edit_text(
                        "✅ Logged in!\n\n"
                        "📎 Send Group Info:\n"
                        "Public: @groupusername\n"
                        "Private: https://t.me/+xxxxx"
                    )
                elif prefix == "musichost":
                    await save_music_host(user_id, session_str)
                    music_userbots[user_id] = user_client
                    music_calls[user_id] = PyTgCalls(user_client)
                    await music_calls[user_id].start()
                    await register_music_handlers(user_client, user_id)
                    await processing_msg.edit_text(
                        "✅ Music userbot hosted!\n\n"
                        "🎵 Use `.play <song>` in any group.\n"
                        "Type /commands for all commands."
                    )
                    state.step = None
            except PasswordHashInvalid:
                await processing_msg.edit_text("❌ Invalid 2FA password!")
                state.step = None
            except Exception as e:
                await processing_msg.edit_text(f"❌ Error: {e}")
                state.step = None

        # ============= CHAT ID STEP =============
        elif state.step == "waiting_chat_id":
            try:
                actual_chat_id = int(text.strip())
                state.data["actual_chat_id"] = actual_chat_id
                # Try to fetch title
                mode = state.data.get("mode")
                if mode == "default":
                    cli = default_account
                else:
                    cli = user_accounts.get(user_id)
                try:
                    chat = await cli.get_chat(actual_chat_id)
                    state.data["chat_title"] = chat.title
                except Exception:
                    state.data["chat_title"] = f"Chat {actual_chat_id}"
                state.step = "audio_input"
                await message.reply_text(
                    f"✅ Chat ID set: {actual_chat_id}\n\n"
                    "🎵 Now send audio:\n"
                    "• Audio file 🎵\n• Voice 🎤\n• YouTube URL 📺"
                )
            except ValueError:
                await message.reply_text("❌ Invalid Chat ID! Send like -100123456789")

        # ============= GROUP STEP =============
        elif state.step in ["default_group", "custom_group"]:
            chat_info = extract_chat_info(text)
            if not chat_info:
                await message.reply_text("❌ Invalid input!")
                return
            state.data["chat_info"] = chat_info

            mode = state.data.get("mode")
            if mode == "default":
                client_to_use = default_account
                stream_key = "default"
            else:
                client_to_use = user_accounts.get(user_id)
                stream_key = user_id

            if not client_to_use:
                await message.reply_text("❌ Session expired!")
                state.step = None
                return

            processing_msg = await message.reply_text("⏳ Checking group access...")
            # ALWAYS ask for chat ID for private groups
            success, cid, ctitle, err, needs_id, is_public = await get_chat_id_smart(
                client_to_use, chat_info, stream_key, ask_chat_id_if_private=True
            )

            if success and cid:
                state.data["actual_chat_id"] = cid
                state.data["chat_title"] = ctitle
                state.step = "audio_input"
                await processing_msg.edit_text(
                    f"✅ Group: {ctitle}\n\n"
                    "🎵 Send audio:\n• File 🎵\n• Voice 🎤\n• YouTube URL 📺"
                )
            elif needs_id:
                state.step = "waiting_chat_id"
                await processing_msg.edit_text(err)
            else:
                await processing_msg.edit_text(err)
                state.step = None

        # ============= AUDIO INPUT =============
        elif state.step == "audio_input":
            await play_url(client, message, text)

    except Exception as e:
        logger.error(f"Handler error: {e}")
        await message.reply_text(f"❌ Error: {e}")
        state.step = None


# ============================================================
# 🎵 PLAY URL / FILE
# ============================================================
async def play_url(client, message, url):
    user_id = message.from_user.id
    state = get_user_state(user_id)

    mode = state.data.get("mode")
    if mode == "default":
        client_to_use = default_account
        calls_to_use = default_calls
        stream_key = "default"
    else:
        client_to_use = user_accounts.get(user_id)
        calls_to_use = user_calls.get(user_id)
        stream_key = user_id

    if not client_to_use or not calls_to_use:
        await message.reply_text("❌ Session expired!")
        state.step = None
        return

    if not (url.startswith("http://") or url.startswith("https://")):
        await message.reply_text("❌ Send YouTube URL or audio file!")
        return

    processing_msg = await message.reply_text("⏳ Downloading audio...")
    audio_path = await download_audio_fast(url)
    if not audio_path or not os.path.exists(audio_path):
        await processing_msg.edit_text("❌ Download failed!")
        state.step = None
        return

    actual_chat_id = state.data["actual_chat_id"]
    chat_title = state.data.get("chat_title", "Group")

    try:
        await processing_msg.edit_text("⏳ Joining VC (loud mode 🔊)...")
        await calls_to_use.join_group_call(
            actual_chat_id,
            AudioPiped(audio_path, HighQualityAudio()),
            stream_type=StreamType().pulse_stream
        )
        active_streams[stream_key] = actual_chat_id
        asyncio.create_task(auto_leave_after_playback(calls_to_use, stream_key, actual_chat_id, audio_path))
        await processing_msg.edit_text(
            f"✅ Now Playing! 🔊\n\n"
            f"📻 Group: {chat_title}\n"
            f"🔥 Boost: {AUDIO_BOOST}x LOUD\n\n"
            "Use /stop to stop."
        )
        state.step = None
    except Exception as e:
        err = str(e)
        if any(x in err for x in ["No active group call", "GROUP_CALL_INVALID", "not found", "GROUPCALL_FORBIDDEN"]):
            await processing_msg.edit_text("⏳ Trying rejoin...")
            ok, rerr = await rejoin_and_play(client_to_use, calls_to_use, actual_chat_id, audio_path, stream_key)
            if ok:
                active_streams[stream_key] = actual_chat_id
                asyncio.create_task(auto_leave_after_playback(calls_to_use, stream_key, actual_chat_id, audio_path))
                await processing_msg.edit_text(f"✅ Now Playing! (rejoin)\n📻 {chat_title}")
                state.step = None
            else:
                await processing_msg.edit_text(f"❌ Rejoin failed: {rerr}")
                state.step = None
        else:
            await processing_msg.edit_text(f"❌ Error: {err}")
            state.step = None
    finally:
        asyncio.create_task(cleanup_file(audio_path))


@bot.on_message(filters.private & (filters.audio | filters.voice))
async def audio_handler(client, message: Message):
    user_id = message.from_user.id
    state = get_user_state(user_id)
    if not await is_authorized(user_id):
        return
    if state.step != "audio_input":
        return

    mode = state.data.get("mode")
    if mode == "default":
        client_to_use = default_account
        calls_to_use = default_calls
        stream_key = "default"
    else:
        client_to_use = user_accounts.get(user_id)
        calls_to_use = user_calls.get(user_id)
        stream_key = user_id

    if not client_to_use or not calls_to_use:
        await message.reply_text("❌ Session expired!")
        state.step = None
        return

    processing_msg = await message.reply_text("⏳ Downloading audio...")
    raw = await message.download(file_name=f"/tmp/downloads/{message.id}.mp3")
    audio_path = await boost_audio_file(raw)

    actual_chat_id = state.data["actual_chat_id"]
    chat_title = state.data.get("chat_title", "Group")

    try:
        await processing_msg.edit_text("⏳ Joining VC (loud 🔊)...")
        await calls_to_use.join_group_call(
            actual_chat_id,
            AudioPiped(audio_path, HighQualityAudio()),
            stream_type=StreamType().pulse_stream
        )
        active_streams[stream_key] = actual_chat_id
        asyncio.create_task(auto_leave_after_playback(calls_to_use, stream_key, actual_chat_id, audio_path))
        await processing_msg.edit_text(
            f"✅ Now Playing! 🔊\n\n"
            f"📻 Group: {chat_title}\n"
            f"🔥 Boost: {AUDIO_BOOST}x LOUD"
        )
        state.step = None
    except Exception as e:
        err = str(e)
        if any(x in err for x in ["No active group call", "GROUP_CALL_INVALID", "not found"]):
            ok, rerr = await rejoin_and_play(client_to_use, calls_to_use, actual_chat_id, audio_path, stream_key)
            if ok:
                active_streams[stream_key] = actual_chat_id
                await processing_msg.edit_text("✅ Now Playing! (rejoin)")
                state.step = None
            else:
                await processing_msg.edit_text(f"❌ {rerr}")
                state.step = None
        else:
            await processing_msg.edit_text(f"❌ {err}")
            state.step = None
    finally:
        asyncio.create_task(cleanup_file(audio_path))
        asyncio.create_task(cleanup_file(raw))


# ============================================================
# 🎵 MUSIC USERBOT HANDLERS (Dynamic per-user)
# ============================================================
music_queues = {}        # {host_id: {chat_id: [songs]}}
music_now_playing = {}   # {host_id: {chat_id: song}}
music_authorized = {}    # {host_id: set of user_ids}


def get_music_commands_text():
    return (
        "🎵 Music Userbot Commands 🎵\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎶 PLAYBACK\n\n"
        "▶️  .play <song>  —  Play a song\n"
        "⏸️  .pause  —  Pause current song\n"
        "▶️  .resume  —  Resume playback\n"
        "⏭️  .skip  —  Skip to next song\n"
        "⏹️  .stop  —  Stop & leave VC\n"
        "🔁  .replay  —  Replay current song\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "📋 QUEUE\n\n"
        "📜  .queue  —  Show queue\n"
        "🧹  .clear  —  Clear queue\n"
        "🔀  .shuffle  —  Shuffle queue\n"
        "🎯  .now  —  Show currently playing\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔊 AUDIO\n\n"
        "🔊  .volume <0-200>  —  Set volume\n"
        "🔇  .mute  —  Mute audio\n"
        "🔉  .unmute  —  Unmute audio\n"
        "🚀  .boost  —  Loud mode toggle\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "👥 PERMISSIONS\n\n"
        "✅  .auth <user>  —  Add sudo user\n"
        "❌  .dauth <user>  —  Remove sudo user\n"
        "👁️  .authlist  —  Show sudo list\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "⚙️  MISC\n\n"
        "🏓  .ping  —  Check bot ping\n"
        "🎙️  .join  —  Force join VC\n"
        "🚪  .leave  —  Leave VC\n"
        "📜  .cmds or /commands  —  Show this list\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "💡 Tip: Only the host & sudo users can use music commands.\n"
        "🔥 Powered by @zudo_userbot"
    )


@bot.on_message(filters.command("commands") & filters.private)
async def commands_cmd(client, message: Message):
    await message.reply_text(get_music_commands_text())


async def register_music_handlers(user_client, host_id):
    """Register .play, .skip etc handlers on the music userbot"""

    if host_id not in music_authorized:
        music_authorized[host_id] = set()
    if host_id not in music_queues:
        music_queues[host_id] = {}
    if host_id not in music_now_playing:
        music_now_playing[host_id] = {}

    def is_allowed(u_id):
        return u_id == host_id or u_id == OWNER_ID or u_id in music_authorized[host_id]

    @user_client.on_message(filters.regex(r"^\.play(\s+.+)?$") & filters.group)
    async def _play(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        query = m.text.split(None, 1)
        if len(query) < 2:
            await m.reply_text("❌ Usage: `.play <song name or url>`")
            return
        q = query[1].strip()
        chat_id = m.chat.id
        msg = await m.reply_text(f"🔎 Searching: {q}")

        try:
            # If URL, use directly
            if q.startswith("http"):
                url = q
                title = "YouTube Audio"
            else:
                # Search via yt-dlp
                import yt_dlp
                ydl_opts = {"quiet": True, "no_warnings": True, "default_search": "ytsearch1"}
                loop = asyncio.get_event_loop()
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = await loop.run_in_executor(None, lambda: ydl.extract_info(f"ytsearch1:{q}", download=False))
                if not info or "entries" not in info or not info["entries"]:
                    await msg.edit_text("❌ Not found!")
                    return
                entry = info["entries"][0]
                url = entry.get("webpage_url") or entry.get("url")
                title = entry.get("title", "Unknown")

            await msg.edit_text(f"⏳ Downloading: {title}")
            audio_path = await download_audio_fast(url)
            if not audio_path:
                await msg.edit_text("❌ Download failed!")
                return

            # Check if already playing in this chat
            if music_now_playing[host_id].get(chat_id):
                # Add to queue
                music_queues[host_id].setdefault(chat_id, []).append({
                    "url": url, "title": title, "path": audio_path,
                    "user": m.from_user.first_name if m.from_user else "Unknown"
                })
                pos = len(music_queues[host_id][chat_id])
                await msg.edit_text(f"➕ Added to queue (#{pos})\n🎵 {title}")
                return

            # Play now
            music_now_playing[host_id][chat_id] = {"title": title, "path": audio_path}
            calls = music_calls.get(host_id)
            await calls.join_group_call(
                chat_id,
                AudioPiped(audio_path, HighQualityAudio()),
                stream_type=StreamType().pulse_stream
            )
            active_streams[f"music_{host_id}_{chat_id}"] = chat_id
            await msg.edit_text(f"▶️ Now Playing 🔊\n🎵 {title}\n🔥 Loud mode active")

        except Exception as e:
            await msg.edit_text(f"❌ Error: {e}")

    @user_client.on_message(filters.regex(r"^\.skip$") & filters.group)
    async def _skip(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        chat_id = m.chat.id
        queue = music_queues[host_id].get(chat_id, [])
        if not queue:
            try:
                await music_calls[host_id].leave_group_call(chat_id)
            except Exception:
                pass
            music_now_playing[host_id].pop(chat_id, None)
            await m.reply_text("⏭️ Skipped! Queue empty, left VC.")
            return
        next_song = queue.pop(0)
        music_now_playing[host_id][chat_id] = next_song
        try:
            await music_calls[host_id].change_stream(
                chat_id,
                AudioPiped(next_song["path"], HighQualityAudio())
            )
            await m.reply_text(f"⏭️ Skipped!\n▶️ Now: {next_song['title']}")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @user_client.on_message(filters.regex(r"^\.pause$") & filters.group)
    async def _pause(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        try:
            await music_calls[host_id].pause_stream(m.chat.id)
            await m.reply_text("⏸️ Paused")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @user_client.on_message(filters.regex(r"^\.resume$") & filters.group)
    async def _resume(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        try:
            await music_calls[host_id].resume_stream(m.chat.id)
            await m.reply_text("▶️ Resumed")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @user_client.on_message(filters.regex(r"^\.stop$") & filters.group)
    async def _stop(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        try:
            await music_calls[host_id].leave_group_call(m.chat.id)
            music_now_playing[host_id].pop(m.chat.id, None)
            music_queues[host_id].pop(m.chat.id, None)
            await m.reply_text("⏹️ Stopped & left VC")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @user_client.on_message(filters.regex(r"^\.leave$") & filters.group)
    async def _leave(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        try:
            await music_calls[host_id].leave_group_call(m.chat.id)
            await m.reply_text("🚪 Left VC")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @user_client.on_message(filters.regex(r"^\.join$") & filters.group)
    async def _join(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        await m.reply_text("🎙️ Use `.play <song>` to join with music")

    @user_client.on_message(filters.regex(r"^\.replay$") & filters.group)
    async def _replay(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        cur = music_now_playing[host_id].get(m.chat.id)
        if not cur:
            await m.reply_text("❌ Nothing playing!")
            return
        try:
            await music_calls[host_id].change_stream(
                m.chat.id,
                AudioPiped(cur["path"], HighQualityAudio())
            )
            await m.reply_text(f"🔁 Replaying: {cur['title']}")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @user_client.on_message(filters.regex(r"^\.queue$") & filters.group)
    async def _queue(c, m: Message):
        q = music_queues[host_id].get(m.chat.id, [])
        cur = music_now_playing[host_id].get(m.chat.id)
        text = "📋 Queue:\n\n"
        if cur:
            text += f"▶️ Now: {cur['title']}\n\n"
        if not q:
            text += "Queue is empty"
        else:
            for i, song in enumerate(q, 1):
                text += f"{i}. {song['title']}\n"
        await m.reply_text(text)

    @user_client.on_message(filters.regex(r"^\.clear$") & filters.group)
    async def _clear(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        music_queues[host_id][m.chat.id] = []
        await m.reply_text("🧹 Queue cleared")

    @user_client.on_message(filters.regex(r"^\.shuffle$") & filters.group)
    async def _shuffle(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        import random
        q = music_queues[host_id].get(m.chat.id, [])
        random.shuffle(q)
        music_queues[host_id][m.chat.id] = q
        await m.reply_text("🔀 Queue shuffled")

    @user_client.on_message(filters.regex(r"^\.now$") & filters.group)
    async def _now(c, m: Message):
        cur = music_now_playing[host_id].get(m.chat.id)
        if cur:
            await m.reply_text(f"🎯 Now playing: {cur['title']}")
        else:
            await m.reply_text("❌ Nothing playing")

    @user_client.on_message(filters.regex(r"^\.volume(\s+\d+)?$") & filters.group)
    async def _volume(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        parts = m.text.split()
        if len(parts) < 2:
            await m.reply_text("❌ Usage: `.volume <0-200>`")
            return
        try:
            v = int(parts[1])
            await music_calls[host_id].change_volume_call(m.chat.id, v)
            await m.reply_text(f"🔊 Volume set to {v}")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @user_client.on_message(filters.regex(r"^\.mute$") & filters.group)
    async def _mute(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        try:
            await music_calls[host_id].mute_stream(m.chat.id)
            await m.reply_text("🔇 Muted")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @user_client.on_message(filters.regex(r"^\.unmute$") & filters.group)
    async def _unmute(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        try:
            await music_calls[host_id].unmute_stream(m.chat.id)
            await m.reply_text("🔉 Unmuted")
        except Exception as e:
            await m.reply_text(f"❌ {e}")

    @user_client.on_message(filters.regex(r"^\.boost$") & filters.group)
    async def _boost(c, m: Message):
        if not m.from_user or not is_allowed(m.from_user.id):
            return
        await m.reply_text(f"🚀 Loud Boost: {AUDIO_BOOST}x active 🔊🔊🔊")

    @user_client.on_message(filters.regex(r"^\.auth(\s+\S+)?$") & filters.group)
    async def _auth(c, m: Message):
        if not m.from_user:
            return
        if m.from_user.id != host_id and m.from_user.id != OWNER_ID:
            return
        target_id = None
        target_name = None
        if m.reply_to_message and m.reply_to_message.from_user:
            target_id = m.reply_to_message.from_user.id
            target_name = m.reply_to_message.from_user.first_name
        else:
            parts = m.text.split()
            if len(parts) < 2:
                await m.reply_text("❌ Usage: .auth (reply) or .auth @user/userid")
                return
            target = parts[1]
            try:
                if target.startswith("@"):
                    u = await c.get_users(target[1:])
                else:
                    u = await c.get_users(int(target))
                target_id = u.id
                target_name = u.first_name
            except Exception as e:
                await m.reply_text(f"❌ {e}")
                return
        music_authorized[host_id].add(target_id)
        await m.reply_text(f"✅ {target_name} ({target_id}) added as music sudo!")

    @user_client.on_message(filters.regex(r"^\.dauth(\s+\S+)?$") & filters.group)
    async def _dauth(c, m: Message):
        if not m.from_user:
            return
        if m.from_user.id != host_id and m.from_user.id != OWNER_ID:
            return
        target_id = None
        if m.reply_to_message and m.reply_to_message.from_user:
            target_id = m.reply_to_message.from_user.id
        else:
            parts = m.text.split()
            if len(parts) < 2:
                await m.reply_text("❌ Usage: .dauth (reply) or .dauth @user/userid")
                return
            target = parts[1]
            try:
                if target.startswith("@"):
                    u = await c.get_users(target[1:])
                else:
                    u = await c.get_users(int(target))
                target_id = u.id
            except Exception as e:
                await m.reply_text(f"❌ {e}")
                return
        music_authorized[host_id].discard(target_id)
        await m.reply_text(f"✅ User {target_id} removed from music sudo!")

    @user_client.on_message(filters.regex(r"^\.authlist$") & filters.group)
    async def _authlist(c, m: Message):
        ids = music_authorized.get(host_id, set())
        if not ids:
            await m.reply_text("ℹ️ No music sudo users")
            return
        text = "👥 Music Sudo Users:\n\n"
        for uid in ids:
            try:
                u = await c.get_users(uid)
                text += f"• {u.first_name} - {u.id}\n"
            except Exception:
                text += f"• {uid}\n"
        await m.reply_text(text)

    @user_client.on_message(filters.regex(r"^\.ping$") & filters.group)
    async def _ping(c, m: Message):
        import time
        s = time.time()
        msg = await m.reply_text("🏓 Pinging...")
        e = time.time()
        await msg.edit_text(f"🏓 Pong! {int((e - s) * 1000)}ms")

    @user_client.on_message(filters.regex(r"^\.cmds$") & filters.group)
    async def _cmds(c, m: Message):
        await m.reply_text(get_music_commands_text())

    logger.info(f"✅ Music handlers registered for host {host_id}")


# ============================================================
# 🚀 STARTUP
# ============================================================
async def startup():
    logger.info("🔄 Restoring sessions from MongoDB...")
    await restore_default_account()
    # Restore all custom user sessions
    async for doc in sessions_col.find({"type": "custom"}):
        await restore_user_account(doc["user_id"])
    # Restore all music hosts
    async for doc in music_hosts_col.find({}):
        await restore_music_host(doc["user_id"])
    logger.info("✅ All sessions restored!")


async def main():
    logger.info("🚀 Starting VC + Music Mega Bot...")
    logger.info(f"Owner ID: {OWNER_ID}")
    logger.info(f"🔊 Audio Boost: {AUDIO_BOOST}x LOUD")
    logger.info("🗄️ MongoDB connected")
    logger.info("🔥 Powered by @zudo_userbot")

    await bot.start()
    await startup()
    logger.info("✅ Bot is running!")

    # Keep alive forever (replaces deprecated pyrogram.idle)
    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Bot stopping...")
    finally:
        try:
            await bot.stop()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
