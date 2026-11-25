# -*- coding: utf-8 -*-
"""
Telegram-бот: парсер username + рассылка сообщений с QR-авторизацией.
Multi-user: у каждого пользователя бота — своя Telethon-сессия.
"""

import asyncio
import io
import logging
import re
import sqlite3
import time
from urllib.parse import urlparse

import qrcode
from telethon import TelegramClient, functions
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError,
    SessionPasswordNeededError,
    UsernameNotOccupiedError,
    UsernameInvalidError,
    UserPrivacyRestrictedError,
    UserIsBlockedError,
    InputUserDeactivatedError,
    PeerFloodError,
)
from telethon.tl.types import User
from telegram import Update, InputFile, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.request import HTTPXRequest

# ============================================================
# НАСТРОЙКИ — ВСТАВЬ СВОИ ЗНАЧЕНИЯ (ОБЯЗАТЕЛЬНО НОВЫЕ!)
# ============================================================

BOT_TOKEN = "9266824511:AAFIRg8fvVMCL0wVQJH4e8SPJFBvs-_WpGE"
API_ID = 37148893  # Вставь новый API_ID
API_HASH = "951959306045b3fce3f68b12754b1aac"

MESSAGE_LIMIT = 1500          
MAX_USERNAMES = 500           
MAX_COMMENTS_TOTAL = 3000     

SEND_DELAY_SECONDS = 30       
MAX_RECIPIENTS_PER_RUN = 200  

DB_PATH = "sessions.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("tg_parser")

processing_users = set()   
active_mailings = {}       


# ============================================================
# МЕНЕДЖЕР КЛИЕНТОВ
# ============================================================

class ClientManager:
    def __init__(self, api_id, api_hash, db_path=DB_PATH):
        self.api_id = api_id
        self.api_hash = api_hash
        self.db_path = db_path
        self.active_clients = {}
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    user_id INTEGER PRIMARY KEY,
                    session_string TEXT NOT NULL
                )
            """)

    def load_session(self, user_id: int):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)
            ).fetchone()
            return row[0] if row else None

    def save_session(self, user_id: int, session_string: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO sessions (user_id, session_string) VALUES (?, ?)",
                (user_id, session_string),
            )

    def delete_session(self, user_id: int):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))

    async def get_client(self, user_id: int):
        client = self.active_clients.get(user_id)
        if client is not None:
            try:
                if client.is_connected() and await client.is_user_authorized():
                    return client
            except Exception:
                log.debug("Клиент user_id=%s сломан, переподключаю", user_id)
            try:
                await client.disconnect()
            except Exception:
                pass
            self.active_clients.pop(user_id, None)

        session_str = self.load_session(user_id)
        if not session_str:
            return None

        # Прокси убран
        client = TelegramClient(StringSession(session_str), self.api_id, self.api_hash)
        try:
            await client.connect()
        except Exception:
            log.exception("Не удалось подключить клиента user_id=%s", user_id)
            return None

        if await client.is_user_authorized():
            self.active_clients[user_id] = client
            return client

        await client.disconnect()
        return None

    async def add_new_client(self, user_id: int, client: TelegramClient):
        self.active_clients[user_id] = client
        self.save_session(user_id, client.session.save())

    async def remove_client(self, user_id: int):
        client = self.active_clients.pop(user_id, None)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        self.delete_session(user_id)

    async def disconnect_all(self):
        for _uid, client in list(self.active_clients.items()):
            try:
                await client.disconnect()
            except Exception:
                pass
        self.active_clients.clear()


# ============================================================
# ПАРСИНГ ССЫЛОК И ПОЛУЧАТЕЛЕЙ
# ============================================================

def parse_public_link(value: str):
    value = value.strip()
    if value.startswith("@"):
        return value[1:]

    parsed = urlparse(value if "://" in value else "https://" + value)
    if parsed.hostname not in ("t.me", "www.t.me", "telegram.me", "www.telegram.me"):
        return None

    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        return None
    if parts[0].startswith("+") or parts[0] == "joinchat":
        return None
    if parts[0] in ("c", "s"):
        return None

    username = parts[0]
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
        return None
    return username


def parse_recipients(raw_text: str):
    cleaned = re.sub(r"https?://(?:www\.)?(?:t|telegram)\.(?:me|dog)/", "", raw_text)
    parts = re.split(r"[\s,;]+", cleaned)
    valid, invalid, seen = [], [], set()
    for p in parts:
        p = p.strip().lstrip("@")
        if not p:
            continue
        if re.fullmatch(r"[A-Za-z0-9_]{5,32}", p):
            key = p.lower()
            if key not in seen:
                seen.add(key)
                valid.append(p)
        else:
            invalid.append(p)
    return valid, invalid


# ============================================================
# ПАРСИНГ КАНАЛА/ГРУППЫ
# ============================================================

def add_sender_username(target: dict, sender, me_id):
    if not sender:
        return
    if getattr(sender, "bot", False):
        return
    sid = getattr(sender, "id", None)
    if me_id is not None and sid == me_id:
        return
    username = getattr(sender, "username", None)
    if username:
        target[username.lower()] = "@" + username


async def collect_from_chat(client, entity, progress_message):
    found = {}
    scanned_messages = 0
    scanned_comments = 0
    comment_groups_checked = set()

    try:
        me = await client.get_me()
        me_id = me.id if me else None
    except Exception:
        me_id = None

    async for message in client.iter_messages(entity, limit=MESSAGE_LIMIT):
        scanned_messages += 1

        try:
            sender = await message.get_sender()
            add_sender_username(found, sender, me_id)
        except Exception:
            log.debug("Не удалось получить автора сообщения %s", message.id, exc_info=True)

        if getattr(message, "replies", None) and getattr(message.replies, "replies", 0):
            if scanned_comments < MAX_COMMENTS_TOTAL:
                try:
                    discussion = await client(
                        functions.messages.GetDiscussionMessageRequest(
                            peer=entity,
                            msg_id=message.id,
                        )
                    )

                    discussion_chat = None
                    for chat in discussion.chats:
                        if getattr(chat, "megagroup", False) or getattr(chat, "gigagroup", False):
                            discussion_chat = chat
                            break

                    if discussion_chat is not None:
                        discussion_entity = await client.get_entity(discussion_chat)
                        top_message = discussion.messages[0]

                        key = (getattr(discussion_chat, "id", None), top_message.id)
                        if key not in comment_groups_checked:
                            comment_groups_checked.add(key)

                            async for comment in client.iter_messages(
                                discussion_entity,
                                reply_to=top_message.id,
                                limit=500,
                            ):
                                if scanned_comments >= MAX_COMMENTS_TOTAL:
                                    break
                                scanned_comments += 1
                                try:
                                    comment_sender = await comment.get_sender()
                                    add_sender_username(found, comment_sender, me_id)
                                except Exception:
                                    log.debug(
                                        "Не удалось получить автора комментария %s",
                                        comment.id,
                                        exc_info=True,
                                    )

                                if len(found) >= MAX_USERNAMES:
                                    break

                except FloodWaitError as error:
                    await progress_message.edit_text(
                        f"Telegram временно ограничил запросы. "
                        f"Подожди {error.seconds} секунд и попробуй снова."
                    )
                    raise
                except Exception:
                    log.debug(
                        "Не удалось прочитать комментарии к посту %s",
                        message.id,
                        exc_info=True,
                    )

        if len(found) >= MAX_USERNAMES or scanned_comments >= MAX_COMMENTS_TOTAL:
            break

        if scanned_messages % 100 == 0:
            try:
                await progress_message.edit_text(
                    f"Проверено сообщений: {scanned_messages}\n"
                    f"Комментариев: {scanned_comments}\n"
                    f"Уникальных username: {len(found)}"
                )
            except Exception:
                pass

    return sorted(found.values(), key=str.casefold), scanned_messages, scanned_comments


# ============================================================
# ОБЩИЕ КОМАНДЫ
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "Привет! Я умею:\n"
        "• собирать публичные @username авторов из каналов и групп;\n"
        "• рассылать сообщение с твоего аккаунта по списку username.\n\n"
        "Подключение:\n"
        "1. /login — отсканируй QR (Настройки → Устройства → Подключить устройство).\n\n"
        "Парсинг username:\n"
        "2. Отправь ссылку на публичный канал/группу (например, https://t.me/channelname).\n"
        "3. Получи файлы TXT и CSV со списком.\n\n"
        "Рассылка:\n"
        "4. /send → пришли текст → пришли список юзернеймов → /confirm.\n"
        "   Сообщения уходят с твоего аккаунта с паузой "
        f"{SEND_DELAY_SECONDS} сек между получателями.\n\n"
        "Команды:\n"
        "/login — подключить аккаунт\n"
        "/logout — отключить аккаунт\n"
        "/status — проверить авторизацию\n"
        "/send — подготовить рассылку\n"
        "/confirm — запустить рассылку\n"
        "/stop — остановить рассылку\n"
        "/cancel — отменить подготовку\n\n"
        f"Лимиты: парсинг — до {MAX_USERNAMES} username, "
        f"рассылка — до {MAX_RECIPIENTS_PER_RUN} за запуск."
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    manager: ClientManager = context.bot_data["client_manager"]
    client = await manager.get_client(user_id)

    if client:
        try:
            me = await client.get_me()
            name = me.first_name or ""
            username = f"@{me.username}" if me.username else "(без username)"
            await update.message.reply_text(
                f"✅ Аккаунт подключён: {name} {username}\n"
                "Можешь отправлять ссылки или /send для рассылки."
            )
        except Exception:
            await update.message.reply_text(
                "⚠️ Сессия есть, но проверить её не удалось. "
                "Попробуй /logout и /login заново."
            )
    else:
        await update.message.reply_text("❌ Аккаунт не подключён. Отправь /login.")


async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    manager: ClientManager = context.bot_data["client_manager"]

    state = active_mailings.get(user_id)
    if state and not state["task"].done():
        state["stop"] = True

    await manager.remove_client(user_id)
    await update.message.reply_text(
        "✅ Сессия удалена. Аккаунт отключён. Можешь /login снова."
    )


async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    manager: ClientManager = context.bot_data["client_manager"]

    existing = await manager.get_client(user_id)
    if existing:
        await update.message.reply_text(
            "Ты уже авторизован. Отправь ссылку на канал, /send для рассылки "
            "или сначала /logout."
        )
        return

    # Прокси убран
    client = TelegramClient(StringSession(), manager.api_id, manager.api_hash)
    try:
        await client.connect()
        qr_login = await client.qr_login()
        url = qr_login.url

        qr_img = qrcode.make(url)
        buf = io.BytesIO()
        qr_img.save(buf, format="PNG")
        buf.seek(0)

        sent_msg = await update.message.reply_photo(
            photo=InputFile(buf, filename="qr.png"),
            caption=(
                "📱 Отсканируй QR-код в Telegram:\n"
                "Настройки → Устройства → Подключить устройство.\n\n"
                "Код действует около минуты."
            ),
        )

        try:
            await asyncio.wait_for(qr_login.wait(), timeout=120)
        except SessionPasswordNeededError:
            context.user_data["awaiting_2fa"] = client
            await sent_msg.edit_caption(
                "🔐 Аккаунт защищён паролем 2FA.\n"
                "Напиши свой пароль следующим сообщением."
            )
            return
        except asyncio.TimeoutError:
            await sent_msg.edit_caption(
                "⏱ Время ожидания истекло. Отправь /login ещё раз."
            )
            await client.disconnect()
            return

        await manager.add_new_client(user_id, client)
        await sent_msg.edit_caption(
            "✅ Аккаунт подключён! Теперь можешь отправлять ссылки на канал "
            "или /send для рассылки."
        )
    except Exception:
        log.exception("Ошибка при входе по QR")
        try:
            await client.disconnect()
        except Exception:
            pass
        await update.message.reply_text("Не удалось войти. Попробуй /login ещё раз.")


async def handle_2fa_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    client: TelegramClient = context.user_data.get("awaiting_2fa")
    if client is None:
        return

    password = (update.message.text or "").strip()
    user_id = update.effective_user.id
    manager: ClientManager = context.bot_data["client_manager"]

    try:
        await update.message.delete()
    except Exception:
        pass

    try:
        await client.sign_in(password=password)
    except Exception as e:
        await update.effective_message.reply_text(
            f"❌ Неверный пароль или ошибка: {type(e).__name__}\n"
            "Попробуй /login заново."
        )
        try:
            await client.disconnect()
        except Exception:
            pass
        context.user_data.pop("awaiting_2fa", None)
        return

    await manager.add_new_client(user_id, client)
    context.user_data.pop("awaiting_2fa", None)
    await update.effective_message.reply_text(
        "✅ Пароль принят. Аккаунт подключён! Можешь отправлять ссылку "
        "или /send для рассылки."
    )


# ============================================================
# ПАРСИНГ КАНАЛА (ОБРАБОТЧИК)
# ============================================================

async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    manager: ClientManager = context.bot_data["client_manager"]

    if user_id in processing_users:
        await update.effective_message.reply_text("Твой предыдущий запрос ещё обрабатывается.")
        return

    state = active_mailings.get(user_id)
    if state and not state["task"].done():
        await update.effective_message.reply_text(
            "Сейчас идёт рассылка. Дождись окончания или /stop."
        )
        return

    client = await manager.get_client(user_id)
    if not client:
        await update.effective_message.reply_text(
            "Сначала авторизуйся: отправь /login и отсканируй QR-код."
        )
        return

    username = parse_public_link(update.effective_message.text or "")
    if not username:
        await update.effective_message.reply_text(
            "Не удалось распознать ссылку.\n"
            "Пример: https://t.me/channelname\n"
            "Или используй /send для рассылки."
        )
        return

    processing_users.add(user_id)
    status = await update.effective_message.reply_text(
        f"Подключаюсь к @{username} и проверяю доступ..."
    )

    try:
        entity = await client.get_entity(username)
        await status.edit_text(
            f"Анализирую сообщения и доступные комментарии @{username}..."
        )

        usernames, messages_count, comments_count = await collect_from_chat(
            client, entity, status
        )

        if not usernames:
            await status.edit_text(
                "Публичные username авторов не найдены. "
                "Возможно, авторы скрыты или у них нет username."
            )
            return

        txt = io.BytesIO(("\n".join(usernames) + "\n").encode("utf-8"))
        txt.name = "usernames.txt"
        await update.effective_message.reply_document(
            document=InputFile(txt, filename="usernames.txt"),
            caption=f"TXT: {len(usernames)} уникальных username",
        )

        csv_text = "username\r\n" + "".join(name + "\r\n" for name in usernames)
        csv_file = io.BytesIO(("\ufeff" + csv_text).encode("utf-8"))
        csv_file.name = "usernames.csv"
        await update.effective_message.reply_document(
            document=InputFile(csv_file, filename="usernames.csv"),
            caption=f"CSV: {len(usernames)} уникальных username",
        )

        await status.edit_text(
            f"Готово!\n"
            f"Уникальных username: {len(usernames)}\n"
            f"Проверено сообщений: {messages_count}\n"
            f"Проверено комментариев: {comments_count}\n\n"
            "Повторы удалены. Выгрузка содержит только публичные username авторов."
        )

    except ValueError as error:
        await status.edit_text(f"Не удалось открыть чат: {error}")
    except Exception:
        log.exception("Ошибка при обработке ссылки")
        await status.edit_text(
            "Не удалось обработать ссылку. Проверь, что твой аккаунт имеет доступ "
            "к этому публичному чату/каналу."
        )
    finally:
        processing_users.discard(user_id)


# ============================================================
# РАССЫЛКА СООБЩЕНИЙ
# ============================================================

async def send_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    manager: ClientManager = context.bot_data["client_manager"]

    client = await manager.get_client(user_id)
    if not client:
        await update.message.reply_text(
            "Сначала авторизуйся: /login и отсканируй QR-код."
        )
        return

    state = active_mailings.get(user_id)
    if state and not state["task"].done():
        await update.message.reply_text("У тебя уже идёт рассылка. /stop чтобы остановить.")
        return

    for k in ("mailing_text", "mailing_recipients",
              "awaiting_recipients", "awaiting_confirm", "awaiting_mailing_text"):
        context.user_data.pop(k, None)
    context.user_data["awaiting_mailing_text"] = True

    await update.message.reply_text(
        "✍️ Пришли текст сообщения, которое нужно разослать.\n\n"
        "Отмена — /cancel."
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for k in ("awaiting_mailing_text", "mailing_text", "mailing_recipients",
              "awaiting_recipients", "awaiting_confirm"):
        context.user_data.pop(k, None)
    await update.message.reply_text("Ок, отменено.")


async def confirm_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    recipients = context.user_data.get("mailing_recipients")
    text = context.user_data.get("mailing_text")

    if not recipients or not text:
        await update.message.reply_text("Нет подготовленной рассылки. Начни с /send.")
        return
    if not context.user_data.get("awaiting_confirm"):
        await update.message.reply_text("Сначала пришли список получателей после /send.")
        return

    context.user_data.pop("awaiting_confirm", None)
    manager: ClientManager = context.bot_data["client_manager"]
    client = await manager.get_client(user_id)
    if not client:
        await update.message.reply_text("Сессия потеряна. /login заново.")
        return

    # Сохраняем состояние
    active_mailings[user_id] = {
        "task": None,
        "stop": False,
        "sent": 0,
        "failed": 0,
        "total": len(recipients),
        "text": text,
        "recipients": recipients,
        "current_idx": 0,
        "paused": False
    }

    task = asyncio.create_task(
        run_mailing(context, user_id, update.effective_chat.id, client, text, recipients, 0)
    )
    active_mailings[user_id]["task"] = task

    await update.message.reply_text(
        f"🚀 Старт рассылки на {len(recipients)} получателей.\n"
        f"Интервал: {SEND_DELAY_SECONDS} сек. Остановить — /stop."
    )


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = active_mailings.get(user_id)
    if not state or state["task"].done():
        await update.message.reply_text("Активной рассылки нет.")
        return
    state["stop"] = True
    await update.message.reply_text("⏹ Останавливаю после текущего сообщения...")


async def resume_mailing_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    state = active_mailings.get(user_id)
    if not state or not state.get("paused"):
        await query.edit_message_text("Нет приостановленной рассылки.")
        return

    await query.edit_message_text("▶️ Продолжаю рассылку...")
    state["paused"] = False
    
    manager: ClientManager = context.bot_data["client_manager"]
    client = await manager.get_client(user_id)
    if not client:
        await context.bot.send_message(query.message.chat_id, "Сессия потеряна. /login заново.")
        active_mailings.pop(user_id, None)
        return

    task = asyncio.create_task(
        run_mailing(context, user_id, query.message.chat_id, client, state["text"], state["recipients"], state["current_idx"])
    )
    state["task"] = task


async def run_mailing(context: ContextTypes.DEFAULT_TYPE, user_id: int, chat_id: int,
                      client: TelegramClient, text: str, recipients: list, start_idx: int = 0):
    state = active_mailings.get(user_id)
    if not state:
        return

    progress_msg = await context.bot.send_message(
        chat_id, f"📤 Рассылка: {start_idx}/{state['total']}..."
    )
    last_edit = 0.0
    failed_log = []

    try:
        for idx, username in enumerate(recipients[start_idx:], start=start_idx + 1):
            if state.get("stop"):
                break

            state["current_idx"] = idx

            try:
                entity = await client.get_entity(username)
                if not isinstance(entity, User):
                    state["failed"] += 1
                    failed_log.append(f"@{username} — не пользователь")
                else:
                    await client.send_message(entity, text)
                    state["sent"] += 1
            except FloodWaitError as e:
                await context.bot.send_message(
                    chat_id,
                    f"⏳ Telegram ограничил отправку. Жду {e.seconds} сек..."
                )
                await asyncio.sleep(e.seconds)
                try:
                    entity = await client.get_entity(username)
                    if isinstance(entity, User):
                        await client.send_message(entity, text)
                        state["sent"] += 1
                    else:
                        state["failed"] += 1
                except Exception as e2:
                    state["failed"] += 1
                    failed_log.append(f"@{username} — {type(e2).__name__}")
            except (UsernameNotOccupiedError, UsernameInvalidError):
                state["failed"] += 1
                failed_log.append(f"@{username} — юзернейм не существует")
            except UserPrivacyRestrictedError:
                state["failed"] += 1
                failed_log.append(f"@{username} — приватность")
            except UserIsBlockedError:
                state["failed"] += 1
                failed_log.append(f"@{username} — заблокировал тебя")
            except InputUserDeactivatedError:
                state["failed"] += 1
                failed_log.append(f"@{username} — аккаунт удалён")
            except PeerFloodError:
                # Ловим спам-блок
                state["paused"] = True
                keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Продолжить рассылку", callback_data="resume_mailing")]])
                await context.bot.send_message(
                    chat_id,
                    "⚠️ **Telegram выдал спам-блок!**\n\n"
                    "Зайди в @SpamBot, нажми Start, выбери кнопку **'This was a mistake'** (или подожди), "
                    "сними ограничение. Затем нажми кнопку ниже, чтобы продолжить.",
                    reply_markup=keyboard,
                    parse_mode="Markdown"
                )
                return # Выходим, сохраняя состояние в active_mailings
            except Exception as e:
                state["failed"] += 1
                failed_log.append(f"@{username} — {type(e).__name__}: {e}")
                log.exception("Ошибка отправки @%s", username)

            now = time.monotonic()
            if now - last_edit >= 3 or idx == state["total"] or state.get("stop"):
                last_edit = now
                try:
                    await progress_msg.edit_text(
                        f"📤 Рассылка: {idx}/{state['total']}\n"
                        f"✅ Отправлено: {state['sent']}\n"
                        f"❌ Ошибок: {state['failed']}"
                    )
                except Exception:
                    pass

            if idx < state["total"] and not state.get("stop"):
                await asyncio.sleep(SEND_DELAY_SECONDS)

        summary = (
            f"{'⏹ Остановлено' if state.get('stop') else '✅ Готово'}.\n"
            f"Отправлено: {state['sent']}\n"
            f"Ошибок: {state['failed']}\n"
            f"Всего: {state['total']}"
        )
        if failed_log:
            summary += "\n\nПервые ошибки:\n" + "\n".join(failed_log[:20])
        await context.bot.send_message(chat_id, summary)
    except Exception:
        log.exception("Ошибка в run_mailing")
        try:
            await context.bot.send_message(
                chat_id, "❌ Рассылка прервана из-за ошибки. Смотри логи."
            )
        except Exception:
            pass
    finally:
        # Удаляем из активных только если это не пауза (спам-блок)
        if not state.get("paused"):
            active_mailings.pop(user_id, None)


# ============================================================
# ЕДИНЫЙ ДИСПЕТЧЕР ТЕКСТОВЫХ СООБЩЕНИЙ
# ============================================================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 1) ждём пароль 2FA
    if context.user_data.get("awaiting_2fa"):
        await handle_2fa_password(update, context)
        return

    # 2) ждём текст для рассылки
    if context.user_data.get("awaiting_mailing_text"):
        text = (update.message.text or "").strip()
        if not text:
            await update.message.reply_text("Текст пустой. Пришли сообщение или /cancel.")
            return
        if len(text) > 4096:
            await update.message.reply_text("Слишком длинно (макс. 4096 символов).")
            return
        context.user_data.pop("awaiting_mailing_text", None)
        context.user_data["mailing_text"] = text
        context.user_data["awaiting_recipients"] = True
        await update.message.reply_text(
            "✅ Текст сохранён.\n\n"
            "Теперь пришли список юзернеймов — по одному в строке, "
            "или через запятую/пробел. Можно с @ или без.\n"
            f"Максимум: {MAX_RECIPIENTS_PER_RUN}.\n\n"
            "Отмена — /cancel."
        )
        return

    # 3) ждём получателей
    if context.user_data.get("awaiting_recipients"):
        raw = update.message.text or ""
        valid, invalid = parse_recipients(raw)
        if not valid:
            await update.message.reply_text(
                "Не нашёл валидных юзернеймов. "
                "Пример: @durov или durov (5–32 символа, A-Z, 0-9, _)."
            )
            return
        truncated = False
        if len(valid) > MAX_RECIPIENTS_PER_RUN:
            valid = valid[:MAX_RECIPIENTS_PER_RUN]
            truncated = True

        context.user_data.pop("awaiting_recipients", None)
        context.user_data["mailing_recipients"] = valid
        context.user_data["awaiting_confirm"] = True

        preview = "\n".join("@" + u for u in valid[:10])
        more = "" if len(valid) <= 10 else f"\n... и ещё {len(valid) - 10}"
        warn = ""
        if invalid:
            warn = f"\n\n⚠️ Пропущено невалидных: {len(invalid)} ({', '.join(invalid[:5])})"
        if truncated:
            warn += f"\n⚠️ Обрезано до {MAX_RECIPIENTS_PER_RUN}."
        await update.message.reply_text(
            f"Готово к рассылке.\n"
            f"Получателей: {len(valid)}\n"
            f"Интервал: {SEND_DELAY_SECONDS} сек\n"
            f"Примерное время: ~{len(valid) * SEND_DELAY_SECONDS // 60} мин\n\n"
            f"{preview}{more}{warn}\n\n"
            f"Текст:\n---\n{context.user_data['mailing_text'][:500]}\n---\n\n"
            f"Подтверди: /confirm\nОтмена: /cancel"
        )
        return

    # 4) иначе — пробуем как ссылку на канал
    await handle_link(update, context)


# ============================================================
# ЗАПУСК
# ============================================================

async def on_shutdown(app: Application):
    manager: ClientManager = app.bot_data.get("client_manager")
    if manager:
        await manager.disconnect_all()


def main():
    if (
        not BOT_TOKEN
        or BOT_TOKEN.startswith("ВСТАВЬ")
        or not API_HASH
        or API_HASH.startswith("ВСТАВЬ")
        or API_ID == 12345678
    ):
        raise RuntimeError("Сначала заполни BOT_TOKEN, API_ID, API_HASH в начале bot.py.")

    manager = ClientManager(API_ID, API_HASH)

    # Прокси убран из HTTPXRequest
    request = HTTPXRequest(connect_timeout=30, read_timeout=30)
    get_updates_request = HTTPXRequest(connect_timeout=30, read_timeout=30)

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(get_updates_request)
        .build()
    )
    
    app.bot_data["client_manager"] = manager
    app.post_shutdown = on_shutdown

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("login", login_command))
    app.add_handler(CommandHandler("logout", logout_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("send", send_command))
    app.add_handler(CommandHandler("confirm", confirm_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("cancel", cancel_command))

    # Добавляем обработчик кнопки "Продолжить"
    app.add_handler(CallbackQueryHandler(resume_mailing_callback, pattern="^resume_mailing$"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("Бот запущен. Нажми Ctrl+C для остановки.")
    app.run_polling()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Остановка бота...")