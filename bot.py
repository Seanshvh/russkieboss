import asyncio
import logging
import os
import re
import random
import io
from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import aiosqlite
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, BufferedInputFile,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# ==================== CONFIG ====================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
DB_PATH = "bot.db"

LOGIN_RE = re.compile(r"^[a-zA-Z0-9_]{3,20}$")
PAGE_SIZE = 8


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# ==================== DATABASE ====================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                tg_id      INTEGER PRIMARY KEY,
                username   TEXT,
                full_name  TEXT,
                login      TEXT UNIQUE,
                crystals   INTEGER NOT NULL DEFAULT 0,
                is_trading INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                network    TEXT NOT NULL,
                address    TEXT NOT NULL,
                added_by   INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS withdrawals (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id        INTEGER NOT NULL,
                login        TEXT NOT NULL,
                network      TEXT NOT NULL,
                wallet       TEXT NOT NULL,
                amount       INTEGER NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP
            )
        """)
        # ТАБЛИЦА ДЛЯ ИСТОРИИ СДЕЛОК
        await db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id      INTEGER NOT NULL,
                coin       TEXT NOT NULL,
                direction  TEXT NOT NULL,
                profit     REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

        # Миграции
        async with db.execute("PRAGMA table_info(users)") as cur:
            cols = {row[1] for row in await cur.fetchall()}
        if "login" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN login TEXT")
        if "crystals" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN crystals INTEGER NOT NULL DEFAULT 0")
        if "is_trading" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN is_trading INTEGER NOT NULL DEFAULT 0")
        await db.commit()


async def get_user(tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE tg_id = ?", (tg_id,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_user_by_login(login: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT tg_id FROM users WHERE login = ?", (login,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else None


async def upsert_user(tg_id: int, username, full_name, login: str | None = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (tg_id, username, full_name, login)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(tg_id) DO UPDATE SET
                username  = excluded.username,
                full_name = excluded.full_name,
                login     = COALESCE(users.login, excluded.login)
        """, (tg_id, username, full_name, login))
        await db.commit()


async def set_trading_status(tg_id: int, status: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_trading = ? WHERE tg_id = ?", (status, tg_id))
        await db.commit()


async def add_crystals(tg_id: int, amount: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET crystals = crystals + ? WHERE tg_id = ?", (int(round(amount)), tg_id))
        await db.commit()


async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users WHERE login IS NOT NULL") as cur:
            total = (await cur.fetchone())[0]
        async with db.execute("SELECT COALESCE(SUM(crystals), 0) FROM users") as cur:
            total_crystals = (await cur.fetchone())[0]
    return {"total": total, "total_crystals": total_crystals}


async def get_all_registered_ids() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT tg_id FROM users WHERE login IS NOT NULL AND crystals > 0") as cur:
            return [r[0] for r in await cur.fetchall()]


async def get_active_traders():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT tg_id, login, crystals FROM users WHERE crystals > 0 AND is_trading = 1 AND login IS NOT NULL") as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_clients_page(page: int):
    offset = page * PAGE_SIZE
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT tg_id, login, crystals FROM users
               WHERE login IS NOT NULL ORDER BY created_at DESC LIMIT ? OFFSET ?""",
            (PAGE_SIZE, offset),
        ) as cur:
            rows = [dict(r) for r in await cur.fetchall()]
        async with db.execute("SELECT COUNT(*) FROM users WHERE login IS NOT NULL") as cur:
            total = (await cur.fetchone())[0]
    return rows, total


async def add_wallet(network: str, address: str, admin_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO wallets (network, address, added_by) VALUES (?, ?, ?)", (network, address, admin_id))
        await db.commit()


async def get_wallets():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM wallets ORDER BY network, id") as cur:
            return [dict(r) for r in await cur.fetchall()]


async def delete_wallet(wallet_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM wallets WHERE id = ?", (wallet_id,))
        await db.commit()


async def get_networks() -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT DISTINCT network FROM wallets ORDER BY network") as cur:
            return [r[0] for r in await cur.fetchall()]


async def create_withdrawal(tg_id, login, network, wallet, amount) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO withdrawals (tg_id, login, network, wallet, amount) VALUES (?, ?, ?, ?, ?)",
            (tg_id, login, network, wallet, amount),
        )
        await db.commit()
        return cur.lastrowid


async def get_withdrawal(wid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM withdrawals WHERE id = ?", (wid,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_pending_withdrawals():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM withdrawals WHERE status = 'pending' ORDER BY id DESC") as cur:
            return [dict(r) for r in await cur.fetchall()]


async def finish_withdrawal(wid: int, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE withdrawals SET status = ?, processed_at = CURRENT_TIMESTAMP WHERE id = ?", (status, wid))
        await db.commit()


async def save_trade(tg_id: int, coin: str, direction: str, profit: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO trades (tg_id, coin, direction, profit) VALUES (?, ?, ?, ?)", (tg_id, coin, direction, profit))
        await db.commit()


async def get_user_trades(tg_id: int, limit=5):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM trades WHERE tg_id = ? ORDER BY id DESC LIMIT ?", (tg_id, limit)) as cur:
            return [dict(r) for r in await cur.fetchall()]


# ==================== ГЕНЕРАТОР И ГРАФИК ====================
def generate_trades_data(balance: int):
    num_trades = random.randint(3, 6)
    trades = []
    total_profit = 0.0
    
    coins = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "DOGE/USDT"]
    directions = ["Long 🟢", "Short 🔴"]
    
    for i in range(1, num_trades + 1):
        percent = random.uniform(0.003, 0.025) # 0.3% - 2.5%
        profit = round(balance * percent, 2)
        total_profit += profit
        
        coin = random.choice(coins)
        direction = random.choice(directions)
        trades.append({"coin": coin, "direction": direction, "profit": profit})
        
    return trades, round(total_profit, 2)


def create_chart_image(balance: int):
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(6, 4))
    
    steps = 20
    x = list(range(steps))
    y = [balance]
    
    for _ in range(steps - 1):
        change = random.uniform(-0.01, 0.03) * balance
        y.append(y[-1] + change)
        
    ax.plot(x, y, color='#00FF00', linewidth=2, marker='o', markersize=3)
    ax.fill_between(x, y, color='#00FF00', alpha=0.1)
    
    ax.set_title("Рост вашего депозита", color='white')
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.set_xticks([])
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', transparent=True)
    plt.close(fig)
    return buf


# ==================== KEYBOARDS ====================
def client_menu_kb(user=None) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data="client:profile"),
            InlineKeyboardButton(text="📈 Статистика", callback_data="client:stats"),
        ],
        [
            InlineKeyboardButton(text="📉 График", callback_data="client:chart"),
            InlineKeyboardButton(text="💬 Поддержка", callback_data="client:support")
        ],
        [
            InlineKeyboardButton(text="💵 Пополнение", callback_data="client:deposit"),
            InlineKeyboardButton(text="📤 Вывод",      callback_data="client:withdraw"),
        ]
    ]
    
    # Если баланс есть, но торговля не запущена - показываем кнопку ЗАПУСКА
    if user and user['crystals'] > 0 and not user['is_trading']:
        buttons.insert(0, [InlineKeyboardButton(text="🚀 ЗАПУСТИТЬ ТОРГОВЛЮ", callback_data="client:start_trading")])
    elif user and user['is_trading']:
        buttons.insert(0, [InlineKeyboardButton(text="🟢 Бот торгует (нажми для инфо)", callback_data="client:trading_info")])
        
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"),
            InlineKeyboardButton(text="👥 Клиенты",    callback_data="admin:users:0"),
        ],
        [
            InlineKeyboardButton(text="💼 Кошельки",   callback_data="admin:wallets"),
            InlineKeyboardButton(text="📤 Заявки",     callback_data="admin:withdrawals"),
        ],
        [
            InlineKeyboardButton(text="📢 Рассылка", callback_data="admin:broadcast"),
        ]
    ])


def back_to_admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ В админ-меню", callback_data="admin:menu")]
    ])

def back_to_client_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ В главное меню", callback_data="client:menu")]
    ])

def cancel_client_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="client:menu")]
    ])

def cancel_admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Вернуться в админ-меню", callback_data="admin:menu")]
    ])


def clients_page_kb(rows, page, total) -> InlineKeyboardMarkup:
    buttons = []
    for r in rows:
        buttons.append([InlineKeyboardButton(
            text=f"{r['login']}  —  $ {r['crystals']}",
            callback_data=f"admin:user:{r['tg_id']}",
        )])
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin:users:{page-1}"))
    if page + 1 < total_pages:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin:users:{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="⬅️ В админ-меню", callback_data="admin:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def client_card_kb(tg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Начислить $", callback_data=f"admin:credit:{tg_id}")],
        [InlineKeyboardButton(text="⬅️ К клиентам", callback_data="admin:users:0")],
        [InlineKeyboardButton(text="⬅️ В админ-меню", callback_data="admin:menu")]
    ])


def networks_kb(networks: list[str]) -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton(text=n, callback_data=f"wd:net:{n}")] for n in networks]
    buttons.append([InlineKeyboardButton(text="⬅️ В главное меню", callback_data="client:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_wallets_kb(wallets: list[dict]) -> InlineKeyboardMarkup:
    buttons = []
    for w in wallets:
        buttons.append([InlineKeyboardButton(
            text=f"❌ {w['network']}: {w['address'][:18]}…",
            callback_data=f"admin:wallet:del:{w['id']}",
        )])
    buttons.append([InlineKeyboardButton(text="➕ Добавить кошелек", callback_data="admin:wallet:add")])
    buttons.append([InlineKeyboardButton(text="⬅️ В админ-меню", callback_data="admin:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_withdrawals_kb(items: list[dict]) -> InlineKeyboardMarkup:
    buttons = []
    for w in items:
        buttons.append([InlineKeyboardButton(
            text=f"#{w['id']} {w['login']} — ${w['amount']} ({w['network']})",
            callback_data=f"admin:wd:{w['id']}",
        )])
    buttons.append([InlineKeyboardButton(text="⬅️ В админ-меню", callback_data="admin:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def withdrawal_review_kb(wid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Принять",   callback_data=f"wd:accept:{wid}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"wd:reject:{wid}"),
        ],
        [InlineKeyboardButton(text="⬅️ К заявкам", callback_data="admin:withdrawals")],
        [InlineKeyboardButton(text="⬅️ В админ-меню", callback_data="admin:menu")]
    ])


# ==================== FSM ====================
class Registration(StatesGroup):
    waiting_login = State()

class CreditCrystals(StatesGroup):
    waiting_amount = State()
    waiting_notification = State()
    confirm = State()

class Broadcast(StatesGroup):
    waiting_message = State()
    confirm = State()

class AddWallet(StatesGroup):
    waiting_network = State()
    waiting_address = State()

class Withdraw(StatesGroup):
    waiting_network = State()
    waiting_wallet  = State()
    waiting_amount  = State()
    confirm         = State()


# ==================== ROUTERS ====================
common_router = Router()
admin_router = Router()
admin_router.message.filter(F.from_user.id.in_(ADMIN_IDS))
admin_router.callback_query.filter(F.from_user.id.in_(ADMIN_IDS))
client_router = Router()


# ==================== РЕГИСТРАЦИЯ ====================
@common_router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    user = await get_user(message.from_user.id)
    if user is None or not user.get("login"):
        await state.set_state(Registration.waiting_login)
        await message.answer(
            f"👋 Привет, <b>{message.from_user.first_name}</b>!\n\n"
            "Придумай себе <b>логин</b> в боте.\n"
            "Требования: 3–20 символов, латиница, цифры и <code>_</code>.\n"
            "Пример: <code>ivan_2024</code>"
        )
        return

    await upsert_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    await state.clear()
    await message.answer(
        f"👋 С возвращением, <b>{user['login']}</b>!\n"
        f"💵 Баланс: <b>${user['crystals']}</b>\n\nВыбери действие:",
        reply_markup=client_menu_kb(user),
    )
    if is_admin(message.from_user.id):
        await message.answer("🔑 Ты админ. Открой /admin")


@common_router.message(Registration.waiting_login)
async def reg_set_login(message: Message, state: FSMContext, bot: Bot):
    login = (message.text or "").strip()
    if not LOGIN_RE.match(login):
        await message.answer("❌ Логин должен быть 3–20 символов: латиница, цифры, <code>_</code>.\nПопробуй ещё раз:")
        return
    if await get_user_by_login(login):
        await message.answer("❌ Такой логин уже занят. Придумай другой:")
        return

    await upsert_user(message.from_user.id, message.from_user.username, message.from_user.full_name, login=login)
    await state.clear()
    
    user = await get_user(message.from_user.id)
    await message.answer(f"✅ Готово! Твой логин: <code>{login}</code>\n💵 Баланс: <b>$0</b>\n\nМеню:", reply_markup=client_menu_kb(user))
    
    admin_text = (
        f"🆕 <b>Новый клиент!</b>\n\n"
        f"👤 Имя: {message.from_user.full_name}\n"
        f"🔑 Логин: <code>{login}</code>\n"
        f"🆔 ID: <code>{message.from_user.id}</code>"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, admin_text)
        except Exception:
            pass


# ==================== КЛИЕНТ ====================
@client_router.callback_query(F.data == "client:profile")
async def show_profile(call: CallbackQuery):
    user = await get_user(call.from_user.id)
    text = f"👤 <b>Профиль</b>\n\nЛогин: <code>{user['login']}</code>\n💵 Баланс: <b>${user['crystals']}</b>\nID: <code>{user['tg_id']}</code>"
    await call.message.edit_text(text, reply_markup=back_to_client_kb())


# ЗАПУСК ТОРГОВЛИ
@client_router.callback_query(F.data == "client:start_trading")
async def start_trading(call: CallbackQuery, bot: Bot):
    user = await get_user(call.from_user.id)
    if user["crystals"] <= 0:
        await call.answer("❌ Сначала пополните баланс!", show_alert=True)
        return
        
    await set_trading_status(user["tg_id"], 1)
    user = await get_user(call.from_user.id) # обновляем данные
    
    text = (
        f"🚀 <b>Торговый бот успешно запущен!</b>\n\n"
        f"Ваш депозит (${user['crystals']}) передан в управление ИИ-боту.\n"
        f"Он будет торговать на фьючерсах, а <b>каждый час</b> присылать вам отчет о заработанных средствах.\n"
        f"Прибыль будет моментально добавляться к вашему балансу.\n\n"
        f"Ожидайте первый отчет через 60 минут!"
    )
    await call.message.edit_text(text, reply_markup=back_to_client_kb())


@client_router.callback_query(F.data == "client:trading_info")
async def trading_info(call: CallbackQuery):
    await call.answer("🟢 Бот уже активно торгует и зарабатывает для вас прибыль. Ожидайте ежечасный отчет!", show_alert=True)


# РЕАЛЬНАЯ СТАТИСТИКА ИЗ БАЗЫ
@client_router.callback_query(F.data == "client:stats")
async def show_client_stats(call: CallbackQuery):
    user = await get_user(call.from_user.id)
    trades = await get_user_trades(user['tg_id'], limit=7)
    
    if not trades:
        text = "📈 <b>Статистика торговли</b>\n\nУ вас пока нет закрытых сделок. Бот начнет торговать после активации."
        await call.message.edit_text(text, reply_markup=back_to_client_kb())
        return
        
    text = "📈 <b>Последние сделки бота:</b>\n\n"
    total = 0
    for t in trades:
        text += f"🔹 {t['coin']} | {t['direction']}: <b>+${t['profit']}</b>\n"
        total += t['profit']
        
    text += f"\n💰 <b>Общая прибыль (за последние сессии): +${round(total, 2)}</b>"
    text += f"\n💵 <b>Текущий баланс:</b> <b>${user['crystals']}</b>"
    
    await call.message.edit_text(text, reply_markup=back_to_client_kb())


@client_router.callback_query(F.data == "client:chart")
async def show_client_chart(call: CallbackQuery, bot: Bot):
    user = await get_user(call.from_user.id)
    if not user or user["crystals"] <= 0:
        await call.answer("❌ Пополните баланс, чтобы увидеть график!", show_alert=True)
        return

    await call.message.edit_text("⏳ Генерируем ваш график...")
    chart_buf = create_chart_image(user["crystals"])
    photo = BufferedInputFile(chart_buf.read(), filename="chart.png")
    caption = f"📉 <b>График доходности</b>\n\nВаш текущий баланс: <b>${user['crystals']}</b>\nНаш бот успешно торгует 24/7."
    
    await call.message.delete()
    await bot.send_photo(
        chat_id=call.from_user.id,
        photo=photo,
        caption=caption,
        reply_markup=back_to_client_kb()
    )


@client_router.callback_query(F.data == "client:support")
async def show_support(call: CallbackQuery):
    await call.message.edit_text("💬 <b>Поддержка</b>\n\nНапиши нам: @tradingbot2027", reply_markup=back_to_client_kb())


@client_router.callback_query(F.data == "client:menu")
async def back_to_client_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    user = await get_user(call.from_user.id)
    await call.message.edit_text("Выбери действие:", reply_markup=client_menu_kb(user))


@client_router.callback_query(F.data == "client:deposit")
async def show_deposit(call: CallbackQuery):
    wallets = await get_wallets()
    if not wallets:
        await call.message.edit_text("💵 <b>Пополнение</b>\n\nПока реквизиты не настроены. Загляни позже.", reply_markup=back_to_client_kb())
        return

    by_net: dict[str, list[str]] = {}
    for w in wallets:
        by_net.setdefault(w["network"], []).append(w["address"])

    lines = ["💵 <b>Пополнение</b>\n", "Переведи средства на один из кошельков ниже и напиши админу @tradingbot2027 с хешем транзакции:\n"]
    for net, addrs in by_net.items():
        lines.append(f"\n<b>{net}</b>")
        for a in addrs:
            lines.append(f"<code>{a}</code>")

    await call.message.edit_text("\n".join(lines), reply_markup=back_to_client_kb(), disable_web_page_preview=True)


@client_router.callback_query(F.data == "client:withdraw")
async def withdraw_start(call: CallbackQuery, state: FSMContext):
    user = await get_user(call.from_user.id)
    if not user or user["crystals"] <= 0:
        await call.answer("❌ Нечего выводить — баланс $0", show_alert=True)
        return

    networks = await get_networks()
    if not networks:
        await call.answer("Вывод временно недоступен", show_alert=True)
        return

    await state.set_state(Withdraw.waiting_network)
    await call.message.edit_text(f"📤 <b>Вывод</b>\n\nБаланс: <b>${user['crystals']}</b>\n\nВыбери сеть:", reply_markup=networks_kb(networks))


@client_router.callback_query(F.data.startswith("wd:net:"), Withdraw.waiting_network)
async def withdraw_set_network(call: CallbackQuery, state: FSMContext):
    network = call.data.split(":", 2)[2]
    await state.update_data(network=network)
    await state.set_state(Withdraw.waiting_wallet)
    await call.message.edit_text(
        f"📤 <b>Вывод</b>\nСеть: <b>{network}</b>\n\nВведи <b>адрес своего кошелька</b>:", 
        reply_markup=cancel_client_kb()
    )


@client_router.message(Withdraw.waiting_wallet)
async def withdraw_set_wallet(message: Message, state: FSMContext):
    wallet = (message.text or "").strip()
    if len(wallet) < 10 or len(wallet) > 200:
        await message.answer("❌ Похоже на неправильный адрес. Попробуй снова:", reply_markup=cancel_client_kb())
        return

    data = await state.get_data()
    user = await get_user(message.from_user.id)
    await state.update_data(wallet=wallet)
    await state.set_state(Withdraw.waiting_amount)
    await message.answer(
        f"📤 <b>Вывод</b>\nСеть: <b>{data['network']}</b>\nКошелёк: <code>{wallet}</code>\n\nСколько $ вывести? (доступно: <b>${user['crystals']}</b>)",
        reply_markup=cancel_client_kb()
    )


@client_router.message(Withdraw.waiting_amount)
async def withdraw_set_amount(message: Message, state: FSMContext):
    try:
        amount = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ Нужно целое число. Попробуй снова:", reply_markup=cancel_client_kb())
        return

    user = await get_user(message.from_user.id)
    if amount <= 0 or amount > user["crystals"]:
        await message.answer(f"❌ Неверная сумма. У тебя: ${user['crystals']}", reply_markup=cancel_client_kb())
        return

    data = await state.get_data()
    await state.update_data(amount=amount)
    await state.set_state(Withdraw.confirm)
    await message.answer(
        f"Проверь заявку:\n\n🌐 Сеть: <b>{data['network']}</b>\n👛 Кошелёк: <code>{data['wallet']}</code>\n💵 Сумма: <b>${amount}</b>\n\nОтправить админу?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Отправить", callback_data="wd:submit")],
            [InlineKeyboardButton(text="⬅️ В главное меню", callback_data="client:menu")],
        ]),
    )


@client_router.callback_query(F.data == "wd:submit", Withdraw.confirm)
async def withdraw_submit(call: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    await state.clear()

    user = await get_user(call.from_user.id)
    wid = await create_withdrawal(user["tg_id"], user["login"], data["network"], data["wallet"], data["amount"])

    text = (
        f"📤 <b>Новая заявка на вывод #{wid}</b>\n\n"
        f"👤 Клиент: <code>{user['login']}</code>\n"
        f"🌐 Сеть: <b>{data['network']}</b>\n"
        f"👛 Кошелёк: <code>{data['wallet']}</code>\n"
        f"💵 Сумма: <b>${data['amount']}</b>"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            pass

    await call.message.edit_text(f"✅ Заявка #{wid} отправлена админу.", reply_markup=client_menu_kb(user))


# ==================== АДМИН ====================
@admin_router.message(Command("admin"))
async def admin_panel(message: Message):
    await message.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu_kb())


@admin_router.callback_query(F.data == "admin:menu")
async def admin_menu_cb(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_menu_kb())


@admin_router.callback_query(F.data == "admin:stats")
async def admin_stats(call: CallbackQuery):
    stats = await get_stats()
    await call.message.edit_text(f"📊 <b>Статистика</b>\n\n👥 Клиентов: <b>{stats['total']}</b>\n💵 Общий баланс: <b>${stats['total_crystals']}</b>", reply_markup=back_to_admin_kb())


@admin_router.callback_query(F.data.startswith("admin:users:"))
async def admin_users(call: CallbackQuery):
    page = int(call.data.split(":")[2])
    rows, total = await get_clients_page(page)
    if not rows:
        await call.message.edit_text("Пока нет зарегистрированных клиентов.", reply_markup=back_to_admin_kb())
        return
    await call.message.edit_text(f"👥 <b>Клиенты</b> ({total})\n\nВыбери клиента:", reply_markup=clients_page_kb(rows, page, total))


@admin_router.callback_query(F.data.startswith("admin:user:"))
async def admin_user_card(call: CallbackQuery):
    tg_id = int(call.data.split(":")[2])
    user = await get_user(tg_id)
    if not user:
        await call.answer("Клиент не найден", show_alert=True)
        return
    trading_status = "🟢 Активна" if user['is_trading'] else "🔴 Остановлена"
    text = f"👤 <b>Карточка клиента</b>\n\nЛогин: <code>{user['login']}</code>\nИмя: {user['full_name']}\nID: <code>{user['tg_id']}</code>\n💵 Баланс: <b>${user['crystals']}</b>\n📈 Торговля: {trading_status}"
    await call.message.edit_text(text, reply_markup=client_card_kb(tg_id))


@admin_router.callback_query(F.data.startswith("admin:credit:"))
async def credit_start(call: CallbackQuery, state: FSMContext):
    tg_id = int(call.data.split(":")[2])
    user = await get_user(tg_id)
    await state.update_data(target_tg_id=tg_id, target_login=user["login"])
    await state.set_state(CreditCrystals.waiting_amount)
    await call.message.edit_text(
        f"💵 <b>Начисление средств</b>\nКлиент: <code>{user['login']}</code>\n\nСколько начислить? Введи <b>целое число</b>.\nМожно отрицательное.",
        reply_markup=cancel_admin_kb()
    )


@admin_router.message(CreditCrystals.waiting_amount)
async def credit_amount(message: Message, state: FSMContext):
    try:
        amount = int((message.text or "").strip())
        if amount == 0: raise ValueError
    except ValueError:
        await message.answer("❌ Нужно целое число (не 0). Попробуй снова:", reply_markup=cancel_admin_kb())
        return

    await state.update_data(amount=amount)
    await state.set_state(CreditCrystals.waiting_notification)
    await message.answer(f"✍️ Введи текст уведомления для клиента.\nСумма: <b>${amount:+d}</b>", reply_markup=cancel_admin_kb())


@admin_router.message(CreditCrystals.waiting_notification)
async def credit_notification(message: Message, state: FSMContext):
    notification = (message.text or "").strip()
    if not notification:
        await message.answer("❌ Пустое сообщение. Введи текст:", reply_markup=cancel_admin_kb())
        return

    data = await state.get_data()
    await state.update_data(notification=notification)
    await state.set_state(CreditCrystals.confirm)
    await message.answer(
        f"Проверь:\n\n👤 Клиент: <code>{data['target_login']}</code>\n💵 Сумма: <b>${data['amount']:+d}</b>\n📝 Уведомление:\n{notification}\n\nПодтвердить?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Начислить", callback_data="credit:go")],
            [InlineKeyboardButton(text="⬅️ В админ-меню", callback_data="admin:menu")],
        ]),
    )


@admin_router.callback_query(F.data == "credit:go", CreditCrystals.confirm)
async def credit_go(call: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    await state.clear()

    tg_id = data["target_tg_id"]
    amount = data["amount"]
    notification = data["notification"]

    await add_crystals(tg_id, amount)
    user = await get_user(tg_id)

    try:
        await bot.send_message(
            tg_id,
            f"💵 <b>Изменение баланса</b>\n\nСумма: <b>${amount:+d}</b>\nТекущий баланс: <b>${user['crystals']}</b>\n\n{notification}",
            reply_markup=client_menu_kb(user)
        )
        delivered = True
    except Exception:
        delivered = False

    await call.message.edit_text(
        f"✅ Готово.\n\n👤 Клиент: <code>{data['target_login']}</code>\n💵 Сумма: <b>${amount:+d}</b>\n💰 Новый баланс: <b>${user['crystals']}</b>\n📩 Уведомление: {'доставлено ✅' if delivered else 'не доставлено ❌'}",
        reply_markup=back_to_admin_kb(),
    )


@admin_router.callback_query(F.data == "admin:broadcast")
async def broadcast_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(Broadcast.waiting_message)
    await call.message.edit_text(
        "📢 Отправь сообщение (текст) для рассылки.\nРассылка уйдет ТОЛЬКО клиентам с балансом > $0.",
        reply_markup=cancel_admin_kb()
    )


@admin_router.message(Broadcast.waiting_message)
async def broadcast_preview(message: Message, state: FSMContext):
    await state.update_data(text=message.text or message.caption or "")
    await state.set_state(Broadcast.confirm)
    await message.answer(
        f"✅ Отправить это сообщение всем активным клиентам?\n\nТекст:\n{message.text or message.caption}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Отправить", callback_data="broadcast:go")],
            [InlineKeyboardButton(text="⬅️ В админ-меню", callback_data="admin:menu")],
        ]),
    )


@admin_router.callback_query(F.data == "broadcast:go", Broadcast.confirm)
async def broadcast_go(call: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    await state.clear()
    await call.message.edit_text("📤 Рассылка запущена…")

    users = await get_all_registered_ids()
    ok = fail = 0
    base_text = data.get("text", "")
    
    for uid in users:
        user = await get_user(uid)
        if not user: continue
        personal_text = f"{base_text}\n\n💰 <b>Ваш актуальный баланс:</b> <b>${user['crystals']}</b>"
        try:
            # ПРИКРЕПЛЯЕМ МЕНЮ К РАССЫЛКЕ
            await bot.send_message(uid, personal_text, reply_markup=client_menu_kb(user))
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)

    await call.message.answer(f"📢 Готово.\n✅ Доставлено: <b>{ok}</b>\n❌ Ошибок: <b>{fail}</b>", reply_markup=back_to_admin_kb())


@admin_router.callback_query(F.data == "admin:wallets")
async def admin_wallets(call: CallbackQuery):
    wallets = await get_wallets()
    if not wallets:
        await call.message.edit_text("💼 <b>Кошельки</b>\n\nПока пусто. Добавь первый:", reply_markup=admin_wallets_kb([]))
        return
    await call.message.edit_text(f"💼 <b>Кошельки</b> ({len(wallets)})\n\nТап по кошельку — удалить.", reply_markup=admin_wallets_kb(wallets))


@admin_router.callback_query(F.data == "admin:wallet:add")
async def wallet_add_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(AddWallet.waiting_network)
    await call.message.edit_text("🌐 Введи название сети/валюты (например: <code>BTC</code>, <code>USDT (TRC20)</code>):", reply_markup=cancel_admin_kb())


@admin_router.message(AddWallet.waiting_network)
async def wallet_add_network(message: Message, state: FSMContext):
    network = (message.text or "").strip()
    if not network or len(network) > 40:
        await message.answer("❌ Название сети 1–40 символов:", reply_markup=cancel_admin_kb())
        return
    await state.update_data(network=network)
    await state.set_state(AddWallet.waiting_address)
    await message.answer("👛 Теперь введи адрес кошелька:", reply_markup=cancel_admin_kb())


@admin_router.message(AddWallet.waiting_address)
async def wallet_add_address(message: Message, state: FSMContext):
    address = (message.text or "").strip()
    if not (10 <= len(address) <= 200):
        await message.answer("❌ Странный адрес. Попробуй снова:", reply_markup=cancel_admin_kb())
        return
    data = await state.get_data()
    await state.clear()
    await add_wallet(data["network"], address, message.from_user.id)
    await message.answer(f"✅ Добавлено:\n<b>{data['network']}</b>\n<code>{address}</code>", reply_markup=back_to_admin_kb())


@admin_router.callback_query(F.data.startswith("admin:wallet:del:"))
async def wallet_delete(call: CallbackQuery):
    wid = int(call.data.split(":")[3])
    await delete_wallet(wid)
    wallets = await get_wallets()
    await call.message.edit_text("🗑 Удалено.\n\n💼 <b>Кошельки</b>:", reply_markup=admin_wallets_kb(wallets))


@admin_router.callback_query(F.data == "admin:withdrawals")
async def admin_withdrawals(call: CallbackQuery):
    items = await get_pending_withdrawals()
    if not items:
        await call.message.edit_text("📤 <b>Заявки на вывод</b>\n\nНет активных заявок.", reply_markup=back_to_admin_kb())
        return
    await call.message.edit_text(f"📤 <b>Заявки на вывод</b> ({len(items)})\n\nВыбери заявку:", reply_markup=admin_withdrawals_kb(items))


@admin_router.callback_query(F.data.startswith("admin:wd:"))
async def admin_withdrawal_view(call: CallbackQuery):
    wid = int(call.data.split(":")[2])
    w = await get_withdrawal(wid)
    if not w:
        await call.answer("Заявка не найдена", show_alert=True)
        return
    text = (
        f"📤 <b>Заявка #{w['id']}</b>\n\n👤 Клиент: <code>{w['login']}</code>\n"
        f"🌐 Сеть: <b>{w['network']}</b>\n👛 Кошелёк: <code>{w['wallet']}</code>\n"
        f"💵 Сумма: <b>${w['amount']}</b>\n📅 Создано: {w['created_at']}\n📊 Статус: <b>{w['status']}</b>"
    )
    await call.message.edit_text(text, reply_markup=withdrawal_review_kb(wid))


@admin_router.callback_query(F.data.startswith("wd:accept:"))
async def withdrawal_accept(call: CallbackQuery, bot: Bot):
    wid = int(call.data.split(":")[2])
    w = await get_withdrawal(wid)
    if not w or w["status"] != "pending":
        await call.answer("Заявка уже обработана", show_alert=True)
        return

    user = await get_user(w["tg_id"])
    if not user or user["crystals"] < w["amount"]:
        await call.answer("❌ У клиента не хватает средств", show_alert=True)
        return

    await add_crystals(w["tg_id"], -w["amount"])
    await finish_withdrawal(wid, "accepted")
    
    updated_user = await get_user(w["tg_id"])

    try:
        await bot.send_message(w["tg_id"], f"✅ <b>Заявка #{wid} принята</b>\n\n💵 Списано: <b>${w['amount']}</b>\n👛 Кошелёк: <code>{w['wallet']}</code>", reply_markup=client_menu_kb(updated_user))
    except Exception:
        pass

    await call.message.edit_text(f"✅ Заявка #{wid} принята, средства списаны.", reply_markup=back_to_admin_kb())


@admin_router.callback_query(F.data.startswith("wd:reject:"))
async def withdrawal_reject(call: CallbackQuery, bot: Bot):
    wid = int(call.data.split(":")[2])
    w = await get_withdrawal(wid)
    if not w or w["status"] != "pending":
        await call.answer("Заявка уже обработана", show_alert=True)
        return

    await finish_withdrawal(wid, "rejected")
    user = await get_user(w["tg_id"])

    try:
        await bot.send_message(w["tg_id"], f"❌ <b>Заявка #{wid} отклонена</b>\n\n💵 Сумма не списана: <b>${w['amount']}</b>\nСвяжись с поддержкой: @tradingbot2027", reply_markup=client_menu_kb(user))
    except Exception:
        pass

    await call.message.edit_text(f"❌ Заявка #{wid} отклонена.", reply_markup=back_to_admin_kb())


# ==================== АВТО-РАССЫЛКА И НАЧИСЛЕНИЕ ПРИБЫЛИ ====================
async def hourly_trading_loop(bot: Bot):
    await asyncio.sleep(10)
    while True:
        # Берем только тех, у кого включена торговля и есть баланс
        users = await get_active_traders()
        for u in users:
            bal = u['crystals']
            trades_data, total_profit = generate_trades_data(bal)
            
            # 1. СОХРАНЯЕМ СДЕЛКИ В ИСТОРИЮ
            trades_text = ""
            for t in trades_data:
                await save_trade(u['tg_id'], t['coin'], t['direction'], t['profit'])
                trades_text += f"🔹 {t['coin']} | {t['direction']}: <b>+${t['profit']}</b>\n"
            
            # 2. РЕАЛЬНО НАЧИСЛЯЕМ ПРИБЫЛЬ НА БАЛАНС
            await add_crystals(u['tg_id'], total_profit)
            updated_user = await get_user(u['tg_id'])
            
            now_time = datetime.now().strftime("%H:%M")
            text = (
                f"⏰ <b>Ежечасный отчет по торговле ({now_time})</b>\n\n"
                f"Наш ИИ-бот успешно отработал сессию на фьючерсах.\n\n"
                f"📊 <b>Результаты сессии:</b>\n"
                f"{trades_text}\n"
                f"💰 <b>Итого профит: +${total_profit}</b>\n\n"
                f"💵 <b>Ваш новый баланс:</b> <b>${updated_user['crystals']}</b>\n"
                f"<i>(Средства уже добавлены в ваш профиль)</i>"
            )
            try:
                # ПРИКРЕПЛЯЕМ МЕНЮ
                await bot.send_message(u['tg_id'], text, reply_markup=client_menu_kb(updated_user))
            except Exception:
                pass
            await asyncio.sleep(0.1)
        await asyncio.sleep(3600)


# ==================== LAUNCH ====================
async def main():
    logging.basicConfig(level=logging.INFO)
    await init_db()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(common_router)
    dp.include_router(admin_router)
    dp.include_router(client_router)

    asyncio.create_task(hourly_trading_loop(bot))

    await bot.delete_webhook(drop_pending_updates=True)
    print("Бот запущен ✅")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())