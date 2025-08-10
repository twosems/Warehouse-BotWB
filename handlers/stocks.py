# handlers/stocks.py
from aiogram import Dispatcher, types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select, func

from database.db import get_session
from database.models import User, UserRole, Warehouse, Product, StockMovement
from handlers.common import send_content

def kb_stocks_root(is_admin: bool) -> InlineKeyboardMarkup:
    kb = [[InlineKeyboardButton(text="Просмотр остатков", callback_data="stocks_view")]]
    if is_admin:
        kb.append([InlineKeyboardButton(text="Корректировка остатков", callback_data="stocks_adjust")])
    kb.append([InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def stocks_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()
    await send_content(cb, "Остатки товара на складах — выберите действие:",
                       reply_markup=kb_stocks_root(user.role == UserRole.admin))

# ===== Просмотр остатков (упрощённо: свод по товарам и складам) =====
async def stocks_view(cb: types.CallbackQuery, user: User):
    await cb.answer()
    # примитивный расчёт остатков: SUM(prihod) - SUM(postavka) + SUM(korrekt)
    async with get_session() as session:
        # просто покажем количество движений как заглушку
        count_mov = (await session.execute(select(func.count()).select_from(StockMovement))).scalar_one()
    txt = f"Здесь будет отчёт по остаткам.\nПока что движений в базе: {count_mov}."
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="stocks")]])
    await send_content(cb, txt, reply_markup=kb)

# ===== Корректировка (только админ) — заглушка FSM =====
async def stocks_adjust(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Требуется доступ администратора.", show_alert=True); return
    await cb.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="(заглушка) Начать корректировку", callback_data="stocks_adjust_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="stocks")]
    ])
    await send_content(cb, "Корректировка остатков (требуется админ).", reply_markup=kb)

def register_stocks_handlers(dp: Dispatcher):
    dp.callback_query.register(stocks_root,   lambda c: c.data == "stocks")
    dp.callback_query.register(stocks_view,   lambda c: c.data == "stocks_view")
    dp.callback_query.register(stocks_adjust, lambda c: c.data == "stocks_adjust")
