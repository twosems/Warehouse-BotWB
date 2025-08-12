# handlers/stocks.py
from aiogram import Dispatcher, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func, and_
from sqlalchemy.orm import aliased

from database.db import get_session
from database.models import User, Warehouse, Product, StockMovement
from handlers.common import send_content
from keyboards.inline import warehouses_kb, products_page_kb


PAGE_SIZE_STOCKS = 15


class StockReportState(StatesGroup):
    warehouse_selected = State()  # Храним wh_id и wh_name
    choosing_article = State()


def kb_stocks_root():
    kb = [[types.InlineKeyboardButton(text="📦 Просмотр остатков", callback_data="stocks_view")]]
    kb.append([types.InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")])
    return types.InlineKeyboardMarkup(inline_keyboard=kb)


async def stocks_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()
    await send_content(cb, "Остатки товара на складах — выберите действие:",
                       reply_markup=kb_stocks_root())


def kb_report_type():
    """Клавиатура выбора типа отчета с кнопкой назад."""
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📊 Отчет по всем товарам", callback_data="report_all")],
        [types.InlineKeyboardButton(text="🔍 Отчет по артикулу", callback_data="report_article")],
        [types.InlineKeyboardButton(text="⬅️ Назад к складам", callback_data="stocks_back_to_wh")],
    ])


def split_message(text: str, max_len: int = 4000) -> list[str]:
    """Разбивает длинное сообщение на части по строкам, с запасом."""
    parts = []
    while len(text) > max_len:
        split_at = text.rfind('\n', 0, max_len)
        if split_at == -1:
            split_at = max_len
        parts.append(text[:split_at].strip())
        text = text[split_at:].strip()
    if text:
        parts.append(text)
    return parts


# ===== Просмотр остатков: выбор склада =====
async def stocks_view(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()

    async with get_session() as session:
        res = await session.execute(
            select(Warehouse).where(Warehouse.is_active == True).order_by(Warehouse.name)
        )
        warehouses = res.scalars().all()
    if not warehouses:
        await send_content(cb, "🚫 Нет активных складов.")
        return

    await send_content(cb, "🏬 Выберите склад для просмотра остатков:",
                       reply_markup=warehouses_kb(warehouses, prefix="pr_wh"))
#                      reply_markup=warehouses_kb(warehouses))


# ===== Выбор склада для просмотра -> меню типа отчета =====
async def pick_warehouse_for_view(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("pr_wh:"):
        return
    await cb.answer()

    wh_id = int(cb.data.split(":")[1])
    async with get_session() as session:
        warehouse = await session.get(Warehouse, wh_id)
        if not warehouse or not warehouse.is_active:
            await send_content(cb, "🚫 Склад не найден или неактивен.")
            return

    await state.set_state(StockReportState.warehouse_selected)
    await state.update_data(wh_id=wh_id, wh_name=warehouse.name)
    await send_content(cb, f"🏬 Склад: *{warehouse.name}*. Выберите тип отчета:",
                       reply_markup=kb_report_type())


# ===== Отчет по всем товарам =====
async def report_all(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    wh_id = data.get('wh_id')
    wh_name = data.get('wh_name')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    SM = aliased(StockMovement)
    async with get_session() as session:
        res = await session.execute(
            select(
                Product.article,
                Product.name,
                func.sum(SM.qty).label("balance")
            )
            .join(SM, and_(SM.product_id == Product.id, SM.warehouse_id == wh_id))
            .where(Product.is_active == True)
            .group_by(Product.id)
            .having(func.sum(SM.qty) > 0)
            .order_by(Product.article)
        )
        rows = res.all()

    if not rows:
        await send_content(cb, f"📉 На складе *{wh_name}* нет товаров с остатками > 0.")
        return

    total_items = len(rows)
    total_balance = sum(row.balance for row in rows)
    lines = [f"🔹 `{row.article}` - *{row.name}*: **{row.balance}** шт." for row in rows]
    text = (
            f"📊 **Остатки на складе {wh_name}** (товары с остатком > 0):\n\n"
            + "\n\n".join(lines) + f"\n\n📈 **Итого:** {total_items} товаров, суммарный остаток: **{total_balance}** шт."
    )
    parts = split_message(text)

    for i, part in enumerate(parts, 1):
        if len(parts) > 1:
            part = f"Часть {i}/{len(parts)}:\n\n{part}"
        await cb.message.answer(part, parse_mode="Markdown")

    # Кнопка назад
    kb_back = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к типам отчета", callback_data="back_to_report_type")],
    ])
    await cb.message.answer("Выберите дальнейшее действие:", reply_markup=kb_back)


# ===== Отчет по артикулу: показ пагинированного списка =====
async def report_article(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.set_state(StockReportState.choosing_article)
    await report_articles_page(cb, user, state, page=1)


async def report_articles_page(cb: types.CallbackQuery, user: User, state: FSMContext, page: int = 1):
    data = await state.get_data()
    wh_id = data.get('wh_id')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    async with get_session() as session:
        # Total: только товары с balance > 0
        subq = select(Product.id).join(StockMovement, StockMovement.product_id == Product.id).where(
            Product.is_active == True, StockMovement.warehouse_id == wh_id
        ).group_by(Product.id).having(func.sum(StockMovement.qty) > 0).subquery()
        total_stmt = select(func.count()).select_from(subq)
        total = await session.scalar(total_stmt)

        # Список: товары с balance > 0
        res = await session.execute(
            select(Product)
            .join(StockMovement, StockMovement.product_id == Product.id)
            .where(Product.is_active == True, StockMovement.warehouse_id == wh_id)
            .group_by(Product.id)
            .having(func.sum(StockMovement.qty) > 0)
            .order_by(Product.article)
            .offset((page - 1) * PAGE_SIZE_STOCKS)
            .limit(PAGE_SIZE_STOCKS)
        )
        products = res.scalars().all()

    if not products:
        await send_content(cb, "📉 Нет товаров с остатками > 0.")
        return

    # Клавиатура без "Отмена", с "Назад"
    kb = products_page_kb(products, page, PAGE_SIZE_STOCKS, total, back_to="back_to_report_type")
    # Подменяем callback_data и убираем "Отмена"
    for row in kb.inline_keyboard:
        for btn in row[:]:  # Копируем список для безопасного удаления
            if btn.callback_data and btn.callback_data.startswith("pr_prod:"):
                btn.callback_data = btn.callback_data.replace("pr_prod:", "report_art:")
            elif btn.callback_data and btn.callback_data.startswith("pr_prod_page:"):
                btn.callback_data = btn.callback_data.replace("pr_prod_page:", "report_art_page:")
            elif btn.text == "❌ Отмена":
                row.remove(btn)

    await send_content(cb, "🔍 Выберите артикул для отчета:", reply_markup=kb)


# ===== Выбор артикула -> показ остатка =====
async def pick_article(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("report_art:"):
        return
    await cb.answer()

    product_id = int(cb.data.split(":")[1])
    data = await state.get_data()
    wh_id = data.get('wh_id')
    wh_name = data.get('wh_name')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    SM = aliased(StockMovement)
    async with get_session() as session:
        product = await session.get(Product, product_id)
        if not product or not product.is_active:
            await send_content(cb, "🚫 Товар не найден или неактивен.")
            return

        balance = await session.scalar(
            select(func.coalesce(func.sum(SM.qty), 0))
            .where(SM.product_id == product_id, SM.warehouse_id == wh_id)
        )

    text = (
        f"📊 **Остаток на складе {wh_name}**\n\n"
        f"🔹 Артикул: `{product.article}`\n"
        f"📦 Товар: *{product.name}*\n"
        f"➡️ Остаток: **{balance}** шт."
    )

    await send_content(cb, text, parse_mode="Markdown")

    # Кнопка назад
    kb_back = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к артикулам", callback_data="report_article")],
    ])
    await cb.message.answer("Выберите дальнейшее действие:", reply_markup=kb_back)


# ===== Назад к типу отчета =====
async def back_to_report_type(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    wh_name = data.get('wh_name', 'неизвестен')
    await send_content(cb, f"🏬 Склад: *{wh_name}*. Выберите тип отчета:",
                       reply_markup=kb_report_type())


# ===== Пагинация для артикулов =====
async def report_articles_page_handler(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("report_art_page:"):
        return
    page = int(cb.data.split(":")[1])
    await report_articles_page(cb, user, state, page=page)


# ===== Назад к складам =====
async def back_to_warehouses(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()
    await stocks_view(cb, user, state)  # Возвращаемся к выбору склада


def register_stocks_handlers(dp: Dispatcher):
    dp.callback_query.register(stocks_root, lambda c: c.data == "stocks")
    dp.callback_query.register(stocks_view, lambda c: c.data == "stocks_view")

    # Флоу для просмотра/отчета
    dp.callback_query.register(pick_warehouse_for_view, lambda c: c.data.startswith("pr_wh:"))
    dp.callback_query.register(report_all, lambda c: c.data == "report_all")
    dp.callback_query.register(report_article, lambda c: c.data == "report_article")
    dp.callback_query.register(report_articles_page_handler, lambda c: c.data.startswith("report_art_page:"))
    dp.callback_query.register(pick_article, lambda c: c.data.startswith("report_art:"))
    dp.callback_query.register(back_to_report_type, lambda c: c.data == "back_to_report_type")
    dp.callback_query.register(back_to_warehouses, lambda c: c.data == "stocks_back_to_wh")