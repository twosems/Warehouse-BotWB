# handlers/reports.py
from aiogram import Dispatcher, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func, and_
from sqlalchemy.orm import aliased

from database.db import get_session
from database.models import User, Warehouse, Product, StockMovement, ProductStage
from handlers.common import send_content
from keyboards.inline import warehouses_kb, products_page_kb

PAGE_SIZE_REPORTS = 15


# ===== FSM =====
class ReportState(StatesGroup):
    warehouse_selected = State()  # держим wh_id и wh_name
    choosing_article = State()


# ===== Общие помощники =====
def split_message(text: str, max_len: int = 4000) -> list[str]:
    """Разбивает длинный текст по строкам, чтобы не упереться в лимит Телеграма."""
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


def kb_reports_root():
    """Корень раздела «Отчёты»."""
    kb = [
        [types.InlineKeyboardButton(text="📦 Остатки по складу", callback_data="rep_view")],
        # здесь в будущем можно добавить другие виды отчётов
    ]
    kb.append([types.InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")])
    return types.InlineKeyboardMarkup(inline_keyboard=kb)


def kb_report_type():
    """Клавиатура выбора типа отчёта внутри выбранного склада."""
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📊 Отчёт по всем товарам", callback_data="rep_all")],
        [types.InlineKeyboardButton(text="🎁 Упакованные остатки", callback_data="rep_packed")],
        [types.InlineKeyboardButton(text="🔍 Отчёт по артикулу", callback_data="rep_article")],
        [types.InlineKeyboardButton(text="⬅️ Назад к складам", callback_data="rep_back_to_wh")],
        [types.InlineKeyboardButton(text="⬅️ В раздел «Отчёты»", callback_data="reports")],
    ])


# ===== Корень «Отчёты» =====
async def reports_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()
    await send_content(
        cb,
        "Раздел «Отчёты». Что нужно показать?",
        reply_markup=kb_reports_root(),
    )


# ===== Просмотр остатков: выбор склада =====
async def rep_view(cb: types.CallbackQuery, user: User, state: FSMContext):
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

    await send_content(
        cb,
        "🏬 Выберите склад для отчёта по остаткам:",
        reply_markup=warehouses_kb(warehouses, prefix="rep_wh"),
    )


# ===== Выбор склада -> меню типа отчёта =====
async def rep_pick_warehouse(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("rep_wh:"):
        return
    await cb.answer()

    wh_id = int(cb.data.split(":")[1])
    async with get_session() as session:
        warehouse = await session.get(Warehouse, wh_id)
        if not warehouse or not warehouse.is_active:
            await send_content(cb, "🚫 Склад не найден или неактивен.")
            return

    await state.set_state(ReportState.warehouse_selected)
    await state.update_data(wh_id=wh_id, wh_name=warehouse.name)
    await send_content(
        cb,
        f"🏬 Склад: *{warehouse.name}*. Выберите тип отчёта:",
        reply_markup=kb_report_type(),
        parse_mode="Markdown",
    )


# ===== Отчёт по всем товарам =====
async def rep_all(cb: types.CallbackQuery, user: User, state: FSMContext):
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
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="rep_back_to_types")],
            [types.InlineKeyboardButton(text="🏬 Выбор склада", callback_data="rep_back_to_wh")],
        ])
        await send_content(
            cb,
            f"📉 На складе *{wh_name}* сейчас нет товаров с остатком.\n\n"
            f"Выберите другой тип отчёта или склад.",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    total_items = len(rows)
    total_balance = sum(row.balance for row in rows)
    lines = [f"🔹 `{row.article}` — *{row.name}*: **{row.balance}** шт." for row in rows]
    text = (
            f"📊 **Остатки на складе {wh_name}** — товары с остатком:\n\n"
            + "\n\n".join(lines)
            + f"\n\n📈 **Итого:** {total_items} товаров, суммарный остаток: **{total_balance}** шт."
    )
    for i, part in enumerate(split_message(text), 1):
        if len(split_message(text)) > 1:
            part = f"Часть {i}/{len(split_message(text))}:\n\n{part}"
        await cb.message.answer(part, parse_mode="Markdown")

    kb_back = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="rep_back_to_types")],
    ])
    await cb.message.answer("Выберите дальнейшее действие:", reply_markup=kb_back)


# ===== Отчёт об упакованных остатках =====
async def rep_packed(cb: types.CallbackQuery, user: User, state: FSMContext):
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
            .join(SM, and_(
                SM.product_id == Product.id,
                SM.warehouse_id == wh_id,
                SM.stage == ProductStage.packed
            ))
            .where(Product.is_active == True)
            .group_by(Product.id)
            .having(func.sum(SM.qty) > 0)
            .order_by(Product.article)
        )
        rows = res.all()

    if not rows:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="rep_back_to_types")],
            [types.InlineKeyboardButton(text="🏬 Выбор склада", callback_data="rep_back_to_wh")],
        ])
        await send_content(
            cb,
            f"📭 На складе *{wh_name}* нет упакованных остатков.",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    total_items = len(rows)
    total_balance = sum(row.balance for row in rows)
    lines = [f"🎁 `{row.article}` — *{row.name}*: **{row.balance}** шт." for row in rows]
    text = (
            f"🎁 **Упакованные остатки на складе {wh_name}**\n\n"
            + "\n\n".join(lines)
            + f"\n\n📈 **Итого:** {total_items} товаров, упаковано: **{total_balance}** шт."
    )
    for i, part in enumerate(split_message(text), 1):
        if len(split_message(text)) > 1:
            part = f"Часть {i}/{len(split_message(text))}:\n\n{part}"
        await cb.message.answer(part, parse_mode="Markdown")

    kb_back = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="rep_back_to_types")],
    ])
    await cb.message.answer("Выберите дальнейшее действие:", reply_markup=kb_back)


# ===== Отчёт по артикулу (список) =====
async def rep_article(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.set_state(ReportState.choosing_article)
    await rep_articles_page(cb, user, state, page=1)


async def rep_articles_page(cb: types.CallbackQuery, user: User, state: FSMContext, page: int = 1):
    data = await state.get_data()
    wh_id = data.get('wh_id')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    async with get_session() as session:
        # total продуктов с положительным балансом
        subq = (
            select(Product.id)
            .join(StockMovement, StockMovement.product_id == Product.id)
            .where(Product.is_active == True, StockMovement.warehouse_id == wh_id)
            .group_by(Product.id)
            .having(func.sum(StockMovement.qty) > 0)
            .subquery()
        )
        total = await session.scalar(select(func.count()).select_from(subq))

        # текущая страница
        res = await session.execute(
            select(Product)
            .join(StockMovement, StockMovement.product_id == Product.id)
            .where(Product.is_active == True, StockMovement.warehouse_id == wh_id)
            .group_by(Product.id)
            .having(func.sum(StockMovement.qty) > 0)
            .order_by(Product.article)
            .offset((page - 1) * PAGE_SIZE_REPORTS)
            .limit(PAGE_SIZE_REPORTS)
        )
        products = res.scalars().all()

    if not products:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="rep_back_to_types")],
            [types.InlineKeyboardButton(text="🏬 Выбор склада", callback_data="rep_back_to_wh")],
        ])
        await send_content(
            cb,
            "📉 На этом складе сейчас нет товаров с остатком.\n\n"
            "Вернитесь назад и выберите другой тип отчёта или склад.",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    kb = products_page_kb(
        products=products,
        page=page,
        page_size=PAGE_SIZE_REPORTS,
        total=total,
        back_to="rep_back_to_types",
        item_prefix="rep_art",
        page_prefix="rep_art_page",
    )

    await send_content(cb, "🔍 Выберите артикул для отчёта:", reply_markup=kb)


# ===== Выбор артикула -> остаток по нему =====
async def rep_pick_article(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("rep_art:"):
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

    kb_back = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к артикулам", callback_data="rep_article")],
    ])
    await cb.message.answer("Выберите дальнейшее действие:", reply_markup=kb_back)


# ===== Навигация назад =====
async def rep_back_to_types(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    wh_name = data.get('wh_name', 'неизвестен')
    await send_content(
        cb,
        f"🏬 Склад: *{wh_name}*. Выберите тип отчёта:",
        reply_markup=kb_report_type(),
        parse_mode="Markdown",
    )


async def rep_articles_page_handler(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("rep_art_page:"):
        return
    page = int(cb.data.split(":")[1])
    await rep_articles_page(cb, user, state, page=page)


async def rep_back_to_warehouses(cb: types.CallbackQuery, user: User, state: FSMContext):
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

    await send_content(
        cb,
        "🏬 Выберите склад:",
        reply_markup=warehouses_kb(warehouses, prefix="rep_wh"),
    )


# ===== Регистрация =====
def register_reports_handlers(dp: Dispatcher):
    # Корень раздела
    dp.callback_query.register(reports_root, lambda c: c.data == "reports")

    # Просмотр остатков (через отчёты)
    dp.callback_query.register(rep_view,           lambda c: c.data == "rep_view")
    dp.callback_query.register(rep_pick_warehouse, lambda c: c.data.startswith("rep_wh:"))

    # Типы отчётов
    dp.callback_query.register(rep_all,    lambda c: c.data == "rep_all")
    dp.callback_query.register(rep_packed, lambda c: c.data == "rep_packed")
    dp.callback_query.register(rep_article, lambda c: c.data == "rep_article")

    # Пагинация и выбор артикула
    dp.callback_query.register(rep_articles_page_handler, lambda c: c.data.startswith("rep_art_page:"))
    dp.callback_query.register(rep_pick_article,          lambda c: c.data.startswith("rep_art:"))

    # Навигация назад
    dp.callback_query.register(rep_back_to_types,      lambda c: c.data == "rep_back_to_types")
    dp.callback_query.register(rep_back_to_warehouses, lambda c: c.data == "rep_back_to_wh")
