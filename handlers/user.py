# handlers/user.py
from aiogram import Dispatcher, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func

from database.db import get_session
from database.models import User, Warehouse, Product, StockMovement, MovementType
from keyboards.inline import (
    warehouses_kb, confirm_kb, incoming_mode_kb, products_page_kb
)
from handlers.common import send_content
from utils.validators import validate_positive_int


class IncomingState(StatesGroup):
    choosing_warehouse = State()
    entering_article = State()
    entering_qty = State()
    entering_comment = State()
    confirming = State()


PAGE_SIZE_PRODUCTS = 10


# ===== Старт прихода =====
async def start_incoming(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    async with get_session() as session:
        res = await session.execute(select(Warehouse).where(Warehouse.is_active == True).order_by(Warehouse.name))
        warehouses = res.scalars().all()
    if not warehouses:
        await send_content(cb, "Нет активных складов. Обратитесь к администратору.")
        await state.clear()
        return
    await state.clear()
    await state.set_state(IncomingState.choosing_warehouse)
    await send_content(cb, "Выберите склад для прихода:", reply_markup=warehouses_kb(warehouses))


# ===== Выбор склада -> режим =====
async def incoming_pick_warehouse(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if not cb.data.startswith("pr_wh:"):
        return
    wh_id = int(cb.data.split(":")[1])
    async with get_session() as session:
        wh_q = await session.execute(select(Warehouse).where(Warehouse.id == wh_id, Warehouse.is_active == True))
        warehouse = wh_q.scalar()
    if not warehouse:
        await cb.message.answer("Склад не найден или неактивен.")
        return
    await state.update_data(warehouse_id=warehouse.id, warehouse_name=warehouse.name)
    await send_content(cb, f"*{warehouse.name}*\nВыберите режим:", reply_markup=incoming_mode_kb())


# ===== Режимы выбора товара =====
async def incoming_mode(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await send_content(cb, "Режим выбора товара:", reply_markup=incoming_mode_kb())


async def incoming_mode_enter(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.set_state(IncomingState.entering_article)
    await send_content(cb, "Введите **артикул** товара:", reply_markup=None)


async def incoming_mode_choose(cb: types.CallbackQuery, user: User, state: FSMContext, page: int = 1):
    await cb.answer()
    async with get_session() as session:
        total = (await session.execute(
            select(func.count()).select_from(Product).where(Product.is_active == True)
        )).scalar_one()
        res = await session.execute(
            select(Product)
            .where(Product.is_active == True)
            .order_by(Product.name)
            .offset((page - 1) * PAGE_SIZE_PRODUCTS)
            .limit(PAGE_SIZE_PRODUCTS)
        )
        products = res.scalars().all()

    if not products:
        await send_content(cb, "Активных товаров нет. Попросите администратора добавить товар.")
        return

    await send_content(cb, "Выберите товар:", reply_markup=products_page_kb(products, page, PAGE_SIZE_PRODUCTS, total))


async def incoming_products_page(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    try:
        _, page_str = cb.data.split(":")
        page = int(page_str)
    except Exception:
        page = 1
    await incoming_mode_choose(cb, user, state, page=page)


async def incoming_pick_product(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    try:
        _, pid_str = cb.data.split(":")
        pid = int(pid_str)
    except Exception:
        await cb.answer("Неверные данные.", show_alert=True)
        return

    async with get_session() as session:
        p_q = await session.execute(select(Product).where(Product.id == pid, Product.is_active == True))
        product = p_q.scalar()

    if not product:
        await cb.answer("Товар не найден/неактивен.", show_alert=True)
        return

    await state.update_data(product_id=product.id, product_article=product.article, product_name=product.name)
    await state.set_state(IncomingState.entering_qty)
    await send_content(cb, f"Товар: *{product.name}* (арт. {product.article})\n\nВведите количество (>0):")


# ===== Ручной ввод артикула =====
async def incoming_enter_article(message: types.Message, user: User, state: FSMContext):
    article = (message.text or "").strip()
    if not article:
        await message.answer("Артикул пустой. Введите ещё раз.")
        return

    async with get_session() as session:
        p_q = await session.execute(select(Product).where(Product.article == article))
        product = p_q.scalar()
        if not product:
            product = Product(article=article, name=article, is_active=True)
            session.add(product)
            await session.commit()

    await state.update_data(product_id=product.id, product_article=article, product_name=product.name)
    await state.set_state(IncomingState.entering_qty)
    await message.answer(f"Товар: *{product.name}* (арт. {product.article})\n\nВведите количество (>0):", parse_mode="Markdown")


# ===== Количество / Комментарий / Подтверждение =====
async def incoming_enter_qty(message: types.Message, user: User, state: FSMContext):
    txt = (message.text or "").strip()
    try:
        qty = int(txt)
    except Exception:
        await message.answer("Нужно целое число. Введите ещё раз:")
        return
    if not validate_positive_int(qty):
        await message.answer("Количество должно быть > 0. Введите ещё раз:")
        return

    await state.update_data(qty=qty)
    await state.set_state(IncomingState.entering_comment)
    await message.answer("Комментарий (или «-», чтобы пропустить):")


async def incoming_enter_comment(message: types.Message, user: User, state: FSMContext):
    comment = (message.text or "").strip()
    if comment == "-":
        comment = ""

    data = await state.get_data()
    wh = data["warehouse_name"]
    prod_name = data["product_name"]
    prod_article = data["product_article"]
    qty = data["qty"]

    await state.update_data(comment=comment)
    text = (
        "Подтвердите приход:\n\n"
        f"Склад: *{wh}*\n"
        f"Товар: *{prod_name}* (арт. {prod_article})\n"
        f"Количество: *{qty}*\n"
        f"Комментарий: {comment or '—'}\n"
    )
    await state.set_state(IncomingState.confirming)
    await message.answer(text, parse_mode="Markdown", reply_markup=confirm_kb(prefix="pr"))


async def incoming_confirm(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if cb.data not in ("pr_confirm", "pr_cancel"):
        return
    if cb.data == "pr_cancel":
        await state.clear()
        await send_content(cb, "Операция отменена.")
        return

    data = await state.get_data()
    async with get_session() as session:
        sm = StockMovement(
            warehouse_id=data["warehouse_id"],
            product_id=data["product_id"],
            qty=data["qty"],
            type=MovementType.prihod,
            user_id=user.id,
            comment=data.get("comment", ""),
        )
        session.add(sm)
        await session.commit()

    await state.clear()
    await send_content(cb, "✅ Приход записан.")


def register_user_handlers(dp: Dispatcher):
    # Старт
    dp.callback_query.register(start_incoming,       lambda c: c.data == "prihod")

    # Склад -> режим
    dp.callback_query.register(incoming_pick_warehouse, lambda c: c.data.startswith("pr_wh:"))

    # Режимы
    dp.callback_query.register(incoming_mode,        lambda c: c.data == "pr_mode")
    dp.callback_query.register(incoming_mode_enter,  lambda c: c.data == "pr_mode_enter")
    dp.callback_query.register(incoming_mode_choose, lambda c: c.data == "pr_mode_choose")
    dp.callback_query.register(incoming_products_page, lambda c: c.data.startswith("pr_prod_page:"))
    dp.callback_query.register(incoming_pick_product,  lambda c: c.data.startswith("pr_prod:"))

    # Вводы
    dp.message.register(incoming_enter_article, IncomingState.entering_article)
    dp.message.register(incoming_enter_qty,     IncomingState.entering_qty)
    dp.message.register(incoming_enter_comment, IncomingState.entering_comment)

    # Подтверждение
    dp.callback_query.register(incoming_confirm, IncomingState.confirming)
