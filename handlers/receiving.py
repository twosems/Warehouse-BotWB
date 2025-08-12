# handlers/receiving.py
from aiogram import Dispatcher, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func, desc

from database.db import get_session
from database.models import User, Warehouse, Product, StockMovement, MovementType
from keyboards.inline import (
    warehouses_kb, products_page_kb, qty_kb, comment_kb, receiving_confirm_kb
)
from handlers.common import send_content
from utils.validators import validate_positive_int
from utils.pagination import build_pagination_keyboard


class IncomingState(StatesGroup):
    choosing_warehouse = State()
    choosing_product = State()
    entering_qty = State()
    entering_comment = State()
    confirming = State()

class ReceivingViewState(StatesGroup):
    viewing_docs = State()


PAGE_SIZE_PRODUCTS = 10
PAGE_SIZE_DOCS = 10


def kb_receiving_root():
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📄 Просмотреть документы", callback_data="view_docs")],
        [types.InlineKeyboardButton(text="➕ Добавить документ", callback_data="add_doc")],
        [types.InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")],
    ])


# ===== Корневое меню для "Поступление" =====
async def receiving_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()
    await send_content(cb, "Поступление товара: выберите действие", reply_markup=kb_receiving_root())


# ===== Просмотреть документы =====
async def view_docs(cb: types.CallbackQuery, user: User, state: FSMContext, page: int = 1):
    await cb.answer()
    await state.set_state(ReceivingViewState.viewing_docs)

    async with get_session() as session:
        # Получаем уникальные документы для prihod
        total_stmt = select(func.count(func.distinct(StockMovement.doc_id))).where(StockMovement.type == MovementType.prihod)
        total = await session.scalar(total_stmt)

        res = await session.execute(
            select(StockMovement.doc_id, func.min(StockMovement.date).label("date"))
            .where(StockMovement.type == MovementType.prihod)
            .group_by(StockMovement.doc_id)
            .order_by(desc("date"))
            .offset((page - 1) * PAGE_SIZE_DOCS)
            .limit(PAGE_SIZE_DOCS)
        )
        docs = res.all()

    if not docs:
        await send_content(cb, "Документов по поступлению нет.", reply_markup=kb_receiving_root())
        return

    rows = []
    for doc in docs:
        date_str = doc.date.strftime('%Y-%m-%d %H:%M')
 #       rows.append([types.InlineKeyboardButton(text=f"Документ №{doc.doc_id} от {date_str}", callback_data=f"view_doc:{doc.doc_id}")])
        rows.append([types.InlineKeyboardButton(
            text=f"Документ №{doc.doc_id} от {date_str}",
            callback_data=f"view_doc:{doc.doc_id}"
        )])

    pag_row = build_pagination_keyboard(
        page=page,
        page_size=PAGE_SIZE_DOCS,
        total=total,
        prev_cb_prefix="view_docs_page",
        next_cb_prefix="view_docs_page",
        prev_text="◀ Предыдущая",
        next_text="Следующая ▶"
    )
    if pag_row:
        rows.append(pag_row)

    rows.append([types.InlineKeyboardButton(text="⬅️ Назад", callback_data="receiving")])

    kb = types.InlineKeyboardMarkup(inline_keyboard=rows)
    await send_content(cb, "Документы по поступлению:", reply_markup=kb)


async def view_docs_page(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    try:
        _, page_str = cb.data.split(":")
        page = int(page_str)
    except Exception:
        page = 1
    await view_docs(cb, user, state, page=page)


# ===== Просмотр конкретного документа =====
async def view_doc(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    try:
        _, doc_id_str = cb.data.split(":")
        doc_id = int(doc_id_str)
    except Exception:
        await cb.answer("Неверные данные.", show_alert=True)
        return

    async with get_session() as session:
        res = await session.execute(
            select(StockMovement, Warehouse, Product, User)
            .join(Warehouse, Warehouse.id == StockMovement.warehouse_id)
            .join(Product, Product.id == StockMovement.product_id)
            .join(User, User.id == StockMovement.user_id)
            .where(StockMovement.doc_id == doc_id, StockMovement.type == MovementType.prihod)
        )
        movements = res.all()

    if not movements:
        await send_content(cb, "Документ не найден.")
        return

    # Предполагаем, что документ может иметь несколько строк (товаров)
    text = f"📑 **Документ №{doc_id} от {movements[0].StockMovement.date.strftime('%Y-%m-%d %H:%M:%S')}**\n\n"
    for mv, wh, prod, usr in movements:
        text += (
            f"🏬 Склад: *{wh.name}*\n"
            f"📦 Товар: *{prod.name}* (арт. {prod.article})\n"
            f"➡️ Количество: *{mv.qty}* шт.\n"
            f"💬 Комментарий: {mv.comment or '—'}\n"
            f"👤 Создал: *{usr.name}*\n\n"
        )

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к документам", callback_data="view_docs")],
    ])
    await send_content(cb, text.strip(), reply_markup=kb, parse_mode="Markdown")


# ===== Добавить документ (текущий флоу) =====
async def add_doc(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    async with get_session() as session:
        res = await session.execute(
            select(Warehouse).where(Warehouse.is_active == True).order_by(Warehouse.name)
        )
        warehouses = res.scalars().all()
    if not warehouses:
        await send_content(cb, "🚫 Нет активных складов. Обратитесь к администратору.")
        await state.clear()
        return

    await state.clear()
    await state.set_state(IncomingState.choosing_warehouse)
    await send_content(cb, "🏬 Выберите склад для поступления товара:",
                       reply_markup=warehouses_kb(warehouses))


# ===== Выбор склада -> сразу список активных товаров =====
async def pick_warehouse(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if not cb.data.startswith("rcv_wh:"):
        return

    wh_id = int(cb.data.split(":")[1])
    async with get_session() as session:
        warehouse = (await session.execute(
            select(Warehouse).where(Warehouse.id == wh_id, Warehouse.is_active == True)
        )).scalar()
    if not warehouse:
        await cb.message.answer("🚫 Склад не найден или неактивен.")
        return

    await state.update_data(warehouse_id=warehouse.id, warehouse_name=warehouse.name)
    await list_products(cb, user, state, page=1)  # Переход к выбору товара


# ===== Назад к выбору склада =====
async def back_to_warehouses(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    async with get_session() as session:
        res = await session.execute(
            select(Warehouse).where(Warehouse.is_active == True).order_by(Warehouse.name)
        )
        warehouses = res.scalars().all()
    await state.set_state(IncomingState.choosing_warehouse)
    await send_content(cb, "🏬 Выберите склад:", reply_markup=warehouses_kb(warehouses))


# ===== Список товаров с пагинацией =====
async def list_products(cb: types.CallbackQuery, user: User, state: FSMContext, page: int = 1):
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
        await send_content(cb, "🚫 Активных товаров нет. Попросите администратора добавить товар.",
                           reply_markup=warehouses_kb([]))
        return

    await state.set_state(IncomingState.choosing_product)
    await send_content(
        cb,
        "📦 Выберите товар:",
        reply_markup=products_page_kb(products, page, PAGE_SIZE_PRODUCTS, total, back_to="rcv_back_wh")
    )


async def products_page(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if not cb.data.startswith("rcv_prod_page:"):
        return
    try:
        _, page_str = cb.data.split(":")
        page = int(page_str)
    except Exception:
        page = 1
    await list_products(cb, user, state, page=page)


# ===== Выбор товара из списка =====
async def pick_product(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if not cb.data.startswith("rcv_prod:"):
        return
    try:
        _, pid_str = cb.data.split(":")
        pid = int(pid_str)
    except Exception:
        await cb.answer("🚫 Неверные данные.", show_alert=True)
        return

    async with get_session() as session:
        product = (await session.execute(
            select(Product).where(Product.id == pid, Product.is_active == True)
        )).scalar()

    if not product:
        await cb.answer("🚫 Товар не найден/неактивен.", show_alert=True)
        return

    await state.update_data(product_id=product.id, product_article=product.article, product_name=product.name)
    await state.set_state(IncomingState.entering_qty)
    await send_content(
        cb,
        f"📦 Товар: *{product.name}* (арт. {product.article})\n\n➡️ Введите количество (>0):",
        reply_markup=qty_kb(back_to="rcv_back_products")
    )


# ===== Назад к списку товаров =====
async def back_to_products(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await list_products(cb, user, state, page=1)


# ===== Количество =====
async def enter_qty(message: types.Message, user: User, state: FSMContext):
    txt = (message.text or "").strip()
    try:
        qty = int(txt)
    except Exception:
        await message.answer("🚫 Нужно целое число. Введите ещё раз:",
                             reply_markup=qty_kb(back_to="rcv_back_products"))
        return
    if not validate_positive_int(qty):
        await message.answer("🚫 Количество должно быть > 0. Введите ещё раз:",
                             reply_markup=qty_kb(back_to="rcv_back_products"))
        return

    await state.update_data(qty=qty)
    await state.set_state(IncomingState.entering_comment)
    await message.answer(
        "💬 Комментарий (или нажмите «Пропустить»):",
        reply_markup=comment_kb(back_to="rcv_back_qty")
    )


# ===== Комментарий =====
async def skip_comment(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    await state.update_data(comment="")
    await state.set_state(IncomingState.confirming)
    await send_content(
        cb,
        confirm_text(data),
        reply_markup=receiving_confirm_kb(confirm_prefix="rcv", back_to="rcv_back_comment")
    )


async def back_to_qty(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    await state.set_state(IncomingState.entering_qty)
    await send_content(
        cb,
        f"📦 Товар: *{data['product_name']}* (арт. {data['product_article']})\n\n➡️ Введите количество (>0):",
        reply_markup=qty_kb(back_to="rcv_back_products")
    )


async def set_comment(message: types.Message, user: User, state: FSMContext):
    comment = (message.text or "").strip()
    if comment == "-":
        comment = ""
    data = await state.get_data()
    await state.update_data(comment=comment)
    await state.set_state(IncomingState.confirming)
    await message.answer(
        confirm_text({**data, "comment": comment}),
        parse_mode="Markdown",
        reply_markup=receiving_confirm_kb(confirm_prefix="rcv", back_to="rcv_back_comment")
    )


def confirm_text(data: dict) -> str:
    return (
        "📑 **Подтвердите поступление:**\n\n"
        f"🏬 Склад: *{data['warehouse_name']}*\n"
        f"📦 Товар: *{data['product_name']}* (арт. {data['product_article']})\n"
        f"➡️ Количество: *{data['qty']}*\n"
        f"💬 Комментарий: {data.get('comment') or '—'}\n"
    )


async def back_to_comment(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.set_state(IncomingState.entering_comment)
    await send_content(cb, "💬 Комментарий (или нажмите «Пропустить»):",
                       reply_markup=comment_kb(back_to="rcv_back_qty"))


# ===== Отмена в любом месте =====
async def cancel_flow(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer("🚫 Отмена")
    await state.clear()
    await send_content(cb, "Операция отменена.", reply_markup=kb_receiving_root())


# ===== Подтверждение и запись =====
async def confirm(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if cb.data not in ("rcv_confirm", "rcv_cancel"):
        return
    if cb.data == "rcv_cancel":
        await state.clear()
        await send_content(cb, "🚫 Операция отменена.", reply_markup=kb_receiving_root())
        return

    data = await state.get_data()
    async with get_session() as session:
        max_doc = (await session.execute(
            select(func.max(StockMovement.doc_id)).where(StockMovement.type == MovementType.prihod)
        )).scalar()
        next_doc = (max_doc or 0) + 1

        sm = StockMovement(
            warehouse_id=data["warehouse_id"],
            product_id=data["product_id"],
            qty=data["qty"],
            type=MovementType.prihod,
            user_id=user.id,
            doc_id=next_doc,
            comment=data.get("comment", ""),
        )
        session.add(sm)
        await session.commit()
        await session.refresh(sm)

    await state.clear()
    done = (
        f"✅ Поступление записано.\n\n"
        f"📑 Документ № *{sm.doc_id}*\n"
        f"📅 Дата: *{sm.date.strftime('%Y-%m-%d %H:%M:%S')}*\n"
        f"🏬 Склад: *{data['warehouse_name']}*\n"
        f"📦 Товар: *{data['product_name']}* (арт. {data['product_article']})\n"
        f"➡️ Количество: *{data['qty']}*\n"
        f"💬 Комментарий: {data.get('comment') or '—'}"
    )
    await send_content(cb, done, reply_markup=kb_receiving_root())


def register_receiving_handlers(dp: Dispatcher):
    # Корень
    dp.callback_query.register(receiving_root, lambda c: c.data == "receiving")

    # Просмотр документов
    dp.callback_query.register(view_docs, lambda c: c.data == "view_docs")
    dp.callback_query.register(view_docs_page, lambda c: c.data.startswith("view_docs_page:"))
    dp.callback_query.register(view_doc, lambda c: c.data.startswith("view_doc:"))

    # Добавить документ
    dp.callback_query.register(add_doc, lambda c: c.data == "add_doc")

    # Склад -> сразу товары
    dp.callback_query.register(pick_warehouse, lambda c: c.data.startswith("rcv_wh:"))
    dp.callback_query.register(back_to_warehouses, lambda c: c.data == "rcv_back_wh")

    # Товары и пагинация
    dp.callback_query.register(products_page, lambda c: c.data.startswith("rcv_prod_page:"))
    dp.callback_query.register(pick_product, lambda c: c.data.startswith("rcv_prod:"))
    dp.callback_query.register(back_to_products, lambda c: c.data == "rcv_back_products")

    # Комментарий/Qty/Отмена/Назад
    dp.callback_query.register(skip_comment, lambda c: c.data == "rcv_skip_comment")
    dp.callback_query.register(back_to_qty, lambda c: c.data == "rcv_back_qty")
    dp.callback_query.register(back_to_comment, lambda c: c.data == "rcv_back_comment")
    dp.callback_query.register(cancel_flow, lambda c: c.data == "rcv_cancel")

    # Вводы
    dp.message.register(enter_qty, IncomingState.entering_qty)
    dp.message.register(set_comment, IncomingState.entering_comment)

    # Подтверждение
    dp.callback_query.register(confirm, IncomingState.confirming)