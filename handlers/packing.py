# handlers/packing.py
from aiogram import Router, Dispatcher, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database.db import get_session
from database.models import (
    Warehouse, Product, StockMovement,
    MovementType, ProductStage, User
)

router = Router()
PAGE_SIZE = 10

# ---------- FSM ----------
class PackFSM(StatesGroup):
    WH = State()
    PRODUCTS = State()
    QTY = State()
    CONFIRM = State()

# ---------- Keyboards ----------
def kb_wh_list(warehouses, page=0) -> InlineKeyboardMarkup:
    start = page * PAGE_SIZE
    chunk = warehouses[start:start + PAGE_SIZE]
    rows = [[InlineKeyboardButton(text=name, callback_data=f"pack:wh:{wid}")]
            for wid, name in chunk]
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"pack:wh:page:{page-1}"))
    if start + PAGE_SIZE < len(warehouses):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"pack:wh:page:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="pack:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_products_list(products, page=0, wh_id: int = 0) -> InlineKeyboardMarkup:
    start = page * PAGE_SIZE
    chunk = products[start:start + PAGE_SIZE]
    rows = [[InlineKeyboardButton(
        text=f"{name} (art. {article}) — RAW {raw}",
        callback_data=f"pack:p:{wh_id}:{pid}"
    )] for pid, name, article, raw in chunk]
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"pack:p:page:{page-1}"))
    if start + PAGE_SIZE < len(products):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"pack:p:page:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ Склад", callback_data="pack:back_wh")])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="pack:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Провести упаковку", callback_data="pack:do")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="pack:cancel")],
    ])

# ---------- Helpers ----------
async def _warehouses_list() -> list[tuple[int, str]]:
    async with get_session() as s:
        rows = (await s.execute(
            select(Warehouse.id, Warehouse.name)
            .where((Warehouse.is_active.is_(True)) | (Warehouse.is_active.is_(None)))
            .order_by(Warehouse.name.asc())
        )).all()
        items = [(r[0], r[1]) for r in rows]
        # ренейм дублей
        counts = {}
        for _, n in items:
            counts[n] = counts.get(n, 0) + 1
        return [(wid, name if counts[name] == 1 else f"{name} (#{wid})") for wid, name in items]

async def _products_with_raw(session: AsyncSession, warehouse_id: int) -> list[tuple[int, str, str, int]]:
    sm = StockMovement
    p = Product
    raw_sum = select(
        sm.product_id.label("pid"),
        func.coalesce(func.sum(sm.qty), 0).label("raw_balance")
    ).where(
        sm.warehouse_id == warehouse_id,
        sm.stage == ProductStage.raw
    ).group_by(sm.product_id).subquery()

    q = select(p.id, p.name, p.article, raw_sum.c.raw_balance) \
        .join(raw_sum, raw_sum.c.pid == p.id) \
        .where(raw_sum.c.raw_balance > 0) \
        .order_by(p.name.asc())

    rows = (await session.execute(q)).all()
    return [(r[0], r[1], r[2], int(r[3])) for r in rows]

async def _get_raw_balance(session: AsyncSession, warehouse_id: int, product_id: int) -> int:
    val = (await session.execute(
        select(func.coalesce(func.sum(StockMovement.qty), 0))
        .where(StockMovement.warehouse_id == warehouse_id)
        .where(StockMovement.product_id == product_id)
        .where(StockMovement.stage == ProductStage.raw)
    )).scalar()
    return int(val or 0)

async def _next_doc_id(session: AsyncSession) -> int:
    val = (await session.execute(select(func.coalesce(func.max(StockMovement.doc_id), 0)))).scalar()
    return int(val or 0) + 1

# ---------- Entry points ----------
async def _start_packing(message_or_cb, state: FSMContext):
    ws = await _warehouses_list()
    await state.set_state(PackFSM.WH)
    if isinstance(message_or_cb, types.CallbackQuery):
        await message_or_cb.message.edit_text("Выберите склад для упаковки:", reply_markup=kb_wh_list(ws, 0))
    else:
        await message_or_cb.answer("Выберите склад для упаковки:", reply_markup=kb_wh_list(ws, 0))

# 1) Из главного меню (inline callback "packing")
@router.callback_query(F.data == "packing")
async def packing_entry_cb(cb: types.CallbackQuery, state: FSMContext, user: User):
    await cb.answer()
    await _start_packing(cb, state)

# 2) На всякий случай — если есть текстовая кнопка
@router.message(F.text.casefold().in_({"упаковка", "🎁 упаковка", "упаковка 🎁", "🎁упаковка"}))
async def packing_entry_text(msg: types.Message, state: FSMContext, user: User):
    await _start_packing(msg, state)

# ---------- Flow ----------
@router.callback_query(PackFSM.WH, F.data.startswith("pack:wh:page:"))
async def wh_page(call: types.CallbackQuery, state: FSMContext):
    page = int(call.data.split(":")[-1])
    ws = await _warehouses_list()
    await call.message.edit_reply_markup(reply_markup=kb_wh_list(ws, page))

@router.callback_query(PackFSM.WH, F.data.startswith("pack:wh:"))
async def wh_pick(call: types.CallbackQuery, state: FSMContext):
    wh_id = int(call.data.split(":")[-1])
    async with get_session() as s:
        products = await _products_with_raw(s, wh_id)
    await state.update_data(warehouse_id=wh_id, products=products, page=0)
    await state.set_state(PackFSM.PRODUCTS)
    await call.message.edit_text("Выберите товар с положительным RAW-остатком:",
                                 reply_markup=kb_products_list(products, 0, wh_id))

@router.callback_query(PackFSM.PRODUCTS, F.data == "pack:back_wh")
async def back_to_wh(call: types.CallbackQuery, state: FSMContext):
    ws = await _warehouses_list()
    await state.set_state(PackFSM.WH)
    await call.message.edit_text("Выберите склад для упаковки:", reply_markup=kb_wh_list(ws, 0))

@router.callback_query(PackFSM.PRODUCTS, F.data.startswith("pack:p:page:"))
async def products_page(call: types.CallbackQuery, state: FSMContext):
    page = int(call.data.split(":")[-1])
    data = await state.get_data()
    products = data.get("products", [])
    wh_id = data.get("warehouse_id")
    await state.update_data(page=page)
    await call.message.edit_reply_markup(reply_markup=kb_products_list(products, page, wh_id or 0))

@router.callback_query(PackFSM.PRODUCTS, F.data.startswith("pack:p:"))
async def product_pick(call: types.CallbackQuery, state: FSMContext):
    _, _, wh_id, pid = call.data.split(":")
    wh_id = int(wh_id); pid = int(pid)
    async with get_session() as s:
        raw_bal = await _get_raw_balance(s, wh_id, pid)
        prod = (await s.execute(select(Product.name, Product.article).where(Product.id == pid))).first()
    if not prod:
        return await call.answer("Товар не найден", show_alert=True)
    name, article = prod
    await state.update_data(product_id=pid)
    await state.set_state(PackFSM.QTY)
    await call.message.edit_text(
        f"Товар: {name} (art. {article})\n"
        f"Доступно сырья (RAW): {raw_bal}\n\n"
        f"Введите количество для упаковки (целое > 0 и ≤ RAW):"
    )

@router.message(PackFSM.QTY)
async def input_qty(message: types.Message, state: FSMContext):
    txt = (message.text or "").strip()
    if not txt.isdigit():
        return await message.answer("Нужно положительное целое число. Введите ещё раз:")
    qty = int(txt)
    if qty <= 0:
        return await message.answer("Количество должно быть > 0. Попробуйте ещё раз:")

    data = await state.get_data()
    wh_id = data["warehouse_id"]
    pid = data["product_id"]

    async with get_session() as s:
        raw_bal = await _get_raw_balance(s, wh_id, pid)

    if qty > raw_bal:
        return await message.answer(f"Нельзя упаковать {qty}: сырья только {raw_bal}. Введите меньшее количество:")

    await state.update_data(qty=qty)
    await state.set_state(PackFSM.CONFIRM)

    async with get_session() as s:
        prod = (await s.execute(select(Product.name, Product.article).where(Product.id == pid))).first()
    name, article = prod
    await message.answer(
        f"Подтверждение упаковки:\n"
        f"Склад: #{wh_id}\n"
        f"Товар: {name} (art. {article})\n"
        f"Количество: {qty}\n\n"
        f"Провести?",
        reply_markup=kb_confirm()
    )

@router.callback_query(PackFSM.CONFIRM, F.data == "pack:do")
async def do_pack(call: types.CallbackQuery, state: FSMContext, user: User):
    data = await state.get_data()
    wh_id = data["warehouse_id"]
    pid = data["product_id"]
    qty = data["qty"]

    async with get_session() as s:
        raw_bal = await _get_raw_balance(s, wh_id, pid)
        if qty > raw_bal:
            # вернёмся к списку товаров
            products = await _products_with_raw(s, wh_id)
            await state.update_data(products=products, page=0)
            await state.set_state(PackFSM.PRODUCTS)
            await call.message.edit_text(
                f"⛔ Недостаточно сырья (RAW={raw_bal}, нужно {qty}). Выберите другой товар/количество.",
                reply_markup=kb_products_list(products, 0, wh_id)
            )
            return

        doc_id = await _next_doc_id(s)
        # две записи: RAW -qty, PACKED +qty
        s.add(StockMovement(
            warehouse_id=wh_id, product_id=pid,
            qty=-qty, type=MovementType.upakovka, stage=ProductStage.raw,
            user_id=user.id, doc_id=doc_id, comment="Упаковка"
        ))
        s.add(StockMovement(
            warehouse_id=wh_id, product_id=pid,
            qty=+qty, type=MovementType.upakovka, stage=ProductStage.packed,
            user_id=user.id, doc_id=doc_id, comment="Упаковка"
        ))
        await s.commit()

    await state.clear()
    await call.message.edit_text(
        "✅ Упаковка проведена.\n"
        f"Документ #{doc_id}: RAW −{qty}, PACKED +{qty}.\n"
        "Можно продолжать через главное меню."
    )

@router.callback_query(F.data == "pack:cancel")
async def cancel(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("Отменено.")

# Совместимость с текущим bot.py (регистрация как раньше)
def register_packing_handlers(dp: Dispatcher):
    dp.include_router(router)
