# handlers/packing.py
from __future__ import annotations

import datetime
from typing import Dict, List, Tuple, Union

from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func, and_, desc
from sqlalchemy.orm import aliased

from database.db import get_session
from database.models import (
    User, UserRole,
    Warehouse, Product, StockMovement,
    ProductStage, MovementType,
    PackDoc, PackDocItem,
)
from handlers.common import send_content
from keyboards.inline import warehouses_kb

router = Router()

# сколько товаров показываем на странице при подборе
PAGE_SIZE = 12


class PackFSM(StatesGroup):
    choose_wh = State()
    picking = State()
    input_qty = State()


# ===== ВСПОМОГАТЕЛЬНЫЕ =====

async def _raw_map(session, wh_id: int) -> Dict[int, int]:
    """
    Карта RAW остатков по складу: product_id -> qty (>0)
    """
    SM = aliased(StockMovement)
    rows = await session.execute(
        select(SM.product_id, func.sum(SM.qty).label("qty"))
        .where(and_(SM.warehouse_id == wh_id, SM.stage == ProductStage.raw))
        .group_by(SM.product_id)
        .having(func.sum(SM.qty) > 0)
    )
    return {pid: qty for pid, qty in rows.all()}


async def _next_pack_number(session, wh_id: int) -> str:
    """
    Генерация номера документа: YYYYMMDD-XXX в разрезе склада и дня
    """
    today = datetime.date.today()
    start = datetime.datetime.combine(today, datetime.time.min)
    end = datetime.datetime.combine(today, datetime.time.max)
    last = await session.scalar(
        select(PackDoc.number)
        .where(and_(PackDoc.warehouse_id == wh_id, PackDoc.created_at.between(start, end)))
        .order_by(desc(PackDoc.id))
        .limit(1)
    )
    seq = 1
    if last and "-" in last:
        try:
            seq = int(last.split("-")[-1]) + 1
        except Exception:
            seq = 1
    return f"{today.strftime('%Y%m%d')}-{seq:03d}"


def _cart_summary(cart: Dict[int, int]) -> Tuple[int, int]:
    """
    Возвращает (кол-во позиций, суммарное qty) для корзины
    """
    if not cart:
        return 0, 0
    return len(cart), sum(cart.values())


# Универсальная inline‑кнопка «Назад»
def back_inline_kb(target: str = "back_to_packing") -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data=target)]]
    )


def _kb_picking(
        products_rows: List[Tuple[int, str, str | None, int]],
        page: int,
        pages: int,
        cart_cnt: int,
        cart_sum: int,
) -> types.InlineKeyboardMarkup:
    """
    Клавиатура для страницы подбора: список товаров (RAW>0), пагинация, корзина/назад
    """
    rows: List[List[types.InlineKeyboardButton]] = []

    for pid, name, art, raw_qty in products_rows:
        caption = f"{name} (арт. {art or '—'}) • RAW: {raw_qty}"
        rows.append([types.InlineKeyboardButton(text=caption, callback_data=f"pack_add:{pid}")])

    # пагинация
    if pages > 1:
        prev_cb = f"pack_page:{page-1}" if page > 1 else "noop"
        next_cb = f"pack_page:{page+1}" if page < pages else "noop"
        rows.append([
            types.InlineKeyboardButton(text="◀", callback_data=prev_cb),
            types.InlineKeyboardButton(text=f"{page}/{pages}", callback_data="noop"),
            types.InlineKeyboardButton(text="▶", callback_data=next_cb),
        ])

    # корзина/навигация
    rows.append([
        types.InlineKeyboardButton(text=f"🧾 Корзина ({cart_cnt}/{cart_sum})", callback_data="pack_cart"),
    ])
    rows.append([
        types.InlineKeyboardButton(text="⬅️ Назад к складам", callback_data="pack_back_wh"),
        types.InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_packing"),
    ])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_cart(can_post: bool) -> types.InlineKeyboardMarkup:
    """
    Клавиатура для корзины (редактирование позиций, создание документа)
    """
    rows: List[List[types.InlineKeyboardButton]] = []

    rows.append([types.InlineKeyboardButton(text="➕ Добавить ещё", callback_data="pack_continue")])
    if can_post:
        rows.append([types.InlineKeyboardButton(text="✅ Создать документ", callback_data="pack_post")])
    else:
        rows.append([types.InlineKeyboardButton(text="⛔ Нет позиций", callback_data="noop")])

    rows.append([
        types.InlineKeyboardButton(text="🗑 Очистить", callback_data="pack_clear"),
        types.InlineKeyboardButton(text="⬅️ Назад к подбору", callback_data="pack_continue"),
    ])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_docs(docs_rows: List[Tuple[int, str, datetime.datetime, str, int]]) -> types.InlineKeyboardMarkup:
    """
    Список документов упаковки
    """
    rows: List[List[types.InlineKeyboardButton]] = []
    for did, number, created_at, wh_name, total in docs_rows:
        label = f"№{number} • {created_at:%d.%m %H:%M} • {wh_name} • {total} шт."
        rows.append([types.InlineKeyboardButton(text=label, callback_data=f"pack_doc:{did}")])
    rows.append([types.InlineKeyboardButton(text="⬅️ Назад", callback_data="pack_root")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_picking(target: Union[types.CallbackQuery, types.Message], state: FSMContext):
    """
    Рендер страницы подбора (универсально для CallbackQuery/Message)
    """
    data = await state.get_data()
    wh_name: str = data["wh_name"]
    page: int = int(data.get("page", 1))
    cart: Dict[int, int] = data.get("cart", {})
    raw_map: Dict[int, int] = data["raw_map"]
    products: List[Tuple[int, str, str | None]] = data["products"]

    pages = max(1, (len(products) + PAGE_SIZE - 1) // PAGE_SIZE)
    start, end = (page - 1) * PAGE_SIZE, (page - 1) * PAGE_SIZE + PAGE_SIZE
    slice_rows = [(pid, name, art, raw_map.get(pid, 0)) for (pid, name, art) in products[start:end]]

    cnt, summ = _cart_summary(cart)
    text = f"🏬 *{wh_name}*\nВыберите товар для упаковки (RAW > 0).\n\n🧾 Корзина: {cnt} поз., {summ} шт."

    await send_content(
        target,
        text,
        parse_mode="Markdown",
        reply_markup=_kb_picking(slice_rows, page, pages, cnt, summ),
    )


# ===== ROOT / МЕНЮ =====

@router.callback_query(F.data == "packing")
async def pack_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await state.clear()
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🆕 Новая упаковка", callback_data="pack_new")],
        [types.InlineKeyboardButton(text="🏷 Документы упаковки", callback_data="pack_docs")],
        [types.InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")],
    ])
    await send_content(cb, "Упаковка — выберите действие:", reply_markup=kb)


# ===== СОЗДАНИЕ НОВОЙ УПАКОВКИ =====

@router.callback_query(F.data == "pack_new")
async def pack_new(cb: types.CallbackQuery, user: User, state: FSMContext):
    await state.clear()
    async with get_session() as session:
        wh = (await session.execute(
            select(Warehouse).where(Warehouse.is_active == True).order_by(Warehouse.name)
        )).scalars().all()
    if not wh:
        return await send_content(cb, "🚫 Нет активных складов.")
    await state.set_state(PackFSM.choose_wh)
    await send_content(cb, "Выберите склад для новой упаковки:", reply_markup=warehouses_kb(wh, prefix="pack_wh"))


@router.callback_query(F.data.startswith("pack_wh:"))
async def pack_choose_wh(cb: types.CallbackQuery, user: User, state: FSMContext):
    # фикс состояния
    if await state.get_state() != PackFSM.choose_wh:
        await state.set_state(PackFSM.choose_wh)

    wh_id = int(cb.data.split(":")[1])
    async with get_session() as session:
        wh = await session.get(Warehouse, wh_id)
        if not wh or not wh.is_active:
            return await send_content(cb, "🚫 Склад не найден или неактивен.")
        raw = await _raw_map(session, wh_id)
        if not raw:
            return await send_content(cb, f"На складе *{wh.name}* нет RAW остатков.", parse_mode="Markdown")
        prod_rows = (await session.execute(
            select(Product.id, Product.name, Product.article)
            .where(and_(Product.is_active == True, Product.id.in_(raw.keys())))
            .order_by(Product.article)
        )).all()

    await state.update_data(
        wh_id=wh_id,
        wh_name=wh.name,
        page=1,
        cart={},
        raw_map=raw,
        products=prod_rows,
    )
    await state.set_state(PackFSM.picking)
    await _render_picking(cb, state)


@router.callback_query(F.data.startswith("pack_page:"))
async def pack_page(cb: types.CallbackQuery, state: FSMContext):
    page = int(cb.data.split(":")[1])
    await state.update_data(page=page)
    await _render_picking(cb, state)


@router.callback_query(F.data.startswith("pack_add:"))
async def pack_add(cb: types.CallbackQuery, state: FSMContext):
    """
    Клик по товару — запрос количества
    """
    pid = int(cb.data.split(":")[1])
    data = await state.get_data()
    raw_map: Dict[int, int] = data["raw_map"]
    can = int(raw_map.get(pid, 0))
    if can <= 0:
        return await cb.answer("Нет RAW остатка", show_alert=True)
    await state.update_data(current_pid=pid, current_can=can)
    await cb.message.answer(f"Введите количество для упаковки (доступно RAW: {can})")
    await state.set_state(PackFSM.input_qty)


@router.message(PackFSM.input_qty)
async def pack_input_qty(msg: types.Message, state: FSMContext):
    """
    Обработка ручного ввода qty и возврат в подбор со свежей корзиной
    """
    try:
        qty = int(msg.text.strip())
        if qty <= 0:
            raise ValueError
    except Exception:
        return await msg.answer("Введите целое положительное число.")

    data = await state.get_data()
    pid = data["current_pid"]
    can = data["current_can"]
    if qty > can:
        return await msg.answer(f"Недостаточно RAW. Доступно: {can}")

    cart: Dict[int, int] = data.get("cart", {})
    cart[pid] = cart.get(pid, 0) + qty

    raw_map: Dict[int, int] = data["raw_map"]
    raw_map[pid] = can - qty

    await state.update_data(cart=cart, raw_map=raw_map)
    await state.set_state(PackFSM.picking)

    await msg.answer("Добавлено ✅")
    await _render_picking(msg, state)


# ===== КОРЗИНА И РЕДАКТИРОВАНИЕ =====

@router.callback_query(F.data == "pack_cart")
async def pack_cart(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    if not cart:
        return await cb.answer("Корзина пуста", show_alert=True)

    async with get_session() as session:
        rows = (await session.execute(
            select(Product.id, Product.name, Product.article).where(Product.id.in_(cart.keys()))
        )).all()
    info = {pid: (name, art) for pid, name, art in rows}

    lines = ["🧾 *Подбор упаковки*:", ""]
    total = 0
    kb_rows: List[List[types.InlineKeyboardButton]] = []
    idx = 1
    for pid, q in cart.items():
        name, art = info.get(pid, ("?", None))
        lines.append(f"{idx}) `{art or pid}` — *{name}*: **{q}** шт.")
        kb_rows.append([
            types.InlineKeyboardButton(text="➖1", callback_data=f"pack_dec:{pid}"),
            types.InlineKeyboardButton(text="➕1", callback_data=f"pack_inc:{pid}"),
            types.InlineKeyboardButton(text="❌", callback_data=f"pack_del:{pid}"),
        ])
        total += q
        idx += 1

    lines += ["", f"📈 Итого: {len(cart)} позиций, {total} шт."]

    # общие кнопки
    kb_rows.append([types.InlineKeyboardButton(text="➕ Добавить ещё", callback_data="pack_continue")])
    if total > 0:
        kb_rows.append([types.InlineKeyboardButton(text="✅ Создать документ", callback_data="pack_post")])
    kb_rows.append([
        types.InlineKeyboardButton(text="🗑 Очистить", callback_data="pack_clear"),
        types.InlineKeyboardButton(text="⬅️ Назад к подбору", callback_data="pack_continue"),
    ])
    kb = types.InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await send_content(cb, "\n".join(lines), parse_mode="Markdown", reply_markup=kb)


@router.callback_query(F.data.startswith("pack_inc:"))
async def pack_inc(cb: types.CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    raw_map: Dict[int, int] = data.get("raw_map", {})
    can_left = int(raw_map.get(pid, 0))
    if can_left <= 0:
        return await cb.answer("Нет RAW для увеличения", show_alert=True)
    cart[pid] = cart.get(pid, 0) + 1
    raw_map[pid] = can_left - 1
    await state.update_data(cart=cart, raw_map=raw_map)
    await pack_cart(cb, state)


@router.callback_query(F.data.startswith("pack_dec:"))
async def pack_dec(cb: types.CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    q = cart.get(pid, 0)
    if q <= 0:
        return await cb.answer("Эта позиция уже 0", show_alert=True)
    cart[pid] = q - 1
    # возвращаем RAW доступность
    raw_map: Dict[int, int] = data.get("raw_map", {})
    raw_map[pid] = raw_map.get(pid, 0) + 1
    if cart[pid] == 0:
        del cart[pid]
    await state.update_data(cart=cart, raw_map=raw_map)
    await pack_cart(cb, state)


@router.callback_query(F.data.startswith("pack_del:"))
async def pack_del(cb: types.CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    q = cart.pop(pid, 0)
    raw_map: Dict[int, int] = data.get("raw_map", {})
    raw_map[pid] = raw_map.get(pid, 0) + q
    await state.update_data(cart=cart, raw_map=raw_map)
    await cb.answer("Удалено")
    await pack_cart(cb, state)


@router.callback_query(F.data == "pack_clear")
async def pack_clear(cb: types.CallbackQuery, state: FSMContext):
    """
    Очистить корзину и пересчитать доступный RAW из базы
    """
    data = await state.get_data()
    async with get_session() as session:
        raw = await _raw_map(session, data["wh_id"])
    await state.update_data(cart={}, raw_map=raw)
    await cb.answer("Корзина очищена")
    await _render_picking(cb, state)


@router.callback_query(F.data == "pack_continue")
async def pack_continue(cb: types.CallbackQuery, state: FSMContext):
    await _render_picking(cb, state)


# ===== СОЗДАНИЕ ДОКУМЕНТА (ПРОВЕДЕНИЕ) =====

@router.callback_query(F.data == "pack_post")
async def pack_post(cb: types.CallbackQuery, user: User, state: FSMContext):
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    if not cart:
        return await cb.answer("Корзина пуста", show_alert=True)
    wh_id = data["wh_id"]

    async with get_session() as session:
        number = await _next_pack_number(session, wh_id)
        doc = PackDoc(number=number, warehouse_id=wh_id, user_id=user.id)
        session.add(doc)
        await session.flush()  # получаем doc.id

        # позиции документа + движения
        for pid, qty in cart.items():
            session.add(PackDocItem(doc_id=doc.id, product_id=pid, qty=qty))
            # raw -
            session.add(StockMovement(
                type=MovementType.upakovka, stage=ProductStage.raw, qty=-qty,
                product_id=pid, warehouse_id=wh_id, doc_id=doc.id
            ))
            # packed +
            session.add(StockMovement(
                type=MovementType.upakovka, stage=ProductStage.packed, qty=qty,
                product_id=pid, warehouse_id=wh_id, doc_id=doc.id
            ))

        # помечаем документ как проведённый
        doc.status = "posted"
        await session.commit()

    await state.clear()
    await send_content(cb, f"✅ Документ упаковки создан: *№{number}*.", parse_mode="Markdown")
    await _show_doc(cb, doc_id=None, number=number)


async def _show_doc(cb: types.CallbackQuery, doc_id: int | None = None, number: str | None = None):
    """
    Карточка документа упаковки
    """
    async with get_session() as session:
        if doc_id:
            doc = await session.get(PackDoc, doc_id)
        else:
            doc = (await session.execute(select(PackDoc).where(PackDoc.number == number))).scalar_one()
        wh = await session.get(Warehouse, doc.warehouse_id)
        items = (await session.execute(
            select(PackDocItem, Product.name, Product.article)
            .join(Product, Product.id == PackDocItem.product_id)
            .where(PackDocItem.doc_id == doc.id)
            .order_by(Product.article)
        )).all()

    total = sum(i.PackDocItem.qty for i in items)
    lines = [
        f"🏷 Документ упаковки *№{doc.number}* от {doc.created_at:%d.%m.%Y %H:%M}",
        f"Склад: *{wh.name}*",
        f"Статус: *{doc.status}*",
        "",
        "Состав:"
    ]
    for idx, row in enumerate(items, start=1):
        it = row.PackDocItem
        name, art = row.name, row.article
        lines.append(f"{idx}) `{art or it.product_id}` — *{name}*: **{it.qty}** шт.")
    lines += ["", f"📈 Итого: {len(items)} позиций, {total} шт."]

    kb_rows = [[types.InlineKeyboardButton(text="⬅️ К списку документов", callback_data="pack_docs")]]
    kb = types.InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await send_content(cb, "\n".join(lines), parse_mode="Markdown", reply_markup=kb)


# ===== СПИСОК ДОКУМЕНТОВ =====

@router.callback_query(F.data == "pack_docs")
async def pack_docs(cb: types.CallbackQuery, state: FSMContext):
    async with get_session() as session:
        rows = (await session.execute(
            select(
                PackDoc.id, PackDoc.number, PackDoc.created_at, Warehouse.name,
                func.coalesce(func.sum(PackDocItem.qty), 0).label("total")
            )
            .join(Warehouse, Warehouse.id == PackDoc.warehouse_id)
            .join(PackDocItem, PackDocItem.doc_id == PackDoc.id)
            .group_by(PackDoc.id, Warehouse.name)
            .order_by(desc(PackDoc.created_at))
            .limit(20)
        )).all()

    if not rows:
        kb = types.InlineKeyboardMarkup(
            inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data="pack_root")]]
        )
        return await send_content(cb, "Документов упаковки пока нет.", reply_markup=kb)

    await send_content(cb, "Последние документы упаковки:", reply_markup=_kb_docs(rows))


@router.callback_query(F.data.startswith("pack_doc:"))
async def pack_doc_open(cb: types.CallbackQuery, state: FSMContext):
    did = int(cb.data.split(":")[1])
    await _show_doc(cb, doc_id=did)


# ===== НАВИГАЦИЯ =====

@router.callback_query(F.data == "pack_back_wh")
async def pack_back_wh(cb: types.CallbackQuery, state: FSMContext):
    # заново начало флоу выбора склада
    await pack_new(cb, user=None, state=state)  # user в pack_new не используется


# Локальный обработчик «Назад» для упаковки
@router.callback_query(F.data == "back_to_packing")
async def back_to_packing(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await cb.message.edit_reply_markup()
    except Exception:
        pass
    await cb.message.answer("Раздел «Упаковка». Выберите действие:", reply_markup=back_inline_kb("back_to_menu"))
    await cb.answer()
