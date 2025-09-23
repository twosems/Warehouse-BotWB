from __future__ import annotations

from datetime import datetime
from typing import List, Dict, Tuple

from aiogram import Router, F, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from database.db import get_session, available_packed
from database.models import (
    Warehouse, Product, StockMovement,
    Supply, SupplyItem, SupplyBox, SupplyFile, User,
    MovementType, ProductStage, UserRole, SupplyStatus
)

router = Router()

PAGE = 10

# ---------- FSM (MVP создания поставки из PACKED) ----------
class SupFSM(StatesGroup):
    MP = State()
    WH = State()
    ITEMS = State()
    QTY = State()
    CONFIRM = State()


# ---------- Keyboards ----------
def kb_sup_tabs(role: UserRole) -> InlineKeyboardMarkup:
    rows = []
    if role in (UserRole.admin, UserRole.manager):
        rows += [
            [InlineKeyboardButton(text="🆕 Черновики", callback_data="sup:list:draft:0")],
            [InlineKeyboardButton(text="📥 К сборке",  callback_data="sup:list:queued:0")],
            [InlineKeyboardButton(text="🛠 В работе",  callback_data="sup:list:assembling:0")],
            [InlineKeyboardButton(text="✅ Собранные", callback_data="sup:list:assembled:0")],
            [InlineKeyboardButton(text="🚚 Доставляется", callback_data="sup:list:in_transit:0")],
            [InlineKeyboardButton(text="🗄 Архив", callback_data="sup:list:arch:0")],
            [InlineKeyboardButton(text="🆕 Создать поставку", callback_data="sup:new")],
        ]
    else:
        rows += [
            [InlineKeyboardButton(text="📥 К сборке",  callback_data="sup:list:queued:0")],
            [InlineKeyboardButton(text="🛠 Мои в работе", callback_data="sup:list:myassembling:0")],
            [InlineKeyboardButton(text="✅ Мои собранные", callback_data="sup:list:myassembled:0")],
            [InlineKeyboardButton(text="🗄 Архив", callback_data="sup:list:myarch:0")],
        ]
    rows.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_mp() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Wildberries", callback_data="sup:mp:wb")],
        [InlineKeyboardButton(text="Ozon",        callback_data="sup:mp:ozon")],
        [InlineKeyboardButton(text="❌ Отмена",    callback_data="sup:cancel")],
    ])


def kb_wh_list(warehouses, page=0) -> InlineKeyboardMarkup:
    start = page * PAGE
    chunk = warehouses[start:start + PAGE]
    rows = [[InlineKeyboardButton(text=name, callback_data=f"sup:wh:{wid}")]
            for wid, name in chunk]
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"sup:wh:page:{page - 1}"))
    if start + PAGE < len(warehouses):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"sup:wh:page:{page + 1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="sup:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_products_packed(products, page: int, wh_id: int) -> InlineKeyboardMarkup:
    start = page * PAGE
    chunk = products[start:start + PAGE]
    rows = [[InlineKeyboardButton(
        text=f"{name} (art. {article}) — PACKED {packed}",
        callback_data=f"sup:add:{wh_id}:{pid}"
    )] for pid, name, article, packed in chunk]
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"sup:prod:page:{page - 1}"))
    if start + PAGE < len(products):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"sup:prod:page:{page + 1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton(text="📩 Сохранить черновик", callback_data="sup:submit")])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="sup:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить ещё", callback_data="sup:more")],
        [InlineKeyboardButton(text="📩 Сохранить черновик", callback_data="sup:submit")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="sup:cancel")],
    ])


def kb_supply_card(s: Supply, role: UserRole, all_sealed: bool) -> InlineKeyboardMarkup:
    rows = []
    st = s.status
    if st == SupplyStatus.draft and role in (UserRole.admin, UserRole.manager):
        rows += [
            [InlineKeyboardButton(text="✏️ Реквизиты", callback_data=f"sup:edit:{s.id}")],
            [InlineKeyboardButton(text="➕ Короб", callback_data=f"sup:box:add:{s.id}")],
            [InlineKeyboardButton(text="📎 PDF", callback_data=f"sup:file:add:{s.id}")],
            [InlineKeyboardButton(text="📤 Отправить на сборку", callback_data=f"sup:queue:{s.id}")],
            [InlineKeyboardButton(text="🗑 Отменить", callback_data=f"sup:cancel:{s.id}")],
        ]
    elif st == SupplyStatus.queued:
        rows += [[InlineKeyboardButton(text="🛠 Взять в работу", callback_data=f"sup:assign:{s.id}")]]
        if role in (UserRole.admin, UserRole.manager):
            rows += [
                [InlineKeyboardButton(text="↩️ В черновик", callback_data=f"sup:to_draft:{s.id}")],
                [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"sup:edit:{s.id}")],
                [InlineKeyboardButton(text="📎 PDF", callback_data=f"sup:file:add:{s.id}")],
            ]
    elif st == SupplyStatus.assembling:
        rows += [
            [InlineKeyboardButton(text="📦 Запечатать все", callback_data=f"sup:box:seal_all:{s.id}")],
            [InlineKeyboardButton(text="🔓 Снять пломбы", callback_data=f"sup:box:unseal_all:{s.id}")],
            [InlineKeyboardButton(text="✅ Отметить как собранную", callback_data=f"sup:assembled:{s.id}")],
        ]
        if role in (UserRole.admin, UserRole.manager):
            rows += [[InlineKeyboardButton(text="↩️ Снять с работы", callback_data=f"sup:to_queue:{s.id}")]]
    elif st == SupplyStatus.assembled:
        rows += [
            [InlineKeyboardButton(text="🚚 Провести (отправить)", callback_data=f"sup:post:{s.id}")],
            [InlineKeyboardButton(text="🔓 Снять пломбу", callback_data=f"sup:box:unseal_all:{s.id}")],
        ]
    elif st == SupplyStatus.in_transit and role in (UserRole.admin, UserRole.manager):
        rows += [
            [InlineKeyboardButton(text="✅ Доставлено", callback_data=f"sup:delivered:{s.id}")],
            [InlineKeyboardButton(text="↩️ Возврат", callback_data=f"sup:return:{s.id}")],
            [InlineKeyboardButton(text="♻️ Расформировать", callback_data=f"sup:unpost:{s.id}")],
        ]
    rows.append([InlineKeyboardButton(text="⬅️ К списку", callback_data=f"sup:list:auto:0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------- Helpers ----------
async def _warehouses_list() -> List[Tuple[int, str]]:
    async with get_session() as s:
        rows = (await s.execute(
            select(Warehouse.id, Warehouse.name)
            .where((Warehouse.is_active.is_(True)) | (Warehouse.is_active.is_(None)))
            .order_by(Warehouse.name.asc())
        )).all()
    items = [(r[0], r[1]) for r in rows]
    counts = {}
    for _, n in items:
        counts[n] = counts.get(n, 0) + 1
    return [(wid, name if counts[name] == 1 else f"{name} (#{wid})") for wid, name in items]


async def _products_with_packed(session: AsyncSession, warehouse_id: int) -> List[Tuple[int, str, str, int]]:
    sm, p = StockMovement, Product
    packed_sum = select(
        sm.product_id.label("pid"),
        func.coalesce(func.sum(sm.qty), 0).label("packed_balance")
    ).where(
        sm.warehouse_id == warehouse_id,
        sm.stage == ProductStage.packed
    ).group_by(sm.product_id).subquery()

    q = select(p.id, p.name, p.article, packed_sum.c.packed_balance) \
        .join(packed_sum, packed_sum.c.pid == p.id) \
        .where(packed_sum.c.packed_balance > 0) \
        .order_by(p.name.asc())
    rows = (await session.execute(q)).all()
    return [(r[0], r[1], r[2], int(r[3])) for r in rows]


async def _get_balance(session: AsyncSession, wh: int, pid: int, stage: ProductStage) -> int:
    val = (await session.execute(
        select(func.coalesce(func.sum(StockMovement.qty), 0))
        .where(StockMovement.warehouse_id == wh)
        .where(StockMovement.product_id == pid)
        .where(StockMovement.stage == stage)
    )).scalar()
    return int(val or 0)


def _now():
    return datetime.utcnow()


# ---------- Lists / Cards ----------
async def _load_supplies(tab: str, user: User, page: int = 0):
    q = select(Supply).order_by(Supply.created_at.desc())
    if tab == "draft":
        q = q.where(Supply.status == SupplyStatus.draft)
    elif tab == "queued":
        q = q.where(Supply.status == SupplyStatus.queued)
    elif tab == "assembling":
        q = q.where(Supply.status == SupplyStatus.assembling)
    elif tab == "assembled":
        q = q.where(Supply.status == SupplyStatus.assembled)
    elif tab == "in_transit":
        q = q.where(Supply.status == SupplyStatus.in_transit)
    elif tab == "arch":
        q = q.where(Supply.status.in_([SupplyStatus.archived_delivered,
                                       SupplyStatus.archived_returned,
                                       SupplyStatus.cancelled]))
    elif tab.startswith("my"):
        if tab == "myassembling":
            q = q.where(Supply.status == SupplyStatus.assembling, Supply.assigned_picker_id == user.id)
        elif tab == "myassembled":
            q = q.where(Supply.status == SupplyStatus.assembled, Supply.assigned_picker_id == user.id)
        else:  # myarch
            q = q.where(Supply.assigned_picker_id == user.id,
                        Supply.status.in_([SupplyStatus.archived_delivered, SupplyStatus.archived_returned]))
    async with get_session() as s:
        rows = (await s.execute(q.offset(page * PAGE).limit(PAGE + 1))).scalars().all()
    has_next = len(rows) > PAGE
    rows = rows[:PAGE]
    return rows, has_next


def _kb_sup_list(tab: str, s_list: List[Supply], page: int, has_next: bool) -> InlineKeyboardMarkup:
    rows = []
    for s in s_list:
        title = f"SUP-{s.created_at:%Y%m%d}-{s.id:03d} • {s.status.value}"
        rows.append([InlineKeyboardButton(text=title, callback_data=f"sup:open:{s.id}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"sup:list:{tab}:{page - 1}"))
    if has_next:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"sup:list:{tab}:{page + 1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="supplies")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_supply_card(call_or_msg, s_id: int, user: User):
    async with get_session() as s:
        sup = await s.get(Supply, s_id)
        if not sup:
            return await (call_or_msg.message.answer if isinstance(call_or_msg, types.CallbackQuery) else call_or_msg.answer)(
                "Поставка не найдена."
            )
        boxes = (await s.execute(select(SupplyBox).where(SupplyBox.supply_id == s_id).order_by(SupplyBox.box_number))).scalars().all()
        items = (await s.execute(select(SupplyItem, Product.article, Product.name)
                                 .join(Product, Product.id == SupplyItem.product_id)
                                 .where(SupplyItem.supply_id == s_id))).all()
        files = (await s.execute(select(SupplyFile).where(SupplyFile.supply_id == s_id)
                                 .order_by(SupplyFile.uploaded_at.desc()))).scalars().all()

    by_box: Dict[int, List[Tuple[str, str, int]]] = {}
    for it, art, name in items:
        by_box.setdefault(it.box_id or 0, []).append((art, name, it.qty))
    lines = [
        f"№SUP-{sup.created_at:%Y%m%d}-{sup.id:03d} • [{sup.status.value}]",
        f"МП: {sup.mp or '—'} • МП-склад: {sup.mp_warehouse or '—'} • Склад: #{sup.warehouse_id}",
        f"Сборщик: {sup.assigned_picker_id or '—'}",
        f"Комментарий: {sup.comment or '—'}",
        "",
        "Короба:"
    ]
    all_sealed = True
    if not boxes:
        lines.append("— нет коробов —")
    else:
        for b in boxes:
            b_items = by_box.get(b.id, [])
            qty = sum(q for _, _, q in b_items)
            lines.append(f"#{b.box_number} • {'sealed' if b.sealed else 'open'} • позиций {len(b_items)} • qty {qty}")
            all_sealed = all_sealed and b.sealed
    lines.append("")
    lines.append("Вложения:")
    lines += ([f"• {f.filename or f.file_id}" for f in files] or ["— нет файлов —"])

    kb = kb_supply_card(sup, user.role, all_sealed)
    txt = "\n".join(lines)
    if isinstance(call_or_msg, types.CallbackQuery):
        await call_or_msg.message.edit_text(txt, reply_markup=kb)
    else:
        await call_or_msg.answer(txt, reply_markup=kb)


# ---------- Entry ----------
@router.callback_query(F.data == "supplies")
async def supplies_root(call: types.CallbackQuery, user: User):
    await call.answer()
    await call.message.edit_text("Раздел «Поставки»", reply_markup=kb_sup_tabs(user.role))


@router.callback_query(F.data.startswith("sup:list:"))
async def sup_list(call: types.CallbackQuery, user: User):
    _, _, tab, page_s = call.data.split(":")
    if tab == "auto":
        tab = "draft" if user.role in (UserRole.admin, UserRole.manager) else "queued"
    page = int(page_s)
    s_list, has_next = await _load_supplies(tab, user, page)
    await call.message.edit_text(f"Поставки — {tab}", reply_markup=_kb_sup_list(tab, s_list, page, has_next))


@router.callback_query(F.data.startswith("sup:open:"))
async def sup_open(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    await _render_supply_card(call, sid, user)


# ---------- Create (FSM) ----------
@router.callback_query(F.data == "sup:new")
async def sup_new(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(SupFSM.MP)
    await call.message.edit_text("Выберите маркетплейс:", reply_markup=kb_mp())


@router.callback_query(SupFSM.MP, F.data.startswith("sup:mp:"))
async def sup_pick_mp(call: types.CallbackQuery, state: FSMContext):
    mp = call.data.split(":")[-1]  # wb|ozon
    await state.update_data(mp=mp)
    ws = await _warehouses_list()
    await state.set_state(SupFSM.WH)
    await call.message.edit_text("Выберите склад-источник:", reply_markup=kb_wh_list(ws, 0))


@router.callback_query(SupFSM.WH, F.data.startswith("sup:wh:page:"))
async def sup_wh_page(call: types.CallbackQuery, state: FSMContext):
    page = int(call.data.split(":")[-1])
    ws = await _warehouses_list()
    await call.message.edit_reply_markup(reply_markup=kb_wh_list(ws, page))


@router.callback_query(SupFSM.WH, F.data.startswith("sup:wh:"))
async def sup_wh_pick(call: types.CallbackQuery, state: FSMContext):
    wh_id = int(call.data.split(":")[-1])
    async with get_session() as s:
        products = await _products_with_packed(s, wh_id)
    await state.update_data(wh_id=wh_id, products=products, page=0, cart={})
    await state.set_state(SupFSM.ITEMS)
    await call.message.edit_text("Добавьте позиции (из упакованного PACKED):",
                                 reply_markup=kb_products_packed(products, 0, wh_id))


@router.callback_query(SupFSM.ITEMS, F.data.startswith("sup:prod:page:"))
async def sup_products_page(call: types.CallbackQuery, state: FSMContext):
    page = int(call.data.split(":")[-1])
    data = await state.get_data()
    await state.update_data(page=page)
    await call.message.edit_reply_markup(reply_markup=kb_products_packed(data["products"], page, data["wh_id"]))


@router.callback_query(SupFSM.ITEMS, F.data.startswith("sup:add:"))
async def sup_add_product(call: types.CallbackQuery, state: FSMContext):
    _, _, wh, pid = call.data.split(":")
    await state.update_data(cur_pid=int(pid))
    await state.set_state(SupFSM.QTY)
    await call.message.edit_text("Введите количество для добавления в поставку:")


@router.message(SupFSM.QTY)
async def sup_qty_input(msg: types.Message, state: FSMContext):
    txt = (msg.text or "").strip()
    if not txt.isdigit() or int(txt) <= 0:
        return await msg.answer("Нужно целое число > 0. Введите ещё раз:")
    qty = int(txt)
    data = await state.get_data()
    wh_id, pid = data["wh_id"], data["cur_pid"]

    async with get_session() as s:
        packed = await _get_balance(s, wh_id, pid, ProductStage.packed)

    warn = ""
    if qty > packed:
        warn = f"⚠️ Упаковано PACKED={packed}, вы добавляете {qty}. Дефицит {qty - packed}. Будет проверено при «Провести»."
    cart = data.get("cart", {})
    cart[pid] = cart.get(pid, 0) + qty
    await state.update_data(cart=cart)
    await state.set_state(SupFSM.CONFIRM)

    async with get_session() as s:
        prod = (await s.execute(select(Product.name, Product.article).where(Product.id == pid))).first()
    name, article = prod
    await msg.answer(
        f"Добавлено: {name} (art. {article}) — {qty}\n{warn}\n"
        f"Что дальше?",
        reply_markup=kb_confirm()
    )


@router.callback_query(SupFSM.CONFIRM, F.data == "sup:more")
async def sup_more(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    async with get_session() as s:
        products = await _products_with_packed(s, data["wh_id"])
    await state.update_data(products=products, page=0)
    await state.set_state(SupFSM.ITEMS)
    await call.message.edit_text("Добавьте позиции (из упакованного PACKED):",
                                 reply_markup=kb_products_packed(products, 0, data["wh_id"]))


@router.callback_query(SupFSM.CONFIRM, F.data == "sup:submit")
async def sup_submit(call: types.CallbackQuery, state: FSMContext, user: User):
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    if not cart:
        return await call.answer("Пусто", show_alert=True)

    mp = data["mp"]
    wh_id = data["wh_id"]

    async with get_session() as s:
        # создаём черновик
        sup = Supply(
            warehouse_id=wh_id,
            created_by=user.id,
            status=SupplyStatus.draft,
            mp=mp,
        )
        s.add(sup)
        await s.flush()
        # авто-создаём короб #1 (MVP)
        box = SupplyBox(supply_id=sup.id, box_number=1, sealed=False)
        s.add(box)
        await s.flush()
        for pid, qty in cart.items():
            s.add(SupplyItem(supply_id=sup.id, product_id=pid, qty=qty, box_id=box.id))
        await s.commit()
        sup_id = sup.id

    await state.clear()
    await call.message.edit_text(f"Черновик поставки создан: SUP-{datetime.utcnow():%Y%m%d}-{sup_id:03d}")
    await _render_supply_card(call, sup_id, user)


# ---------- Status transitions ----------
@router.callback_query(F.data.startswith("sup:queue:"))
async def sup_to_queue(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid)
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if user.role not in (UserRole.admin, UserRole.manager): return await call.answer("Недостаточно прав", show_alert=True)
        if sup.status != SupplyStatus.draft: return await call.answer("Только из 'draft'", show_alert=True)
        sup.status = SupplyStatus.queued
        sup.queued_at = _now()
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:assign:"))
async def sup_assign(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid)
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if sup.status != SupplyStatus.queued: return await call.answer("Только из 'queued'", show_alert=True)
        sup.assigned_picker_id = user.id
        sup.status = SupplyStatus.assembling
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:assembled:"))
async def sup_mark_assembled(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid)
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if sup.status != SupplyStatus.assembling: return await call.answer("Неверный статус", show_alert=True)
        sup.status = SupplyStatus.assembled
        sup.assembled_at = _now()
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:post:"))
async def sup_post(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid, options=[joinedload(Supply.items)])
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if sup.status != SupplyStatus.assembled: return await call.answer("Только из 'assembled'", show_alert=True)

        # Валидация доступности PACKED c учетом резервов
        for it in sup.items:
            can = await available_packed(s, sup.warehouse_id, it.product_id)
            if it.qty > can:
                return await call.answer(f"Недостаточно PACKED по товару {it.product_id}: доступно {can}, нужно {it.qty}", show_alert=True)

        max_doc = await s.scalar(select(func.max(StockMovement.doc_id)).where(StockMovement.type == MovementType.postavka))
        next_doc = int(max_doc or 0) + 1
        docname = f"SUP-{sup.created_at:%Y%m%d}-{sup.id:03d}"

        for it in sup.items:
            s.add(StockMovement(
                warehouse_id=sup.warehouse_id,
                product_id=it.product_id,
                qty=-it.qty,
                type=MovementType.postavka,
                stage=ProductStage.packed,
                user_id=user.id,
                doc_id=next_doc,
                comment=f"[SUP {docname}] Отправка в {sup.mp or 'MP'}/{sup.mp_warehouse or '-'}"
            ))

        sup.status = SupplyStatus.in_transit
        sup.posted_at = _now()
        await s.commit()

    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:delivered:"))
async def sup_delivered(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid)
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if user.role not in (UserRole.admin, UserRole.manager): return await call.answer("Недостаточно прав", show_alert=True)
        if sup.status != SupplyStatus.in_transit: return await call.answer("Только из 'in_transit'", show_alert=True)
        sup.status = SupplyStatus.archived_delivered
        sup.delivered_at = _now()
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:return:"))
async def sup_return(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid, options=[joinedload(Supply.items)])
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if user.role not in (UserRole.admin, UserRole.manager): return await call.answer("Недостаточно прав", show_alert=True)
        if sup.status != SupplyStatus.in_transit: return await call.answer("Только из 'in_transit'", show_alert=True)

        max_doc = await s.scalar(select(func.max(StockMovement.doc_id)).where(StockMovement.type == MovementType.postavka))
        next_doc = int(max_doc or 0) + 1
        docname = f"SUP-RET-{sup.created_at:%Y%m%d}-{sup.id:03d}"

        for it in sup.items:
            s.add(StockMovement(
                warehouse_id=sup.warehouse_id,
                product_id=it.product_id,
                qty=it.qty,
                type=MovementType.postavka,
                stage=ProductStage.packed,
                user_id=user.id,
                doc_id=next_doc,
                comment=f"[{docname}] Возврат из МП"
            ))

        sup.status = SupplyStatus.archived_returned
        sup.returned_at = _now()
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:unpost:"))
async def sup_unpost(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid, options=[joinedload(Supply.items)])
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if user.role not in (UserRole.admin, UserRole.manager): return await call.answer("Недостаточно прав", show_alert=True)
        if sup.status != SupplyStatus.in_transit: return await call.answer("Только из 'in_transit'", show_alert=True)

        max_doc = await s.scalar(select(func.max(StockMovement.doc_id)).where(StockMovement.type == MovementType.postavka))
        next_doc = int(max_doc or 0) + 1
        docname = f"SUP-UNPOST-{sup.created_at:%Y%m%d}-{sup.id:03d}"

        for it in sup.items:
            s.add(StockMovement(
                warehouse_id=sup.warehouse_id,
                product_id=it.product_id,
                qty=it.qty,
                type=MovementType.postavka,
                stage=ProductStage.packed,
                user_id=user.id,
                doc_id=next_doc,
                comment=f"[{docname}] Расформирование (возврат на склад)"
            ))

        sup.status = SupplyStatus.assembled
        sup.unposted_at = _now()
        await s.commit()
    await _render_supply_card(call, sid, user)


# ---------- Boxes (MVP действия) ----------
@router.callback_query(F.data.startswith("sup:box:add:"))
async def sup_box_add(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid)
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if sup.status not in (SupplyStatus.draft, SupplyStatus.queued, SupplyStatus.assembling):
            return await call.answer("На этом статусе нельзя добавлять короб.", show_alert=True)
        last = (await s.execute(select(func.max(SupplyBox.box_number)).where(SupplyBox.supply_id == sid))).scalar()
        num = int(last or 0) + 1
        s.add(SupplyBox(supply_id=sid, box_number=num, sealed=False))
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:box:seal_all:"))
async def sup_box_seal_all(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        rows = (await s.execute(select(SupplyBox).where(SupplyBox.supply_id == sid))).scalars().all()
        for b in rows:
            b.sealed = True
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:box:unseal_all:"))
async def sup_box_unseal_all(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        rows = (await s.execute(select(SupplyBox).where(SupplyBox.supply_id == sid))).scalars().all()
        for b in rows:
            b.sealed = False
        await s.commit()
    await _render_supply_card(call, sid, user)


# ---------- Files (PDF) ----------
@router.callback_query(F.data.startswith("sup:file:add:"))
async def sup_file_add_hint(call: types.CallbackQuery, state: FSMContext):
    sid = int(call.data.split(":")[-1])
    await state.update_data(upload_sup_id=sid)
    await call.message.answer("Пришлите PDF-файл для прикрепления к поставке (application/pdf).")


@router.message(F.document)
async def sup_file_upload(msg: types.Message, user: User, state: FSMContext):
    data = await state.get_data()
    sid = data.get("upload_sup_id")
    if not sid:
        return
    doc = msg.document
    if not doc or (doc.mime_type != "application/pdf"):
        return await msg.answer("Нужен PDF.")
    async with get_session() as s:
        s.add(SupplyFile(supply_id=int(sid), file_id=doc.file_id, filename=doc.file_name or "file.pdf", uploaded_by=user.id))
        await s.commit()
    await msg.answer("PDF прикреплён.")


# ---------- Cancel (FSM) ----------
@router.callback_query(F.data == "sup:cancel")
async def sup_cancel(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("Операция отменена.", reply_markup=kb_sup_tabs(UserRole.manager))  # роль не знаем тут ⇒ вернёмся из меню


# ---------- Registrar ----------
from aiogram import Dispatcher
def register_supplies_handlers(dp: Dispatcher):
    dp.include_router(router)
