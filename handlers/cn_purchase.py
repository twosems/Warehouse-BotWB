from __future__ import annotations

import re
from datetime import datetime
from typing import Optional, Tuple, List

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select, or_, func

from database.db import get_session
from database.models import (
    CnPurchase, CnPurchaseItem, CnPurchaseStatus,
    MskInboundDoc, MskInboundItem,
    Product,
)

router = Router()
PAGE_SIZE = 8

# -------- safe edit ----------
async def safe_edit_text(msg: Message, text: str):
    try:
        await msg.edit_text(text)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            raise

async def safe_edit_reply_markup(msg: Message, markup: InlineKeyboardMarkup | None):
    try:
        await msg.edit_reply_markup(reply_markup=markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            raise

# -------- helpers ----------
def fmt_dt(dt: datetime | None) -> str:
    if not dt:
        return "—"
    # локально: DD.MM.YYYY HH:MM
    return dt.strftime("%d.%m.%Y %H:%M")

# -------- cb parsers ----------
_re_int = re.compile(r"(\d+)")

def last_int(data: str) -> Optional[int]:
    if not data:
        return None
    m = _re_int.findall(data)
    return int(m[-1]) if m else None

def last_two_ints(data: str) -> Tuple[Optional[int], Optional[int]]:
    if not data:
        return None, None
    m = _re_int.findall(data)
    if not m:
        return None, None
    if len(m) == 1:
        return None, int(m[0])
    return int(m[-2]), int(m[-1])

# -------- FSM ----------
class CnCreateState(StatesGroup):
    picking_product = State()   # список товаров
    waiting_qty = State()       # ввод количества
    waiting_cost = State()      # ввод цены
    entering_search = State()   # ввод строки поиска
    confirm_item = State()      # подтверждение (создать/добавить/назад)
    editing_comment = State()   # ✏️ изменение комментария из карточки

# -------- Keyboards ----------
def cn_root_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Доставляется в карго", callback_data="cn:list:cargo")],
        [InlineKeyboardButton(text="🚚 Доставляется в РФ",    callback_data="cn:list:ru")],
        [InlineKeyboardButton(text="🗄️ Архив",                callback_data="cn:list:archive")],
        [InlineKeyboardButton(text="➕ Создать документ",      callback_data="cn:new")],
        [InlineKeyboardButton(text="⬅️ Назад",                 callback_data="back_to_menu")],
    ])

def cn_doc_actions_kb(doc_id: int, status: CnPurchaseStatus) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if status == CnPurchaseStatus.SENT_TO_CARGO:
        rows.append([InlineKeyboardButton(
            text="➡️ Перевести: Доставка склад МСК",
            callback_data=f"cn:status:{doc_id}:to_msk"
        )])
        rows.append([InlineKeyboardButton(text="➕ Добавить позицию", callback_data=f"cn:item:add:{doc_id}")])
        rows.append([InlineKeyboardButton(text="✏️ Комментарий", callback_data=f"cn:comment:edit:{doc_id}")])
    elif status == CnPurchaseStatus.SENT_TO_MSK:
        rows.append([InlineKeyboardButton(text="🏢 Открыть в «Склад МСК»", callback_data=f"msk:open:by_cn:{doc_id}")])
        rows.append([InlineKeyboardButton(text="✏️ Комментарий", callback_data=f"cn:comment:edit:{doc_id}")])
    else:
        # Архив CN — только просмотр
        pass

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="cn:root")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def cn_lists_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="cn:root")]
    ])

# -------- Products picker ----------
async def fetch_products(search: Optional[str], page: int) -> tuple[list[Product], int]:
    async with get_session() as s:
        q = select(Product).where(Product.is_active.is_(True))
        if search:
            like = f"%{search.strip()}%"
            q = q.where(or_(Product.name.ilike(like), Product.article.ilike(like)))
        total = (await s.execute(select(func.count()).select_from(q.subquery()))).scalar_one()
        q = q.order_by(Product.name.asc()).offset(page * PAGE_SIZE).limit(PAGE_SIZE)
        rows = (await s.execute(q)).scalars().all()
    return rows, int(total)

def product_picker_kb(doc_id: int, page: int, total: int, rows: list[Product], search: Optional[str]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    for p in rows:
        cap = f"{p.name} · {p.article}"
        buttons.append([InlineKeyboardButton(text=cap, callback_data=f"cn:prod:choose:{doc_id}:{p.id}")])

    max_page = max((total - 1) // PAGE_SIZE, 0)
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"cn:prod:list:{doc_id}:{page-1}"))
    if page < max_page:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"cn:prod:list:{doc_id}:{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton(
        text=("🔎 Изменить поиск" if search else "🔎 Поиск"),
        callback_data=f"cn:prod:search:{doc_id}:{page}"
    )])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад к документу", callback_data=f"cn:open:{doc_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def show_product_picker(msg: Message, doc_id: int, state: FSMContext, page: int = 0):
    data = await state.get_data()
    search = data.get("cn_search_text")
    rows, total = await fetch_products(search, page)
    text = "Выберите товар из базы" + (f" (поиск: `{search}`)" if search else "") + f"\nВсего найдено: {total}"
    await safe_edit_text(msg, text)
    await safe_edit_reply_markup(msg, product_picker_kb(doc_id, page, total, rows, search))
    await state.set_state(CnCreateState.picking_product)

# -------- Entry --------
@router.message(F.text == "Закупка CN")
async def cn_entry(msg: Message):
    await msg.answer("Раздел «Закупка CN».", reply_markup=None)
    await msg.answer("Выберите:", reply_markup=cn_root_kb())

@router.callback_query(F.data == "cn:root")
async def cn_root(cb: CallbackQuery):
    await safe_edit_text(cb.message, "Раздел «Закупка CN».")
    await safe_edit_reply_markup(cb.message, cn_root_kb())
    await cb.answer()

# -------- Lists as buttons --------
@router.callback_query(F.data.startswith("cn:list:"))
async def cn_list(cb: CallbackQuery):
    mode = cb.data.split(":")[-1]  # cargo | ru | archive
    async with get_session() as s:
        rows = (await s.execute(select(CnPurchase).order_by(CnPurchase.created_at.desc()))).scalars().all()

    if mode == "cargo":
        rows = [r for r in rows if r.status == CnPurchaseStatus.SENT_TO_CARGO]
        title = "📦 Доставляется в карго"
    elif mode == "ru":
        rows = [r for r in rows if r.status == CnPurchaseStatus.SENT_TO_MSK]
        title = "🚚 Доставляется в РФ"
    else:
        rows = [r for r in rows if r.status == CnPurchaseStatus.DELIVERED_TO_MSK]
        title = "🗄️ Архив"

    if not rows:
        await safe_edit_text(cb.message, f"{title}\n\nСписок пуст.")
        await safe_edit_reply_markup(cb.message, cn_lists_kb())
        await cb.answer()
        return

    kb_rows: list[list[InlineKeyboardButton]] = []
    for r in rows:
        kb_rows.append([InlineKeyboardButton(
            text=f"📄 {r.code} — {r.status.value}",
            callback_data=f"cn:open:{r.id}"
        )])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="cn:root")])

    await safe_edit_text(cb.message, title)
    await safe_edit_reply_markup(cb.message, InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await cb.answer()

# -------- Create: initial status = SENT_TO_CARGO -> picker --------
@router.callback_query(F.data == "cn:new")
async def cn_new(cb: CallbackQuery, state: FSMContext):
    code = "CN-" + datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    async with get_session() as s:
        doc = CnPurchase(code=code, status=CnPurchaseStatus.SENT_TO_CARGO, comment=None)
        # таймстемпы
        if hasattr(doc, "sent_to_cargo_at"):
            doc.sent_to_cargo_at = datetime.utcnow()
        s.add(doc)
        await s.flush()
        doc_id = doc.id
        await s.commit()

    await state.update_data(
        cn_doc_id=doc_id, cn_search_text=None,
        selected_product_id=None, qty=None, cost=None
    )

    await safe_edit_text(cb.message, f"Документ создан: #{code}\nЗагрузка списка товаров…")
    await safe_edit_reply_markup(cb.message, None)
    await show_product_picker(cb.message, doc_id, state, page=0)
    await cb.answer()

# -------- Picker / search / choose --------
@router.callback_query(F.data.startswith("cn:item:add:"))
async def cn_item_add_from_card(cb: CallbackQuery, state: FSMContext):
    doc_id = last_int(cb.data)
    if not doc_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return
    await state.update_data(cn_doc_id=doc_id, selected_product_id=None, qty=None, cost=None)
    await show_product_picker(cb.message, doc_id, state, page=0)
    await cb.answer()

@router.callback_query(F.data.startswith("cn:prod:list:"))
async def cn_prod_list(cb: CallbackQuery, state: FSMContext):
    doc_id, page = last_two_ints(cb.data)
    if doc_id is None or page is None:
        await cb.answer("Неверные параметры пагинации.", show_alert=True)
        return
    await show_product_picker(cb.message, doc_id, state, page=page)
    await cb.answer()

@router.callback_query(F.data.startswith("cn:prod:search:"))
async def cn_prod_search(cb: CallbackQuery, state: FSMContext):
    await state.set_state(CnCreateState.entering_search)
    await safe_edit_text(cb.message, "Введите строку поиска (имя или артикул). Отправьте '-' чтобы очистить фильтр.")
    await safe_edit_reply_markup(cb.message, None)
    await cb.answer()

@router.message(CnCreateState.entering_search)
async def cn_receive_search_text(msg: Message, state: FSMContext):
    text = msg.text.strip()
    search = None if text == "-" else text
    await state.update_data(cn_search_text=search)
    data = await state.get_data()
    doc_id = data["cn_doc_id"]
    out = await msg.answer("Поиск обновлён. Загрузка списка…")
    await show_product_picker(out, doc_id, state, page=0)

# -------- Choose -> qty -> cost -> confirm --------
@router.callback_query(F.data.startswith("cn:prod:choose:"))
async def cn_prod_choose(cb: CallbackQuery, state: FSMContext):
    doc_id, product_id = last_two_ints(cb.data)
    if doc_id is None or product_id is None:
        await cb.answer("Не удалось определить товар/документ.", show_alert=True)
        return
    await state.update_data(cn_doc_id=doc_id, selected_product_id=product_id)
    await safe_edit_text(cb.message, "Введите количество единиц (шт.).")
    await safe_edit_reply_markup(cb.message, None)
    await state.set_state(CnCreateState.waiting_qty)
    await cb.answer()

@router.message(CnCreateState.waiting_qty)
async def cn_item_qty(msg: Message, state: FSMContext):
    txt = msg.text.strip()
    if not txt.isdigit():
        await msg.answer("Некорректное число. Введите количество единиц (шт.).")
        return
    await state.update_data(qty=int(txt))
    await msg.answer("Введите стоимость единицы товара (₽).")
    await state.set_state(CnCreateState.waiting_cost)

@router.message(CnCreateState.waiting_cost)
async def cn_item_cost(msg: Message, state: FSMContext):
    raw = msg.text.replace(",", ".").strip()
    try:
        cost = float(raw)
    except Exception:
        await msg.answer("Некорректная цена. Введите стоимость единицы товара (₽).")
        return

    await state.update_data(cost=cost)
    data = await state.get_data()
    async with get_session() as s:
        p = await s.get(Product, data["selected_product_id"])
    name = f"{p.name} · {p.article}" if p else f"product_id={data['selected_product_id']}"

    text = (
        "Добавляем позицию:\n"
        f"• {name}\n"
        f"• Кол-во: {data['qty']} шт.\n"
        f"• Цена: {data['cost']} ₽\n\n"
        "Выберите действие:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧾 Создать документ", callback_data="cn:item:commit:finish")],
        [InlineKeyboardButton(text="➕ Добавить товар",   callback_data="cn:item:commit:add_more")],
        [InlineKeyboardButton(text="⬅️ Назад",            callback_data=f"cn:prod:list:{data['cn_doc_id']}:0")],
    ])
    out = await msg.answer(text, reply_markup=kb)
    await state.update_data(confirm_msg_id=out.message_id)
    await state.set_state(CnCreateState.confirm_item)

async def _commit_item(state: FSMContext):
    data = await state.get_data()
    async with get_session() as s:
        s.add(CnPurchaseItem(
            cn_purchase_id=data["cn_doc_id"],
            product_id=data["selected_product_id"],
            qty=data["qty"],
            unit_cost_rub=data["cost"],
        ))
        await s.commit()

@router.callback_query(F.data == "cn:item:commit:add_more")
async def cn_commit_add_more(cb: CallbackQuery, state: FSMContext):
    await _commit_item(state)
    await state.update_data(selected_product_id=None, qty=None, cost=None)
    await safe_edit_text(cb.message, "Позиция добавлена. Выберите следующий товар:")
    await show_product_picker(cb.message, (await state.get_data())["cn_doc_id"], state, page=0)
    await cb.answer("Добавлено.")

@router.callback_query(F.data == "cn:item:commit:finish")
async def cn_commit_finish(cb: CallbackQuery, state: FSMContext):
    await _commit_item(state)
    data = await state.get_data()
    doc_id = data.get("cn_doc_id")
    if not doc_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return
    await state.update_data(selected_product_id=None, qty=None, cost=None, confirm_msg_id=None)
    await render_doc(cb.message, doc_id)
    await cb.answer("Позиция добавлена, документ открыт.")

# -------- View / comment / status --------
async def render_doc(msg: Message, doc_id: int):
    async with get_session() as s:
        doc = await s.get(CnPurchase, doc_id)
        items = (await s.execute(select(CnPurchaseItem).where(CnPurchaseItem.cn_purchase_id == doc_id))).scalars().all()
        pmap = {}
        if items:
            pids = [it.product_id for it in items]
            prows = (await s.execute(select(Product).where(Product.id.in_(pids)))).scalars().all()
            pmap = {p.id: p for p in prows}

    lines = [
        f"📄 {doc.code} — {doc.status.value}",
        f"💬 Комментарий: {doc.comment or '—'}",
        "",
        "🧱 Позиции:",
    ]
    if not items:
        lines.append("— пока пусто —")
    else:
        for it in items:
            p = pmap.get(it.product_id)
            title = f"{p.name} · {p.article}" if p else f"id={it.product_id}"
            lines.append(f"• {title} — {it.qty} шт. × {it.unit_cost_rub} ₽")

    # Хронология показываем, когда документ в Архиве CN
    if doc.status == CnPurchaseStatus.DELIVERED_TO_MSK:
        lines += [
            "",
            "🕓 Хронология:",
            f"• Создан: {fmt_dt(getattr(doc, 'created_at', None))}",
            f"• Передан на доставку в карго: {fmt_dt(getattr(doc, 'sent_to_cargo_at', None))}",
            f"• Передан на доставку на склад МСК: {fmt_dt(getattr(doc, 'sent_to_msk_at', None))}",
            f"• Архивирован: {fmt_dt(getattr(doc, 'archived_at', None))}",
        ]

    await safe_edit_text(msg, "\n".join(lines))
    await safe_edit_reply_markup(msg, cn_doc_actions_kb(doc_id, doc.status))

@router.callback_query(F.data.startswith("cn:open:"))
async def cn_open(cb: CallbackQuery):
    doc_id = last_int(cb.data)
    if not doc_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return
    await render_doc(cb.message, doc_id)
    await cb.answer()

@router.callback_query(F.data.startswith("cn:comment:edit:"))
async def cn_comment_edit(cb: CallbackQuery, state: FSMContext):
    doc_id = last_int(cb.data)
    if not doc_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return
    async with get_session() as s:
        doc = await s.get(CnPurchase, doc_id)
        if doc.status == CnPurchaseStatus.DELIVERED_TO_MSK:
            await cb.answer("Документ в архиве. Редактирование комментария недоступно.", show_alert=True)
            return
    await state.update_data(cn_doc_id=doc_id)
    await safe_edit_text(cb.message, "Введите новый комментарий (или '-' чтобы очистить):")
    await safe_edit_reply_markup(cb.message, None)
    await state.set_state(CnCreateState.editing_comment)
    await cb.answer()

@router.message(CnCreateState.editing_comment)
async def cn_comment_edit_save(msg: Message, state: FSMContext):
    data = await state.get_data()
    doc_id = data["cn_doc_id"]
    comment = None if msg.text.strip() == "-" else msg.text.strip()
    async with get_session() as s:
        doc = await s.get(CnPurchase, doc_id)
        doc.comment = comment
        await s.commit()
    out = await msg.answer("Комментарий обновлён. Открываю документ…")
    await render_doc(out, doc_id)

@router.callback_query(F.data.startswith("cn:status:"))
async def cn_set_status(cb: CallbackQuery):
    if not cb.data.endswith(":to_msk"):
        await cb.answer("Недоступный переход", show_alert=True)
        return
    doc_id = last_int(cb.data)
    if not doc_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return

    async with get_session() as s:
        doc = await s.get(CnPurchase, doc_id)
        if doc.status != CnPurchaseStatus.SENT_TO_CARGO:
            await cb.answer("Перевод возможен только из статуса «Доставляется в карго».", show_alert=True)
            return
        doc.status = CnPurchaseStatus.SENT_TO_MSK
        if hasattr(doc, "sent_to_msk_at"):
            doc.sent_to_msk_at = datetime.utcnow()
        await s.flush()

        msk = (await s.execute(select(MskInboundDoc).where(MskInboundDoc.cn_purchase_id == doc.id))).scalar_one_or_none()
        if msk is None:
            msk = MskInboundDoc(
                cn_purchase_id=doc.id,
                created_at=datetime.utcnow(),
                created_by_user_id=None,
                comment=f"Из CN #{doc.code}",
            )
            s.add(msk)
            await s.flush()
            items = (await s.execute(select(CnPurchaseItem).where(CnPurchaseItem.cn_purchase_id == doc.id))).scalars().all()
            for it in items:
                s.add(MskInboundItem(
                    msk_inbound_id=msk.id,
                    product_id=it.product_id,
                    qty=it.qty,
                    unit_cost_rub=it.unit_cost_rub,
                ))

        await s.commit()

    await cb.answer("Статус обновлён: документ доступен в «Склад МСК → Доставка в РФ».")
    await render_doc(cb.message, doc_id)

# -------- register --------
def register_cn_purchase_handlers(dp):
    dp.include_router(router)
