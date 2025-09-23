# handlers/msk_inbound.py
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional, Tuple, List, Dict

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select

from database.db import get_session
from database.models import (
    MovementType, ProductStage,
    CnPurchase,  # нужен для кода и таймлайна
    CnPurchaseStatus,
    MskInboundDoc, MskInboundItem, MskInboundStatus,
    Warehouse, Product, StockMovement, User,
)

router = Router()

# ========= safe edit =========
async def safe_edit_text(msg: Message, text: str):
    try:
        await msg.edit_text(text)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            await msg.answer(text)

async def safe_edit_reply_markup(msg: Message, markup: InlineKeyboardMarkup | None):
    try:
        await msg.edit_reply_markup(reply_markup=markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            if markup:
                await msg.answer("⬇️", reply_markup=markup)

# ========= helpers =========
_re_int = re.compile(r"(\d+)")
_DOCNAME_RE = re.compile(r"\[(?:DOCNAME|NAME)\s*:\s*([^\]]+)\]", re.IGNORECASE)

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

def fmt_dt(dt: datetime | None) -> str:
    return dt.strftime("%d.%m.%Y %H:%M") if dt else "—"

def docname_from_text(text: Optional[str]) -> Optional[str]:
    """Достаёт [DOCNAME: ...] из комментария, если есть."""
    if not text:
        return None
    m = _DOCNAME_RE.search(text)
    return m.group(1).strip() if m else None

# ========= keyboards =========
def msk_root_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚚 Доставка в РФ",          callback_data="msk:list:in_ru")],
        [InlineKeyboardButton(text="🏢 Доставка на наш склад",  callback_data="msk:list:to_our")],
        [InlineKeyboardButton(text="🗄️ Архив",                  callback_data="msk:list:archive")],
        [InlineKeyboardButton(text="⬅️ Назад",                  callback_data="back_to_menu")],
    ])

def msk_doc_kb(msk_id: int, status: MskInboundStatus, warehouse_id: Optional[int], cn_id: Optional[int]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    if cn_id:
        rows.append([InlineKeyboardButton(text="👀 Фото CN", callback_data=f"cn:photos:{cn_id}:1")])

    if status == MskInboundStatus.PENDING and not warehouse_id:
        rows.append([InlineKeyboardButton(
            text="➡️ Перевести: Доставка на наш склад",
            callback_data=f"msk:to_our:{msk_id}"
        )])
    if status == MskInboundStatus.PENDING and warehouse_id:
        rows.append([InlineKeyboardButton(
            text="✅ Принято (оприходовать)",
            callback_data=f"msk:deliver:{msk_id}"
        )])

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="msk:root")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def msk_wh_kb(msk_id: int, warehouses: list[Warehouse]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    for w in warehouses:
        buttons.append([InlineKeyboardButton(text=w.name, callback_data=f"msk:whchoose:{msk_id}:{w.id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"msk:open:{msk_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ========= entry =========
@router.message(F.text == "Склад МСК")
async def msk_entry(msg: Message):
    await msg.answer("Раздел «Склад МСК».", reply_markup=None)
    await msg.answer("Выберите:", reply_markup=msk_root_kb())

@router.callback_query(F.data == "msk:root")
async def msk_root(cb: CallbackQuery):
    await safe_edit_text(cb.message, "Раздел «Склад МСК».")
    await safe_edit_reply_markup(cb.message, msk_root_kb())
    await cb.answer()

# ========= lists =========
@router.callback_query(F.data.startswith("msk:list:"))
async def msk_list(cb: CallbackQuery):
    mode = cb.data.split(":")[-1]  # in_ru | to_our | archive
    async with get_session() as s:
        all_rows = (await s.execute(select(MskInboundDoc).order_by(MskInboundDoc.created_at.desc()))).scalars().all()

        # подгружаем коды CN одним запросом
        cn_ids = [r.cn_purchase_id for r in all_rows if r and r.cn_purchase_id]
        cn_map: Dict[int, str] = {}
        if cn_ids:
            cn_rows = (await s.execute(select(CnPurchase.id, CnPurchase.code).where(CnPurchase.id.in_(cn_ids)))).all()
            cn_map = {i: code for i, code in cn_rows}

    if mode == "in_ru":
        rows = [r for r in all_rows if r.status == MskInboundStatus.PENDING and not r.warehouse_id]
        title = "🚚 Доставка в РФ"
    elif mode == "to_our":
        rows = [r for r in all_rows if r.status == MskInboundStatus.PENDING and r.warehouse_id]
        title = "🏢 Доставка на наш склад"
    else:
        rows = [r for r in all_rows if r.status == MskInboundStatus.RECEIVED]
        title = "🗄️ Архив"

    if not rows:
        await safe_edit_text(cb.message, f"{title}\n\nСписок пуст.")
        await safe_edit_reply_markup(cb.message, InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="msk:root")]
        ]))
        await cb.answer()
        return

    kb_rows: list[list[InlineKeyboardButton]] = []
    for r in rows:
        # имя документа: DOCNAME из комментария MSK или код CN
        human = docname_from_text(r.comment) or cn_map.get(r.cn_purchase_id, f"CN#{r.cn_purchase_id}")
        kb_rows.append([InlineKeyboardButton(
            text=f"📦 {human} · MSK #{r.id}",
            callback_data=f"msk:open:{r.id}"
        )])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="msk:root")])

    await safe_edit_text(cb.message, title)
    await safe_edit_reply_markup(cb.message, InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await cb.answer()

# ========= open doc =========
async def _fetch_msk_view(msk_id: int):
    async with get_session() as s:
        msk = await s.get(MskInboundDoc, msk_id)
        items = (await s.execute(select(MskInboundItem).where(MskInboundItem.msk_inbound_id == msk_id))).scalars().all()

        pmap = {}
        if items:
            pids = [it.product_id for it in items]
            prows = (await s.execute(select(Product).where(Product.id.in_(pids)))).scalars().all()
            pmap = {p.id: p for p in prows}

        wh_name = msk.warehouse.name if msk and msk.warehouse else None

        # связанный CN
        cn = await s.get(CnPurchase, msk.cn_purchase_id) if msk else None

    return msk, items, pmap, wh_name, cn

async def render_msk_doc(msg: Message, msk_id: int):
    msk, items, pmap, wh_name, cn = await _fetch_msk_view(msk_id)
    if not msk:
        await safe_edit_text(msg, "Документ не найден или удалён.")
        return

    if msk.status == MskInboundStatus.PENDING and not msk.warehouse_id:
        status_text = "🚚 Доставка в РФ"
    elif msk.status == MskInboundStatus.PENDING and msk.warehouse_id:
        status_text = "🏢 Доставка на наш склад"
    else:
        status_text = "🗄️ Принято (архив)"

    # читаем «человеческое имя»: DOCNAME или код CN
    docname = docname_from_text(msk.comment) or (getattr(cn, "code", None) or f"CN#{msk.cn_purchase_id}")

    lines = [
        f"📦 {docname} · MSK-док #{msk.id}",
        f"Статус: {status_text}",
        f"Склад назначения: {wh_name or '—'}",
        f"💬 Комментарий: {getattr(msk, 'comment', None) or '—'}",
        "",
        "🧱 Позиции:",
    ]
    if not items:
        lines.append("— нет позиций —")
    else:
        for it in items:
            p = pmap.get(it.product_id)
            title = f"{p.name} · {p.article}" if p else f"id={it.product_id}"
            price = f"{(it.unit_cost_rub or 0):.2f}"
            lines.append(f"• {title} — {it.qty} шт. × {price} ₽")

    # Полная хронология (как в CN: 1–6)
    lines += [
        "",
        "🕓 Хронология:",
        f"1) Создан: {fmt_dt(getattr(cn, 'created_at', None))}",
        f"2) Отправлен в карго: {fmt_dt(getattr(cn, 'sent_to_cargo_at', None))}",
        f"3) Переведён в отправку на склад МСК: {fmt_dt(getattr(cn, 'sent_to_msk_at', None))}",
        f"4) Отправлен на региональный склад: {fmt_dt(getattr(msk, 'to_our_at', None))}",
        f"5) Приходован на склад: {fmt_dt(getattr(msk, 'received_at', None))}",
        f"6) Архивирован: {fmt_dt(getattr(cn, 'archived_at', None))}",
    ]

    await safe_edit_text(msg, "\n".join(lines))
    await safe_edit_reply_markup(msg, msk_doc_kb(msk.id, msk.status, msk.warehouse_id, msk.cn_purchase_id))

@router.callback_query(F.data.startswith("msk:open:"))
async def msk_open(cb: CallbackQuery):
    parts = cb.data.split(":")
    # msk:open:by_cn:{cn_id}
    if len(parts) >= 3 and parts[2] == "by_cn":
        cn_id = last_int(cb.data)
        async with get_session() as s:
            msk = (await s.execute(select(MskInboundDoc).where(MskInboundDoc.cn_purchase_id == cn_id))).scalar_one_or_none()
        if not msk:
            await cb.answer("MSK-документ не найден.", show_alert=True)
            return
        msk_id = msk.id
    else:
        msk_id = last_int(cb.data)

    if not msk_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return

    await render_msk_doc(cb.message, msk_id)
    await cb.answer()

# ========= choose target warehouse =========
@router.callback_query(F.data.startswith("msk:to_our:"))
async def msk_to_our(cb: CallbackQuery):
    msk_id = last_int(cb.data)
    if not msk_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return

    async with get_session() as s:
        warehouses = (await s.execute(select(Warehouse).order_by(Warehouse.name.asc()))).scalars().all()
    if not warehouses:
        await cb.answer("Нет доступных складов.", show_alert=True)
        return

    await safe_edit_text(cb.message, "Выберите склад назначения:")
    await safe_edit_reply_markup(cb.message, msk_wh_kb(msk_id, warehouses))
    await cb.answer()

@router.callback_query(F.data.startswith("msk:whchoose:"))
async def msk_whchoose(cb: CallbackQuery):
    msk_id, wh_id = last_two_ints(cb.data)
    if not msk_id or not wh_id:
        await cb.answer("Не удалось определить склад.", show_alert=True)
        return

    async with get_session() as s:
        w = await s.get(Warehouse, wh_id)
        if not w:
            await cb.answer("Склад не найден. Выберите другой.", show_alert=True)
            return

        msk = await s.get(MskInboundDoc, msk_id)
        msk.warehouse_id = wh_id
        if not getattr(msk, "to_our_at", None):
            msk.to_our_at = datetime.utcnow()

        # Архивируем CN при выборе склада (как и раньше)
        cn = await s.get(CnPurchase, msk.cn_purchase_id)
        cn.status = CnPurchaseStatus.DELIVERED_TO_MSK
        if hasattr(cn, "archived_at"):
            cn.archived_at = datetime.utcnow()

        await s.commit()

    await render_msk_doc(cb.message, msk_id)
    await cb.answer("Склад выбран. Теперь нажмите «✅ Принято (оприходовать)».", show_alert=True)

# ========= deliver (create stock movements) =========
@router.callback_query(F.data.startswith("msk:deliver:"))
async def msk_deliver(cb: CallbackQuery):
    msk_id = last_int(cb.data)
    if not msk_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return

    async with get_session() as s:
        msk = await s.get(MskInboundDoc, msk_id)
        if not msk:
            await cb.answer("Документ не найден.", show_alert=True)
            return
        if not msk.warehouse_id:
            await cb.answer("Не выбран склад назначения.", show_alert=True)
            return

        cn = await s.get(CnPurchase, msk.cn_purchase_id) if msk.cn_purchase_id else None
        cn_code = getattr(cn, "code", None)

        db_user = (await s.execute(
            select(User).where(User.telegram_id == cb.from_user.id)
        )).scalar_one_or_none()
        user_id = db_user.id if db_user else None

        items = (await s.execute(
            select(MskInboundItem).where(MskInboundItem.msk_inbound_id == msk_id)
        )).scalars().all()

        if not items:
            await cb.answer("В документе нет позиций.", show_alert=True)
            return

        # имя документа: сначала DOCNAME из комментария MSK, иначе CN-код, иначе MSK #
        docname = docname_from_text(msk.comment) or cn_code or f"MSK#{msk.id}"

        # единый комментарий с маркером DOCNAME
        base_comment = "Оприходовано со склада МСК"
        comment_full = f"[DOCNAME: {docname}] {base_comment}: MSK #{msk.id}" + (f" (из {cn_code})" if cn_code else "")

        now = datetime.utcnow()
        # под одним doc_id — групповое поступление
        # определение doc_id перенесено в СУБД (автоинкремент StockMovement.doc_id отсутствует),
        # поэтому используем «следующий»: max(doc_id)+1 среди прихода.
        max_doc = (await s.execute(
            select(StockMovement.doc_id).where(StockMovement.type == MovementType.prihod).order_by(StockMovement.doc_id.desc())
        )).scalars().first()
        next_doc = (max_doc or 0) + 1

        for it in items:
            s.add(StockMovement(
                type=MovementType.prihod,
                stage=ProductStage.raw,
                qty=it.qty,
                product_id=it.product_id,
                warehouse_id=msk.warehouse_id,
                date=now,
                user_id=user_id,
                doc_id=next_doc,
                comment=comment_full,
            ))

        msk.status = MskInboundStatus.RECEIVED
        msk.received_at = now
        msk.received_by_user_id = user_id

        await s.commit()

    await cb.answer("Принято. Поступление создано, документ перенесён в Архив.", show_alert=True)
    await render_msk_doc(cb.message, msk_id)
