# handlers/manager.py
from __future__ import annotations

from typing import List, Tuple

from aiogram import Router, F, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select, func, desc

from database.db import get_session, available_packed
from database.models import (
    User, UserRole,
    Supply, SupplyItem, Warehouse, Product,
    StockMovement, MovementType, ProductStage,
)
from handlers.common import send_content

router = Router()
PAGE = 10

# ---------------------------
# UI helpers
# ---------------------------

def _kb_manager_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 К сборке",     callback_data="mgr:list:queued")],
        [InlineKeyboardButton(text="🛠 В работе",     callback_data="mgr:list:assembling")],
        [InlineKeyboardButton(text="✅ Собранные",    callback_data="mgr:list:assembled")],
        [InlineKeyboardButton(text="🚚 В пути",       callback_data="mgr:list:in_transit")],
        [InlineKeyboardButton(text="⬅️ Назад",        callback_data="back_to_menu")],
    ])

_TITLES = {
    "queued": "📥 К сборке",
    "assembling": "🛠 В работе",
    "assembled": "✅ Собранные",
    "in_transit": "🚚 Доставляется",
    "archived_delivered": "🗄 Доставлена (архив)",
    "archived_returned": "🗄 Возврат (архив)",
    "cancelled": "❌ Отменена",
}

def _kb_list(items: List[Tuple[int, str, int]], page: int, status: str) -> InlineKeyboardMarkup:
    start = page * PAGE
    chunk = items[start:start+PAGE]
    rows: List[List[InlineKeyboardButton]] = []

    for sid, wh_name, cnt in chunk:
        rows.append([InlineKeyboardButton(
            text=f"SUP-{sid} • {wh_name} • позиций {cnt}",
            callback_data=f"mgr:open:{sid}"
        )])

    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"mgr:list:{status}:{page-1}"))
    if start + PAGE < len(items):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"mgr:list:{status}:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="🏠 Меню менеджера", callback_data="manager")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_card(s: Supply) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    # Доступные действия зависят от статуса (см. ТЗ §6.5)
    if s.status == "in_transit":
        rows.append([InlineKeyboardButton(text="✅ Доставлено",          callback_data=f"mgr:delivered:{s.id}")])
        rows.append([InlineKeyboardButton(text="↩️ Возврат",             callback_data=f"mgr:return:{s.id}")])
        rows.append([InlineKeyboardButton(text="♻️ Расформировать",      callback_data=f"mgr:unpost:{s.id}")])

    # Общая навигация
    rows.append([InlineKeyboardButton(text="⬅️ К спискам", callback_data="manager")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------------------------
# Root
# ---------------------------

@router.callback_query(F.data == "manager")
async def manager_root(cb: types.CallbackQuery, user: User):
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return
    await send_content(cb, "Управление поставками:", reply_markup=_kb_manager_root())


# ---------------------------
# Списки по статусам (с пагинацией)
# ---------------------------

@router.callback_query(F.data.startswith("mgr:list:"))
async def mgr_list(cb: types.CallbackQuery, user: User):
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return

    parts = cb.data.split(":")
    # варианты: "mgr:list:queued" или "mgr:list:queued:2"
    status = parts[2]
    page = int(parts[3]) if len(parts) > 3 else 0

    async with get_session() as s:
        rows = (await s.execute(
            select(
                Supply.id,
                Warehouse.name,
                func.count(SupplyItem.id)
            )
            .join(Warehouse, Warehouse.id == Supply.warehouse_id)
            .outerjoin(SupplyItem, SupplyItem.supply_id == Supply.id)
            .where(Supply.status == status)           # ВАЖНО: VARCHAR сравниваем со строкой
            .group_by(Supply.id, Warehouse.name)
            .order_by(Supply.id.desc())
        )).all()

    items: List[Tuple[int, str, int]] = [(r[0], r[1], int(r[2])) for r in rows]
    if not items:
        await send_content(cb, f"{_TITLES.get(status, status)}\n\nСписок пуст.", reply_markup=_kb_manager_root())
        return

    await send_content(
        cb,
        f"{_TITLES.get(status, status)} — выберите поставку:",
        reply_markup=_kb_list(items, page, status)
    )


# ---------------------------
# Карточка поставки (просмотр)
# ---------------------------

@router.callback_query(F.data.startswith("mgr:open:"))
async def mgr_open(cb: types.CallbackQuery, user: User):
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return

    sid = int(cb.data.split(":")[-1])
    async with get_session() as s:
        sup = (await s.execute(select(Supply).where(Supply.id == sid))).scalar_one_or_none()
        if not sup:
            await cb.answer("Поставка не найдена", show_alert=True)
            return

        wh_name = (await s.execute(select(Warehouse.name).where(Warehouse.id == sup.warehouse_id))).scalar_one()
        items = (await s.execute(
            select(SupplyItem.product_id, SupplyItem.qty)
            .where(SupplyItem.supply_id == sid)
            .order_by(SupplyItem.id)
        )).all()

        # Тело карточки + контроль доступности
        lines: List[str] = []
        total_qty = 0
        total_def = 0
        for pid, need in items:
            prod = (await s.execute(select(Product.name, Product.article).where(Product.id == pid))).first()
            name, art = prod if prod else (f"#{pid}", None)
            avail = await available_packed(s, sup.warehouse_id, pid)
            deficit = max(0, need - max(avail, 0))
            total_qty += int(need)
            total_def += int(deficit)
            lines.append(
                f"• `{art or pid}` — *{name}*: план {need}, доступно PACKED {avail}, дефицит {deficit}"
            )

    head = (
        f"📦 Поставка *SUP-{sid}*\n"
        f"🏬 Склад-источник: *{wh_name}*\n"
        f"🧭 Статус: *{sup.status}*\n"
        f"—\n"
    )
    body = "\n".join(lines) if lines else "_Позиции отсутствуют._"
    tail = f"\n\n📈 Итого: {len(items)} позиций, план {total_qty}, суммарный дефицит {total_def}"
    await send_content(cb, head + body + tail, parse_mode="Markdown", reply_markup=_kb_card(sup))


# ---------------------------
# Действия по in_transit (менеджер)
# ---------------------------

async def _next_doc_id() -> int:
    async with get_session() as s:
        max_doc = (await s.execute(select(func.max(StockMovement.doc_id)))).scalar()
        return int((max_doc or 0) + 1)

@router.callback_query(F.data.startswith("mgr:delivered:"))
async def mgr_delivered(cb: types.CallbackQuery, user: User):
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return

    sid = int(cb.data.split(":")[-1])
    async with get_session() as s:
        sup = (await s.execute(select(Supply).where(Supply.id == sid))).scalar_one_or_none()
        if not sup:
            return await cb.answer("Поставка не найдена", show_alert=True)
        if sup.status != "in_transit":
            return await cb.answer("Действие доступно только из статуса in_transit", show_alert=True)

        sup.status = "archived_delivered"
        await s.commit()

    await cb.answer("Отмечено как доставлено.")
    await mgr_open(cb, user)  # перерисовать карточку


@router.callback_query(F.data.startswith("mgr:return:"))
async def mgr_return(cb: types.CallbackQuery, user: User):
    """
    Возврат: приход PACKED по всем позициям поставки и статус -> archived_returned.
    """
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return

    sid = int(cb.data.split(":")[-1])
    async with get_session() as s:
        sup = (await s.execute(select(Supply).where(Supply.id == sid))).scalar_one_or_none()
        if not sup:
            return await cb.answer("Поставка не найдена", show_alert=True)
        if sup.status != "in_transit":
            return await cb.answer("Действие доступно только из статуса in_transit", show_alert=True)

        rows = (await s.execute(
            select(SupplyItem.product_id, SupplyItem.qty)
            .where(SupplyItem.supply_id == sid)
        )).all()

        doc_id = await _next_doc_id()
        for pid, qty in rows:
            s.add(StockMovement(
                warehouse_id=sup.warehouse_id,
                product_id=pid,
                qty=qty,
                type=MovementType.postavka,            # тип «поставка» используем для связанных движений
                stage=ProductStage.packed,
                user_id=user.id,
                doc_id=doc_id,
                comment=f"[SUP-RET {sid}] Возврат из МП",
            ))

        sup.status = "archived_returned"
        await s.commit()

    await cb.answer("Возврат оформлен.")
    await mgr_open(cb, user)


@router.callback_query(F.data.startswith("mgr:unpost:"))
async def mgr_unpost(cb: types.CallbackQuery, user: User):
    """
    Расформировать: приход PACKED (возврат на склад) и статус -> assembled.
    """
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return

    sid = int(cb.data.split(":")[-1])
    async with get_session() as s:
        sup = (await s.execute(select(Supply).where(Supply.id == sid))).scalar_one_or_none()
        if not sup:
            return await cb.answer("Поставка не найдена", show_alert=True)
        if sup.status != "in_transit":
            return await cb.answer("Действие доступно только из статуса in_transit", show_alert=True)

        rows = (await s.execute(
            select(SupplyItem.product_id, SupplyItem.qty)
            .where(SupplyItem.supply_id == sid)
        )).all()

        doc_id = await _next_doc_id()
        for pid, qty in rows:
            s.add(StockMovement(
                warehouse_id=sup.warehouse_id,
                product_id=pid,
                qty=qty,
                type=MovementType.postavka,
                stage=ProductStage.packed,
                user_id=user.id,
                doc_id=doc_id,
                comment=f"[SUP-UNPOST {sid}] Расформирование поставки",
            ))

        sup.status = "assembled"   # вернули в собранные; короба открываются — реализуется в карточке/коробах
        await s.commit()

    await cb.answer("Поставка расформирована.")
    await mgr_open(cb, user)


# ---------------------------
# Регистрация
# ---------------------------

def register_manager_handlers(dp):
    dp.include_router(router)
