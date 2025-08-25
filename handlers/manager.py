# handlers/manager.py
from __future__ import annotations

from aiogram import Router, F, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database.db import get_session
from database.models import (
    User,
    Supply, SupplyItem, Product, Warehouse, StockMovement,
    ProductStage, MovementType,
)

router = Router()
PAGE = 10


def kb_pick_list(items, page: int = 0) -> InlineKeyboardMarkup:
    """
    items: list of tuples (supply_id, warehouse_name, items_count)
    """
    start = page * PAGE
    chunk = items[start:start + PAGE]

    rows = [[InlineKeyboardButton(
        text=f"#{sid} | {wh} | позиций {cnt}",
        callback_data=f"pick:view:{sid}"
    )] for sid, wh, cnt in chunk]

    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"pick:list:{page-1}"))
    if start + PAGE < len(items):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"pick:list:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="supplies")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_pick_card(sid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Собрано", callback_data=f"pick:done:{sid}")],
        [InlineKeyboardButton(text="⬅️ К списку", callback_data="pick:list:0")],
    ])


async def _packed(session: AsyncSession, wh: int, pid: int) -> int:
    val = (await session.execute(
        select(func.coalesce(func.sum(StockMovement.qty), 0))
        .where(StockMovement.warehouse_id == wh)
        .where(StockMovement.product_id == pid)
        .where(StockMovement.stage == ProductStage.packed)
    )).scalar()
    return int(val or 0)


async def _raw(session: AsyncSession, wh: int, pid: int) -> int:
    val = (await session.execute(
        select(func.coalesce(func.sum(StockMovement.qty), 0))
        .where(StockMovement.warehouse_id == wh)
        .where(StockMovement.product_id == pid)
        .where(StockMovement.stage == ProductStage.raw)
    )).scalar()
    return int(val or 0)


@router.callback_query(F.data == "picking")
@router.callback_query(F.data.startswith("pick:list:"))
async def pick_list(call: types.CallbackQuery, user: User):
    """
    Список поставок со статусом on_picking (постранично)
    """
    page = int(call.data.split(":")[-1]) if call.data.startswith("pick:list:") else 0
    async with get_session() as s:
        rows = (await s.execute(
            select(
                Supply.id,
                Warehouse.name,                         # показываем имя склада
                func.count(SupplyItem.id)
            )
            .join(Warehouse, Warehouse.id == Supply.warehouse_id)
            .join(SupplyItem, SupplyItem.supply_id == Supply.id, isouter=True)
            .where(Supply.status == "on_picking")
            .group_by(Supply.id, Warehouse.name)
            .order_by(Supply.id.desc())
        )).all()

    # items = [(sid, wh_name, cnt), ...]
    items = [(r[0], r[1], int(r[2])) for r in rows]
    await call.message.edit_text("🧰 Задания на сборку:", reply_markup=kb_pick_list(items, page))


@router.callback_query(F.data.startswith("pick:view:"))
async def pick_view(call: types.CallbackQuery, user: User):
    """
    Карточка конкретной поставки: показываем потребности и текущие остатки RAW/PACKED
    """
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = (await s.execute(select(Supply).where(Supply.id == sid))).scalar_one_or_none()
        if not sup:
            return await call.answer("Поставка не найдена", show_alert=True)

        wh_name = (await s.execute(select(Warehouse.name).where(Warehouse.id == sup.warehouse_id))).scalar_one()

        # список позиций поставки
        items = (await s.execute(
            select(SupplyItem.product_id, SupplyItem.qty)
            .where(SupplyItem.supply_id == sid)
        )).all()

        lines = []
        for pid, need in items:
            name = (await s.execute(select(Product.name).where(Product.id == pid))).scalar_one() or f"#{pid}"
            packed = await _packed(s, sup.warehouse_id, pid)
            raw = await _raw(s, sup.warehouse_id, pid)
            lines.append(f"• {name}: нужно {need} | PACKED {packed} | RAW {raw}")

    text = f"📦 Поставка #{sid}\nСклад: {wh_name}\nСтатус: on_picking\n\n" + "\n".join(lines)
    await call.message.edit_text(text, reply_markup=kb_pick_card(sid))


@router.callback_query(F.data.startswith("pick:done:"))
async def pick_done(call: types.CallbackQuery, user: User):
    """
    Авто-доупаковка дефицитов (если хватает RAW) и перевод поставки в status='picked'
    Все движения пишем с user_id = user.id (а не tg_id).
    """
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = (await s.execute(select(Supply).where(Supply.id == sid))).scalar_one_or_none()
        if not sup:
            return await call.answer("Поставка не найдена", show_alert=True)

        rows = (await s.execute(
            select(SupplyItem.product_id, SupplyItem.qty)
            .where(SupplyItem.supply_id == sid)
        )).all()

        # проверим дефициты
        shortages = []
        for pid, need in rows:
            packed = await _packed(s, sup.warehouse_id, pid)
            deficit = max(0, need - packed)
            if deficit:
                raw = await _raw(s, sup.warehouse_id, pid)
                if raw < deficit:
                    shortages.append((pid, need, packed, raw, deficit))

        if shortages:
            # покажем первые 3 строки, чтобы не улететь лимит алерта
            lines = [
                f"⛔ Недостаточно сырья: товар #{pid} | нужно {need} | PACKED {packed} | RAW {raw} | дефицит {deficit}"
                for pid, need, packed, raw, deficit in shortages
            ]
            msg = "\n".join(lines[:3]) + ("..." if len(lines) > 3 else "")
            return await call.answer(msg, show_alert=True)

        # доупаковка дефицитов (raw- / packed+)
        for pid, need in rows:
            packed = await _packed(s, sup.warehouse_id, pid)
            deficit = max(0, need - packed)
            if deficit:
                # raw -deficit
                s.add(StockMovement(
                    warehouse_id=sup.warehouse_id,
                    product_id=pid,
                    qty=-deficit,
                    type=MovementType.upakovka,
                    stage=ProductStage.raw,
                    user_id=user.id,                        # <-- ВАЖНО: id из таблицы users
                    comment=f"auto pack for supply#{sid}",
                ))
                # packed +deficit
                s.add(StockMovement(
                    warehouse_id=sup.warehouse_id,
                    product_id=pid,
                    qty=deficit,
                    type=MovementType.upakovka,
                    stage=ProductStage.packed,
                    user_id=user.id,                        # <-- ВАЖНО: id из таблицы users
                    comment=f"auto pack for supply#{sid}",
                ))

        sup.status = "picked"
        await s.commit()

    await call.message.edit_text(f"✅ Поставка #{sid} отмечена как «Собрано».")
