# handlers/supplies.py
from aiogram import Router, F, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database.db import get_session
from database.models import (
    Warehouse, Product, StockMovement,
    Supply, SupplyItem, User,
    MovementType, ProductStage, UserRole
)

router = Router()
PAGE = 10

# ---------- FSM ----------
class SupFSM(StatesGroup):
    MP = State()
    WH = State()
    ITEMS = State()
    QTY = State()
    CONFIRM = State()

# ---------- Keyboards ----------
def kb_sup_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🆕 Создать поставку", callback_data="sup:new")],
        [InlineKeyboardButton(text="📋 Задания на сборку", callback_data="pick:list:0")],
        [InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")],
    ])

def kb_mp() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Wildberries", callback_data="sup:mp:wb")],
        [InlineKeyboardButton(text="Ozon",        callback_data="sup:mp:ozon")],
        [InlineKeyboardButton(text="❌ Отмена",    callback_data="sup:cancel")],
    ])

def kb_wh_list(warehouses, page=0) -> InlineKeyboardMarkup:
    start = page * PAGE
    chunk = warehouses[start:start+PAGE]
    rows = [[InlineKeyboardButton(text=name, callback_data=f"sup:wh:{wid}")]
            for wid, name in chunk]
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"sup:wh:page:{page-1}"))
    if start + PAGE < len(warehouses):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"sup:wh:page:{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="sup:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_products_packed(products, page: int, wh_id: int) -> InlineKeyboardMarkup:
    start = page * PAGE
    chunk = products[start:start+PAGE]
    rows = [[InlineKeyboardButton(
        text=f"{name} (art. {article}) — PACKED {packed}",
        callback_data=f"sup:add:{wh_id}:{pid}"
    )] for pid, name, article, packed in chunk]
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"sup:prod:page:{page-1}"))
    if start + PAGE < len(products):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"sup:prod:page:{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton(text="📩 Отправить на сборку", callback_data="sup:submit")])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="sup:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить ещё", callback_data="sup:more")],
        [InlineKeyboardButton(text="📩 Отправить на сборку", callback_data="sup:submit")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="sup:cancel")],
    ])

# ---------- Helpers ----------
async def _warehouses_list() -> list[tuple[int,str]]:
    async with get_session() as s:
        rows = (await s.execute(
            select(Warehouse.id, Warehouse.name)
            .where((Warehouse.is_active.is_(True)) | (Warehouse.is_active.is_(None)))
            .order_by(Warehouse.name.asc())
        )).all()
    items = [(r[0], r[1]) for r in rows]
    # ренейм дублей
    counts = {}
    for _, n in items: counts[n] = counts.get(n, 0) + 1
    return [(wid, name if counts[name]==1 else f"{name} (#{wid})") for wid, name in items]

async def _products_with_packed(session: AsyncSession, warehouse_id: int) -> list[tuple[int,str,str,int]]:
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

# ---------- Entry ----------
@router.callback_query(F.data == "supplies")
async def supplies_root(call: types.CallbackQuery):
    await call.message.edit_text("Раздел «Поставки»", reply_markup=kb_sup_root())

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
        warn = f"⚠️ Упаковано PACKED={packed}, вы добавляете {qty}. Дефицит {qty-packed}. Разрешаю — будет доупаковано на «Собрано»."
    cart = data.get("cart", {})
    cart[pid] = cart.get(pid, 0) + qty
    await state.update_data(cart=cart)
    await state.set_state(SupFSM.CONFIRM)

    # карточка
    async with get_session() as s:
        prod = (await s.execute(select(Product.name, Product.article).where(Product.id==pid))).first()
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
    cart: dict[int,int] = data.get("cart", {})
    if not cart:
        return await call.answer("Корзина пуста", show_alert=True)

    mp = data["mp"]
    wh_id = data["wh_id"]

    async with get_session() as s:
        # создаём supply
        sup = Supply(
            warehouse_id=wh_id,
            created_by=user.id,
            status="on_picking",
             )
        s.add(sup)
        await s.flush()
        sup_id = sup.id
        # позиции
        for pid, qty in cart.items():
            s.add(SupplyItem(supply_id=sup_id, product_id=pid, qty=qty))
        await s.commit()

    # уведомление менеджерам
    async with get_session() as s:
        managers = (await s.execute(select(User.telegram_id).where(User.role == UserRole.manager))).scalars().all()
    lines = []
    async with get_session() as s:
        for pid, qty in cart.items():
            name = (await s.execute(select(Product.name).where(Product.id==pid))).scalar_one()
            lines.append(f"• {name} — {qty}")
    text = "📦 Новая поставка на сборку\n" \
           f"MP: {mp.upper()} | Склад #{wh_id}\n" \
           f"Позиции:\n" + "\n".join(lines) + f"\n\nID поставки: #{sup_id}"

    for tg_id in managers:
        try:
            await call.message.bot.send_message(tg_id, text)
        except Exception:
            pass

    await state.clear()
    await call.message.edit_text(f"Поставка #{sup_id} отправлена менеджерам «на сборку».")
# внизу handlers/supplies.py
from aiogram import Dispatcher

def register_supplies_handlers(dp: Dispatcher):
    dp.include_router(router)
