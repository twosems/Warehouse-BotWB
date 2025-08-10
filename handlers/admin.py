# handlers/admin.py
import logging
from aiogram import types, Bot, Dispatcher
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from sqlalchemy import select, func, update
from sqlalchemy.exc import IntegrityError

from database.db import get_session, ensure_core_data
from database.models import (
    User, UserRole,
    Warehouse, Product,
    StockMovement, Supply, SupplyItem
)
from handlers.common import send_content


# ---------- FSM ----------
class AdminState(StatesGroup):
    selecting_action = State()
    selecting_user = State()
    entering_message = State()

class ProductState(StatesGroup):
    entering_article = State()
    entering_name = State()
    confirming = State()

class WarehouseCreateState(StatesGroup):
    entering_name = State()

class WarehouseRenameState(StatesGroup):
    entering_name = State()

class ProductEditState(StatesGroup):
    selecting = State()
    renaming = State()


# ---------- Клавиатуры ----------
def kb_admin_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏬 Склады", callback_data="admin_wh")],
        [InlineKeyboardButton(text="📦 Товары", callback_data="admin_prod")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton(text="Назад в меню", callback_data="back_to_menu")],
    ])

def kb_admin_users() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Список пользователей", callback_data="admin_list_users")],
        [InlineKeyboardButton(text="Удалить пользователя", callback_data="admin_delete_user")],
        [InlineKeyboardButton(text="Отправить сообщение", callback_data="admin_send_message")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
    ])

def kb_admin_wh_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Список", callback_data="admin_wh_list")],
        [InlineKeyboardButton(text="Добавить", callback_data="admin_wh_add")],
        [InlineKeyboardButton(text="Редактировать", callback_data="admin_wh_edit")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
    ])

def kb_wh_edit_pick(whs) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{w.name} ({'✅' if w.is_active else '🚫'})", callback_data=f"admin_wh_pick:{w.id}")]
        for w in whs
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_wh")])
    return kb

def kb_wh_actions(wh: Warehouse) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"admin_wh_rename:{wh.id}")],
        [InlineKeyboardButton(
            text=("🟢 Активировать" if not wh.is_active else "🔴 Деактивировать"),
            callback_data=f"admin_wh_toggle:{wh.id}"
        )],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"admin_wh_del:{wh.id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_wh_edit")],
    ])

def kb_admin_prod_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить", callback_data="admin_product_add")],
        [InlineKeyboardButton(text="Редактировать", callback_data="admin_product_edit")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
    ])

def kb_prod_pick(products) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{p.name} (арт. {p.article}) {'✅' if p.is_active else '🚫'}", callback_data=f"adm_prod_pick:{p.id}")]
        for p in products
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_prod")])
    return kb

def kb_prod_actions(p: Product) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"adm_prod_rename:{p.id}")],
        [InlineKeyboardButton(text=("🟢 Активировать" if not p.is_active else "🔴 Деактивировать"),
                              callback_data=f"adm_prod_toggle:{p.id}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"adm_prod_del:{p.id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_product_edit")],
    ])

def kb_back(data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=data)]])

def kb_confirm(prefix: str, id_: int, back: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"{prefix}_confirm:{id_}")],
        [InlineKeyboardButton(text="⬅️ Отмена",     callback_data=back)],
    ])


# ---------- Корневое админ-меню ----------
async def on_admin(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен: только для администраторов.", show_alert=True); return
    await cb.answer()
    await send_content(cb, "Администрирование: выберите раздел", reply_markup=kb_admin_root())
    await state.set_state(AdminState.selecting_action)


# ---------- Пользователи ----------
async def admin_users_menu(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await send_content(cb, "Пользователи: выберите действие", reply_markup=kb_admin_users())

async def admin_list_users(cb: types.CallbackQuery, user: User):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        result = await session.execute(select(User).order_by(User.id))
        users = result.scalars().all()
    if not users:
        await send_content(cb, "Пользователи не найдены.", reply_markup=kb_admin_users()); return
    text = "Список пользователей:\n" + "\n".join(
        f"ID: {u.telegram_id}, Имя: {u.name}, Роль: {u.role.value}" for u in users
    )
    await send_content(cb, text, reply_markup=kb_admin_users())

async def admin_delete_user(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        result = await session.execute(select(User).where(User.telegram_id != user.telegram_id))
        users = result.scalars().all()
    if not users:
        await send_content(cb, "Нет пользователей для удаления.", reply_markup=kb_admin_users()); return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{u.name} ({u.role.value})", callback_data=f"delete_user:{u.telegram_id}")]
        for u in users
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")])
    await send_content(cb, "Выберите пользователя для удаления:", reply_markup=kb)
    await state.set_state(AdminState.selecting_user)

async def admin_confirm_delete_user(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    try:
        _, user_id_str = cb.data.split(":"); user_id = int(user_id_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        target_q = await session.execute(select(User).where(User.telegram_id == user_id))
        target_user = target_q.scalar()
        if not target_user:
            await cb.answer("Пользователь не найден.", show_alert=True); return
        await session.delete(target_user); await session.commit()
    await cb.answer("Пользователь удален.")
    await send_content(cb, "Пользователь успешно удален.", reply_markup=kb_admin_users())
    await state.clear()

async def admin_send_message(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        result = await session.execute(select(User).where(User.telegram_id != user.telegram_id))
        users = result.scalars().all()
    if not users:
        await send_content(cb, "Нет пользователей для отправки сообщения.", reply_markup=kb_admin_users()); return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{u.name} ({u.role.value})", callback_data=f"send_msg:{u.telegram_id}")]
        for u in users
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")])
    await send_content(cb, "Выберите получателя:", reply_markup=kb)
    await state.set_state(AdminState.selecting_user)

async def admin_enter_message(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    try:
        _, user_id_str = cb.data.split(":"); user_id = int(user_id_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    await state.update_data(target_user_id=user_id)
    await send_content(cb, "Введите текст сообщения:")
    await state.set_state(AdminState.entering_message)

async def admin_send_message_text(message: types.Message, user: User, state: FSMContext, bot: Bot):
    if user.role != UserRole.admin:
        await message.answer("Доступ запрещен."); return
    data = await state.get_data(); target_user_id = data.get("target_user_id")
    if not target_user_id:
        await message.answer("Ошибка: пользователь не выбран."); return
    try:
        await bot.send_message(target_user_id, f"Сообщение от администратора:\n{message.text}")
        await message.answer("Сообщение успешно отправлено.")
    except Exception as e:
        logging.exception("Ошибка отправки сообщения: %s", e)
        await message.answer("Не удалось отправить сообщение.")
    await state.clear()


# ---------- Склады ----------
async def admin_wh_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await ensure_core_data()
    await send_content(cb, "Склады: выберите действие", reply_markup=kb_admin_wh_root())

async def admin_wh_list(cb: types.CallbackQuery, user: User):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        res = await session.execute(select(Warehouse).order_by(Warehouse.name))
        whs = res.scalars().all()
    if not whs:
        await send_content(cb, "Складов нет.", reply_markup=kb_admin_wh_root()); return
    text = "Склады:\n" + "\n".join(f"- {w.name} (id={w.id}, active={w.is_active})" for w in whs)
    await send_content(cb, text, reply_markup=kb_admin_wh_root())

# Добавление склада
async def admin_wh_add(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await state.clear()
    await state.set_state(WarehouseCreateState.entering_name)
    await send_content(
        cb, "Введите **название склада**:",
        reply_markup=kb_back("admin_wh")
    )

async def admin_wh_add_apply(message: types.Message, user: User, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Название пустое. Введите ещё раз:"); return
    async with get_session() as session:
        try:
            w = Warehouse(name=name, is_active=True)
            session.add(w)
            await session.commit()
        except IntegrityError:
            await session.rollback()
            await message.answer("Склад с таким названием уже существует. Введите другое имя:"); return
    await state.clear()
    await message.answer("✅ Склад создан.", reply_markup=kb_back("admin_wh"))

# Редактирование (выбор склада)
async def admin_wh_edit(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        res = await session.execute(select(Warehouse).order_by(Warehouse.name))
        whs = res.scalars().all()
    if not whs:
        await send_content(cb, "Складов нет.", reply_markup=kb_admin_wh_root()); return
    await send_content(cb, "Выберите склад для редактирования:", reply_markup=kb_wh_edit_pick(whs))

async def admin_wh_pick(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); wh_id = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Warehouse).where(Warehouse.id == wh_id))
        wh = q.scalar()
    if not wh:
        await cb.answer("Склад не найден.", show_alert=True); return
    await state.update_data(wh_id=wh.id)
    await send_content(cb, f"Склад: *{wh.name}* (active={wh.is_active})\nВыберите действие:", reply_markup=kb_wh_actions(wh))

# Переименование склада
async def admin_wh_rename_start(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); wh_id = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    await state.clear()
    await state.set_state(WarehouseRenameState.entering_name)
    await state.update_data(wh_id=wh_id)
    await send_content(cb, "Введите *новое название склада*:", reply_markup=kb_back("admin_wh_edit"))

async def admin_wh_rename_apply(message: types.Message, user: User, state: FSMContext):
    data = await state.get_data()
    wh_id = data.get("wh_id")
    name = (message.text or "").strip()
    if not wh_id:
        await message.answer("Не выбран склад для переименования."); return
    if not name:
        await message.answer("Название пустое. Введите ещё раз:"); return
    async with get_session() as session:
        try:
            await session.execute(update(Warehouse).where(Warehouse.id == wh_id).values(name=name))
            await session.commit()
        except IntegrityError:
            await session.rollback()
            await message.answer("Склад с таким названием уже существует. Введите другое имя:"); return
    await state.clear()
    await message.answer("✅ Название склада обновлено.", reply_markup=kb_back("admin_wh_edit"))

# Тоггл активности
async def admin_wh_toggle(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); wh_id = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Warehouse).where(Warehouse.id == wh_id))
        wh = q.scalar()
        if not wh:
            await cb.answer("Склад не найден.", show_alert=True); return
        wh.is_active = not wh.is_active
        await session.commit()
    await send_content(cb, f"✅ Готово. Активность склада теперь: { 'True' if wh.is_active else 'False' }",
                       reply_markup=kb_admin_wh_root())

# Удаление склада (с проверкой связей)
async def admin_wh_del(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); wh_id = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return

    # Проверим связи
    async with get_session() as session:
        sm_count = (await session.execute(
            select(func.count()).select_from(StockMovement).where(StockMovement.warehouse_id == wh_id)
        )).scalar_one()
        sup_count = (await session.execute(
            select(func.count()).select_from(Supply).where(Supply.warehouse_id == wh_id)
        )).scalar_one()

    if sm_count > 0 or sup_count > 0:
        await send_content(cb, "❗ Нельзя удалить склад: есть связанные движения/поставки.\n"
                               "Вы можете его деактивировать.", reply_markup=kb_wh_actions(Warehouse(id=wh_id, name="...", is_active=True)))
        return

    await send_content(cb, "Удалить этот склад безвозвратно?", reply_markup=kb_confirm("admin_wh_del", wh_id, "admin_wh_edit"))

async def admin_wh_del_confirm(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); wh_id = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Warehouse).where(Warehouse.id == wh_id))
        wh = q.scalar()
        if not wh:
            await cb.answer("Склад уже отсутствует."); return
        await session.delete(wh)
        await session.commit()
    await send_content(cb, "✅ Склад удалён.", reply_markup=kb_admin_wh_root())


# ---------- Товары ----------
async def admin_prod_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await send_content(cb, "Товары: выберите действие", reply_markup=kb_admin_prod_root())

# Добавить товар
async def admin_product_add(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await state.clear()
    await state.set_state(ProductState.entering_article)
    await send_content(cb, "Введите **артикул** (уникальный):", reply_markup=kb_back("admin_prod"))

async def admin_product_enter_article(message: types.Message, user: User, state: FSMContext):
    article = (message.text or "").strip()
    if not article:
        await message.answer("Артикул пустой. Введите ещё раз:"); return
    async with get_session() as session:
        exists_q = await session.execute(select(Product).where(Product.article == article))
        exists = exists_q.scalar()
    if exists:
        await message.answer("Такой артикул уже существует. Введите другой:"); return
    await state.update_data(article=article)
    await state.set_state(ProductState.entering_name)
    await message.answer("Введите **название** товара:")

async def admin_product_enter_name(message: types.Message, user: User, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Название пустое. Введите ещё раз:"); return
    data = await state.get_data(); article = data["article"]
    text = ("Подтвердите создание товара:\n\n"
            f"Артикул: *{article}*\n"
            f"Название: *{name}*\n"
            f"Активный: *Да*\n")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Создать", callback_data="adm_prod_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_prod")],
    ])
    await state.update_data(name=name)
    await state.set_state(ProductState.confirming)
    await message.answer(text, parse_mode="Markdown", reply_markup=kb)

async def admin_product_confirm(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    async with get_session() as session:
        p = Product(article=data["article"], name=data["name"], is_active=True)
        session.add(p); await session.commit()
    await state.clear()
    await send_content(cb, "✅ Товар создан.", reply_markup=kb_admin_prod_root())

# Редактировать товар
async def admin_product_edit(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        total = (await session.execute(select(func.count()).select_from(Product))).scalar_one()
        res = await session.execute(select(Product).order_by(Product.name).limit(30))
        products = res.scalars().all()
    if not products:
        await send_content(cb, "Товаров нет.", reply_markup=kb_admin_prod_root()); return
    await send_content(cb, f"Выберите товар (первые 30, всего {total}):", reply_markup=kb_prod_pick(products))
    await state.set_state(ProductEditState.selecting)

async def admin_product_pick(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); pid = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Product).where(Product.id == pid))
        p = q.scalar()
    if not p:
        await cb.answer("Товар не найден.", show_alert=True); return
    await state.update_data(prod_id=p.id)
    await send_content(cb, f"Товар: *{p.name}* (арт. {p.article}) active={p.is_active}\nВыберите действие:",
                       reply_markup=kb_prod_actions(p))

# Переименование товара
async def admin_product_rename_start(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); pid = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    await state.update_data(prod_id=pid)
    await send_content(cb, "Введите новое *название товара*:", reply_markup=kb_back("admin_product_edit"))
    await state.set_state(ProductEditState.renaming)

async def admin_product_rename_apply(message: types.Message, user: User, state: FSMContext):
    data = await state.get_data()
    name = (message.text or "").strip()
    if not name:
        await message.answer("Название пустое. Введите ещё раз:"); return
    pid = data.get("prod_id")
    async with get_session() as session:
        await session.execute(update(Product).where(Product.id == pid).values(name=name))
        await session.commit()
    await state.clear()
    await message.answer("✅ Название товара обновлено.", reply_markup=kb_back("admin_product_edit"))

# Тоггл активности товара
async def admin_product_toggle(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); pid = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Product).where(Product.id == pid))
        p = q.scalar()
        if not p:
            await cb.answer("Товар не найден.", show_alert=True); return
        p.is_active = not p.is_active
        await session.commit()
    await send_content(cb, f"✅ Готово. Активность товара теперь: { 'True' if p.is_active else 'False' }",
                       reply_markup=kb_admin_prod_root())

# Удаление товара (с проверкой связей)
async def admin_prod_del(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); pid = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return

    # Проверим связи
    async with get_session() as session:
        mv_count = (await session.execute(
            select(func.count()).select_from(StockMovement).where(StockMovement.product_id == pid)
        )).scalar_one()
        si_count = (await session.execute(
            select(func.count()).select_from(SupplyItem).where(SupplyItem.product_id == pid)
        )).scalar_one()

    if mv_count > 0 or si_count > 0:
        await send_content(cb, "❗ Нельзя удалить товар: есть связанные движения/позиции поставок.\n"
                               "Вы можете его деактивировать.", reply_markup=kb_back("admin_product_edit"))
        return

    await send_content(cb, "Удалить этот товар безвозвратно?", reply_markup=kb_confirm("adm_prod_del", pid, "admin_product_edit"))

async def admin_prod_del_confirm(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); pid = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Product).where(Product.id == pid))
        p = q.scalar()
        if not p:
            await cb.answer("Товар уже отсутствует."); return
        await session.delete(p)
        await session.commit()
    await send_content(cb, "✅ Товар удалён.", reply_markup=kb_admin_prod_root())


# ---------- Регистрация ----------
def register_admin_handlers(dp: Dispatcher):
    # Корень
    dp.callback_query.register(on_admin,                   lambda c: c.data == "admin")

    # Пользователи
    dp.callback_query.register(admin_users_menu,           lambda c: c.data == "admin_users")
    dp.callback_query.register(admin_list_users,           lambda c: c.data == "admin_list_users")
    dp.callback_query.register(admin_delete_user,          lambda c: c.data == "admin_delete_user")
    dp.callback_query.register(admin_confirm_delete_user,  lambda c: c.data.startswith("delete_user:"))
    dp.callback_query.register(admin_send_message,         lambda c: c.data == "admin_send_message")
    dp.callback_query.register(admin_enter_message,        lambda c: c.data.startswith("send_msg:"))
    dp.message.register(admin_send_message_text,           AdminState.entering_message)

    # Склады
    dp.callback_query.register(admin_wh_root,              lambda c: c.data == "admin_wh")
    dp.callback_query.register(admin_wh_list,              lambda c: c.data == "admin_wh_list")
    dp.callback_query.register(admin_wh_add,               lambda c: c.data == "admin_wh_add")
    dp.message.register(admin_wh_add_apply,                WarehouseCreateState.entering_name)
    dp.callback_query.register(admin_wh_edit,              lambda c: c.data == "admin_wh_edit")
    dp.callback_query.register(admin_wh_pick,              lambda c: c.data.startswith("admin_wh_pick:"))
    dp.callback_query.register(admin_wh_rename_start,      lambda c: c.data.startswith("admin_wh_rename:"))
    dp.message.register(admin_wh_rename_apply,             WarehouseRenameState.entering_name)
    dp.callback_query.register(admin_wh_toggle,            lambda c: c.data.startswith("admin_wh_toggle:"))
    dp.callback_query.register(admin_wh_del,               lambda c: c.data.startswith("admin_wh_del:"))
    dp.callback_query.register(admin_wh_del_confirm,       lambda c: c.data.startswith("admin_wh_del_confirm:"))

    # Товары
    dp.callback_query.register(admin_prod_root,            lambda c: c.data == "admin_prod")
    dp.callback_query.register(admin_product_add,          lambda c: c.data == "admin_product_add")
    dp.message.register(admin_product_enter_article,       ProductState.entering_article)
    dp.message.register(admin_product_enter_name,          ProductState.entering_name)
    dp.callback_query.register(admin_product_confirm,      ProductState.confirming)

    dp.callback_query.register(admin_product_edit,         lambda c: c.data == "admin_product_edit")
    dp.callback_query.register(admin_product_pick,         lambda c: c.data.startswith("adm_prod_pick:"))
    dp.callback_query.register(admin_product_rename_start, lambda c: c.data.startswith("adm_prod_rename:"))
    dp.message.register(admin_product_rename_apply,        ProductEditState.renaming)
    dp.callback_query.register(admin_product_toggle,       lambda c: c.data.startswith("adm_prod_toggle:"))
    dp.callback_query.register(admin_prod_del,             lambda c: c.data.startswith("adm_prod_del:"))
    dp.callback_query.register(admin_prod_del_confirm,     lambda c: c.data.startswith("adm_prod_del_confirm:"))
