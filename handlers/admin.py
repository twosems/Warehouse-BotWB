# handlers/admin.py
import logging
from typing import Optional, List, Tuple
import os  # для os.path.basename в отчёте
from aiogram import types, Dispatcher, Bot
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select, func, update, desc
from sqlalchemy.orm import aliased
from sqlalchemy.sql import and_

from database.db import get_session
from database.models import (
    User, UserRole,
    Warehouse, Product,
    StockMovement, Supply, SupplyItem,
    AuditLog,
    MenuItem, RoleMenuVisibility,
)
from database.menu_visibility import (
    ensure_menu_visibility_defaults,
    get_visible_menu_items_for_role,
    toggle_menu_visibility,
)
from handlers.common import send_content


# =========================
#          FSM
# =========================
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


# =========================
#       KEYBOARDS
# =========================
def kb_admin_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏬 Склады", callback_data="admin_wh")],
        [InlineKeyboardButton(text="📦 Товары", callback_data="admin_prod")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton(text="🧾 Журнал действий", callback_data="admin_audit")],
        [InlineKeyboardButton(text="💾 Бэкап БД (сейчас)", callback_data="admin_backup_now")],  # ← НОВОЕ
        [InlineKeyboardButton(text="🧩 Настройки меню", callback_data="adm_menu_roles")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")],
    ])

def kb_admin_users() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Список пользователей", callback_data="admin_list_users")],
        [InlineKeyboardButton(text="Сменить роль пользователя", callback_data="admin_change_role")],
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

def kb_wh_edit_pick(whs: List[Warehouse]) -> InlineKeyboardMarkup:
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
            text=("🟢 Активировать" if not getattr(wh, "is_active", True) else "🔴 Деактивировать"),
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

def kb_prod_pick(products: List[Product]) -> InlineKeyboardMarkup:
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

# --- Пользователи: смена роли ---
def kb_pick_user_for_role(users: List[User]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{u.name or u.telegram_id} — {u.role.value}", callback_data=f"role_user:{u.telegram_id}")]
            for u in users
        ]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")])
    return kb

def kb_pick_role(telegram_id: int, current: UserRole) -> InlineKeyboardMarkup:
    def mark(r: UserRole) -> str:
        return "✅ " if r == current else ""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{mark(UserRole.admin)}Администратор", callback_data=f"role_set:{telegram_id}:admin")],
        [InlineKeyboardButton(text=f"{mark(UserRole.user)}Пользователь",   callback_data=f"role_set:{telegram_id}:user")],
        [InlineKeyboardButton(text=f"{mark(UserRole.manager)}Менеджер",     callback_data=f"role_set:{telegram_id}:manager")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_change_role")],
    ])

# --- Настройки меню: выбор роли и переключатели ---
def kb_menu_roles_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настройки меню: admin",   callback_data="adm_menu_role:admin")],
        [InlineKeyboardButton(text="⚙️ Настройки меню: user",    callback_data="adm_menu_role:user")],
        [InlineKeyboardButton(text="⚙️ Настройки меню: manager", callback_data="adm_menu_role:manager")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
    ])

def kb_menu_visibility(role: UserRole, state_map: dict) -> InlineKeyboardMarkup:
    def mark(flag): return "✅" if flag else "🚫"
    rows = []
    for key, label in [
        ("stocks", "📦 Остатки"),
        ("receiving", "➕ Поступление"),
        ("supplies", "🚚 Поставки"),
        ("packing", "🎁 Упаковка"),
        ("reports", "📈 Отчёты"),
        ("admin", "⚙️ Администрирование"),
    ]:
        rows.append([InlineKeyboardButton(
            text=f"{label} {mark(state_map.get(key, False))}",
            callback_data=f"adm_menu_toggle:{role.value}:{key}"
        )])
    rows.append([InlineKeyboardButton(text="⬅️ К выбору роли", callback_data="adm_menu_roles")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =========================
#       ROOT
# =========================
async def on_admin(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен: только для администраторов.", show_alert=True); return
    await cb.answer()
    await send_content(cb, "Администрирование: выберите раздел", reply_markup=kb_admin_root())
    await state.set_state(AdminState.selecting_action)


# =========================
#       USERS
# =========================
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
    lines = []
    for u in users:
        lines.append(f"ID: {u.id} | TG: {u.telegram_id} | Имя: {u.name or '-'} | Роль: {u.role.value}")
    await send_content(cb, "Список пользователей:\n" + "\n".join(lines), reply_markup=kb_admin_users())

async def admin_delete_user(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        result = await session.execute(select(User).where(User.telegram_id != user.telegram_id).order_by(User.id))
        users = result.scalars().all()
    if not users:
        await send_content(cb, "Нет пользователей для удаления.", reply_markup=kb_admin_users()); return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{u.name or u.telegram_id} ({u.role.value})", callback_data=f"delete_user:{u.telegram_id}")]
        for u in users
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")])
    await send_content(cb, "Выберите пользователя для удаления:", reply_markup=kb)
    await state.set_state(AdminState.selecting_user)

async def admin_confirm_delete_user(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    try:
        _, user_id_str = cb.data.split(":"); tg_id = int(user_id_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        target_q = await session.execute(select(User).where(User.telegram_id == tg_id))
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
        [InlineKeyboardButton(text=f"{u.name or u.telegram_id} ({u.role.value})", callback_data=f"send_msg:{u.telegram_id}")]
        for u in users
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")])
    await send_content(cb, "Выберите получателя:", reply_markup=kb)
    await state.set_state(AdminState.selecting_user)

async def admin_enter_message(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, user_id_str = cb.data.split(":"); tg_id = int(user_id_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    await state.update_data(target_user_tg=tg_id)
    await send_content(cb, "Введите текст сообщения:")
    await state.set_state(AdminState.entering_message)

async def admin_send_message_text(message: types.Message, user: User, state: FSMContext, bot: Bot):
    if user.role != UserRole.admin:
        await message.answer("Доступ запрещен."); return
    data = await state.get_data(); target_tg = data.get("target_user_tg")
    if not target_tg:
        await message.answer("Ошибка: пользователь не выбран."); return
    try:
        await bot.send_message(target_tg, f"Сообщение от администратора:\n{message.text}")
        await message.answer("Сообщение успешно отправлено.")
    except Exception as e:
        logging.exception("Ошибка отправки сообщения: %s", e)
        await message.answer("Не удалось отправить сообщение.")
    await state.clear()

# --- Управление ролями ---
async def admin_change_role(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        res = await session.execute(select(User).order_by(User.id))
        users = res.scalars().all()
    if not users:
        await send_content(cb, "Пользователей нет.", reply_markup=kb_admin_users()); return
    await send_content(cb, "Выберите пользователя для смены роли:", reply_markup=kb_pick_user_for_role(users))

async def admin_pick_user_for_role(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, tg_id_str = cb.data.split(":")
        target_tg_id = int(tg_id_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return

    if target_tg_id == user.telegram_id:
        await cb.answer("Нельзя менять роль самому себе.", show_alert=True); return

    async with get_session() as session:
        q = await session.execute(select(User).where(User.telegram_id == target_tg_id))
        target = q.scalar()
    if not target:
        await cb.answer("Пользователь не найден.", show_alert=True); return

    await send_content(cb, f"Текущая роль: {target.role.value}. Выберите новую:",
                       reply_markup=kb_pick_role(target.telegram_id, target.role))

async def admin_apply_role(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, tg_id_str, role_str = cb.data.split(":")
        target_tg_id = int(tg_id_str)
        new_role = UserRole(role_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return

    if target_tg_id == user.telegram_id:
        await cb.answer("Нельзя менять роль самому себе.", show_alert=True); return

    async with get_session() as session:
        q = await session.execute(select(User).where(User.telegram_id == target_tg_id))
        target = q.scalar()
        if not target:
            await cb.answer("Пользователь не найден.", show_alert=True); return
        old_role = target.role
        if old_role == new_role:
            await send_content(cb, f"Роль не изменилась: по-прежнему {new_role.value}.", reply_markup=kb_admin_users()); return
        target.role = new_role
        await session.commit()

    await send_content(cb, f"Готово. Роль пользователя {target_tg_id} изменена: {old_role.value} → {new_role.value}.",
                       reply_markup=kb_admin_users())


# =========================
#       WAREHOUSES
# =========================
async def admin_wh_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
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

async def admin_wh_add(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await state.clear()
    await state.set_state(WarehouseCreateState.entering_name)
    await send_content(cb, "Введите название склада:", reply_markup=kb_back("admin_wh"))

async def admin_wh_add_apply(message: types.Message, user: User, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Название пустое. Введите ещё раз:"); return
    async with get_session() as session:
        # Проверка уникальности
        exists = (await session.execute(select(Warehouse).where(Warehouse.name == name))).scalar()
        if exists:
            await message.answer("Склад с таким названием уже существует. Введите другое имя:"); return
        w = Warehouse(name=name, is_active=True)
        session.add(w)
        await session.commit()
    await state.clear()
    await message.answer("✅ Склад создан.", reply_markup=kb_back("admin_wh"))

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
    await send_content(cb, f"Склад: {wh.name} (active={wh.is_active}). Выберите действие:", reply_markup=kb_wh_actions(wh))

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
    await send_content(cb, "Введите новое название склада:", reply_markup=kb_back("admin_wh_edit"))

async def admin_wh_rename_apply(message: types.Message, user: User, state: FSMContext):
    data = await state.get_data()
    wh_id = data.get("wh_id")
    name = (message.text or "").strip()
    if not wh_id:
        await message.answer("Не выбран склад для переименования."); return
    if not name:
        await message.answer("Название пустое. Введите ещё раз:"); return
    async with get_session() as session:
        # Проверка уникальности
        exists = (await session.execute(select(Warehouse).where(Warehouse.name == name, Warehouse.id != wh_id))).scalar()
        if exists:
            await message.answer("Склад с таким названием уже существует. Введите другое имя:"); return
        await session.execute(update(Warehouse).where(Warehouse.id == wh_id).values(name=name))
        await session.commit()
    await state.clear()
    await message.answer("✅ Название склада обновлено.", reply_markup=kb_back("admin_wh_edit"))

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
    await send_content(cb, f"✅ Готово. Активность склада теперь: {'True' if wh.is_active else 'False'}",
                       reply_markup=kb_admin_wh_root())

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
        await send_content(cb, "Нельзя удалить склад: есть связанные движения/поставки.\nВы можете его деактивировать.",
                           reply_markup=kb_back("admin_wh_edit"))
        return

    await send_content(cb, "Удалить этот склад безвозвратно?",
                       reply_markup=kb_confirm("admin_wh_del", wh_id, "admin_wh_edit"))

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


# =========================
#        PRODUCTS
# =========================
async def admin_prod_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await send_content(cb, "Товары: выберите действие", reply_markup=kb_admin_prod_root())

async def admin_product_add(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await state.clear()
    await state.set_state(ProductState.entering_article)
    await send_content(cb, "Введите артикул (уникальный):", reply_markup=kb_back("admin_prod"))

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
    await message.answer("Введите название товара:")

async def admin_product_enter_name(message: types.Message, user: User, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Название пустое. Введите ещё раз:"); return
    data = await state.get_data(); article = data["article"]
    text = ("Подтвердите создание товара:\n"
            f"Артикул: {article}\n"
            f"Название: {name}\n"
            f"Активный: Да\n")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Создать", callback_data="adm_prod_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_prod")],
    ])
    await state.update_data(name=name)
    await state.set_state(ProductState.confirming)
    await message.answer(text, reply_markup=kb)

async def admin_product_confirm(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    async with get_session() as session:
        p = Product(article=data["article"], name=data["name"], is_active=True)
        session.add(p); await session.commit()
    await state.clear()
    await send_content(cb, "✅ Товар создан.", reply_markup=kb_admin_prod_root())

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
    await send_content(cb, f"Товар: {p.name} (арт. {p.article}) active={p.is_active}. Выберите действие:",
                       reply_markup=kb_prod_actions(p))

async def admin_product_rename_start(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); pid = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    await state.update_data(prod_id=pid)
    await send_content(cb, "Введите новое название товара:", reply_markup=kb_back("admin_product_edit"))
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
    await send_content(cb, f"✅ Готово. Активность товара теперь: {'True' if p.is_active else 'False'}",
                       reply_markup=kb_admin_prod_root())

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
        await send_content(cb, "Нельзя удалить товар: есть связанные движения/позиции поставок.\nВы можете его деактивировать.",
                           reply_markup=kb_back("admin_product_edit"))
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


# =========================
#        AUDIT LOG
# =========================
AUDIT_PAGE = 10

def _format_audit_row(row: Tuple[AuditLog, Optional[User]]) -> str:
    log, usr = row
    who = f"{usr.name} (id={usr.id})" if usr else "system"
    # Без Markdown, чтобы не ловить ошибки парсинга
    parts = [
        f"[{log.created_at}]",
        f"user: {who}",
        f"action: {log.action.value}",
        f"table: {log.table_name}",
        f"pk: {log.record_pk}",
    ]
    # Коротко покажем diff/old/new, если есть
    if log.diff:
        parts.append(f"diff: {str(log.diff)[:200]}")
    elif log.new_data and not log.old_data:
        parts.append(f"new: {str(log.new_data)[:200]}")
    elif log.old_data and not log.new_data:
        parts.append(f"old: {str(log.old_data)[:200]}")
    return " | ".join(parts)

async def admin_audit_root(cb: types.CallbackQuery, user: User, state: FSMContext, page: int = 1):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()

    # Пагинация: считаем общее количество
    async with get_session() as session:
        total = (await session.execute(select(func.count()).select_from(AuditLog))).scalar_one()

        # Выборка с джойном на пользователя (LEFT OUTER)
        res = await session.execute(
            select(AuditLog, User)
            .join(User, User.id == AuditLog.user_id, isouter=True)
            .order_by(desc(AuditLog.id))
            .offset((page - 1) * AUDIT_PAGE)
            .limit(AUDIT_PAGE)
        )
        rows = res.all()

    if not rows:
        await send_content(cb, "Журнал пуст.", reply_markup=kb_admin_root())
        return

    lines = [ _format_audit_row(r) for r in rows ]
    text = "Журнал действий (последние записи):\n\n" + "\n".join(lines)

    # Пагинация кнопками
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="◀ Предыдущая", callback_data=f"admin_audit_page:{page-1}"))
    if page * AUDIT_PAGE < total:
        buttons.append(InlineKeyboardButton(text="Следующая ▶", callback_data=f"admin_audit_page:{page+1}"))

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    if buttons:
        kb.inline_keyboard.append(buttons)
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")])

    await send_content(cb, text, reply_markup=kb)

async def admin_audit_page(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, p = cb.data.split(":")
        page = int(p)
    except Exception:
        page = 1
    await admin_audit_root(cb, user, state, page=page)


# =========================
#    MENU VISIBILITY
# =========================
async def admin_menu_roles_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()

    # гарантируем, что таблица настроек заполнена дефолтами
    async with get_session() as session:
        await ensure_menu_visibility_defaults(session)

    await send_content(cb, "Выберите роль для настройки видимости меню:", reply_markup=kb_menu_roles_root())

async def admin_menu_role(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, role_str = cb.data.split(":")
        role = UserRole(role_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return

    # соберём текущую карту видимости
    async with get_session() as session:
        res = await session.execute(select(RoleMenuVisibility).where(RoleMenuVisibility.role == role))
        rows = res.scalars().all()
    state_map = {r.item.value: r.visible for r in rows}
    await send_content(cb, f"Настройки меню для роли {role.value}:",
                       reply_markup=kb_menu_visibility(role, state_map))
# ===== БЭКАП БД =====
async def admin_backup_now(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()

    from utils.backup import make_backup_and_maybe_upload

    try:
        info = make_backup_and_maybe_upload()
    except FileNotFoundError as e:
        await send_content(cb, f"❌ Не найден pg_dump или credentials: {e}", reply_markup=kb_admin_root()); return
    except Exception as e:
        await send_content(cb, f"❌ Ошибка бэкапа: {e}", reply_markup=kb_admin_root()); return

    size_mb = round(info["size"] / 1024 / 1024, 2)
    text = (
        "✅ Бэкап создан.\n\n"
        f"Файл: {os.path.basename(info['local_path'])}\n"
        f"Размер: {size_mb} МБ\n"
        f"Локально: {info['local_path']}\n"
    )
    if info["drive_file_id"]:
        text += f"Google Drive: {info['drive_link'] or '(ссылка недоступна)'}\n"
    await send_content(cb, text, reply_markup=kb_admin_root())


async def admin_menu_toggle(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, role_str, key = cb.data.split(":")
        role = UserRole(role_str)
        item = MenuItem(key)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return

    async with get_session() as session:
        new_flag = await toggle_menu_visibility(session, role, item)

        # Обновим экран роли
        res = await session.execute(select(RoleMenuVisibility).where(RoleMenuVisibility.role == role))
        rows = res.scalars().all()

    state_map = {r.item.value: r.visible for r in rows}
    await send_content(cb, f"Настройки меню для роли {role.value} (обновлено {item.value}: {'вкл' if new_flag else 'выкл'}):",
                       reply_markup=kb_menu_visibility(role, state_map))


# =========================
#     REGISTER ROUTES
# =========================
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

    # Управление ролями
    dp.callback_query.register(admin_change_role,          lambda c: c.data == "admin_change_role")
    dp.callback_query.register(admin_pick_user_for_role,   lambda c: c.data.startswith("role_user:"))
    dp.callback_query.register(admin_apply_role,           lambda c: c.data.startswith("role_set:"))

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

    # Журнал
    dp.callback_query.register(admin_audit_root,           lambda c: c.data == "admin_audit")
    dp.callback_query.register(admin_audit_page,           lambda c: c.data.startswith("admin_audit_page:"))

    # Настройки меню
    dp.callback_query.register(admin_menu_roles_root,      lambda c: c.data == "adm_menu_roles")
    dp.callback_query.register(admin_menu_role,            lambda c: c.data.startswith("adm_menu_role:"))
    dp.callback_query.register(admin_menu_toggle,          lambda c: c.data.startswith("adm_menu_toggle:"))
    dp.callback_query.register(admin_backup_now,           lambda c: c.data == "admin_backup_now")
