from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from database.db import get_session
from database.models import UserRole, MenuItem
from database.menu_visibility import (
    LABELS,
    MENU_ORDER,
    ensure_menu_visibility_defaults,
    get_visibility_map_for_role,
    toggle_menu_visibility,
)

router = Router()


def _kb_visibility(role: UserRole, vis_map: dict[MenuItem, bool]) -> InlineKeyboardMarkup:
    # Debug (можно убрать после проверки)
    print("DEBUG MENU_ORDER:", [mi.name for mi in MENU_ORDER])

    rows: list[list[InlineKeyboardButton]] = []
    for mi in MENU_ORDER:
        visible = vis_map.get(mi, False)
        mark = "✅" if visible else "❌"
        rows.append([
            InlineKeyboardButton(
                text=f"{LABELS[mi]} {mark}",
                callback_data=f"menuvis:{role.name}:{mi.name}:{int(not visible)}",
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ К ролям", callback_data="menuvis:roles")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "menuvis:roles")
async def menuvis_roles(cb: CallbackQuery):
    """Экран выбора роли для настройки видимости."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настройки меню: admin",   callback_data="menuvis:open:admin")],
        [InlineKeyboardButton(text="⚙️ Настройки меню: user",    callback_data="menuvis:open:user")],
        [InlineKeyboardButton(text="⚙️ Настройки меню: manager", callback_data="menuvis:open:manager")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
    ])
    await cb.message.edit_text("Выберите роль для настройки видимости меню:", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("menuvis:open:"))
async def open_menu_visibility(cb: CallbackQuery):
    """Открыть экран видимости роли. Формат: menuvis:open:<ROLE>"""
    role = UserRole[cb.data.split(":")[2]]

    async with get_session() as session:
        # Гарантируем, что в БД есть все пары (role,item)
        await ensure_menu_visibility_defaults(session)
        vis_map = await get_visibility_map_for_role(session, role)

    await cb.message.edit_text(
        f"Настройки меню для роли {role.name.lower()}:",
        reply_markup=_kb_visibility(role, vis_map),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("menuvis:"))
async def toggle_visibility(cb: CallbackQuery):
    """
    Переключить флаг. Формат: menuvis:<ROLE>:<ITEM>:<NEWVAL>
    Пример: menuvis:admin:stocks:0
    """
    parts = cb.data.split(":")
    if parts[1] == "roles":
        # Раньше этот кейс глотался — теперь есть отдельный хендлер menuvis_roles
        await cb.answer()
        return

    role = UserRole[parts[1]]
    item = MenuItem[parts[2]]
    new_val = bool(int(parts[3]))

    async with get_session() as session:
        # переключаем/ставим явное значение; запись создастся, если её не было
        await toggle_menu_visibility(session, role, item, value=new_val)
        vis_map = await get_visibility_map_for_role(session, role)

    await cb.message.edit_text(
        f"Настройки меню для роли {role.name.lower()}:",
        reply_markup=_kb_visibility(role, vis_map),
    )
    await cb.answer()
