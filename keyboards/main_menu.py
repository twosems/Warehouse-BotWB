# keyboards/main_menu.py
from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database.db import get_session
from database.menu_visibility import get_visible_menu_items_for_role
from database.models import UserRole, MenuItem

# порядок и тексты кнопок (callback_data в третьей колонке)
ITEMS = [
    (MenuItem.stocks,        "📦 Остатки",      "stocks"),
    (MenuItem.receiving,     "➕ Поступление",  "receiving"),
    (MenuItem.supplies,      "🚚 Поставки",     "supplies"),
    (MenuItem.packing,       "🎁 Упаковка",     "packing"),
    (MenuItem.picking,       "🧰 Сборка",       "picking"),
    (MenuItem.reports,       "📈 Отчёты",       "reports"),
    # ▼ НОВОЕ ▼
    (MenuItem.purchase_cn,   "🇨🇳 Закупка CN",  "cn:root"),
    (MenuItem.msk_warehouse, "🏢 Склад MSK",    "msk:root"),
    # admin вынесем отдельной строкой ниже
]

async def get_main_menu(role: UserRole) -> InlineKeyboardMarkup:
    """
    Собирает главное меню исходя из таблицы role_menu_visibility.
    Админ увидит всё (по дефолту), для user/manager — согласно настройкам.
    """
    async with get_session() as session:
        visible = await get_visible_menu_items_for_role(session, role)

    def is_visible(item: MenuItem) -> bool:
        return item in visible

    rows: list[list[InlineKeyboardButton]] = []

    # сетка: по 2 кнопки в ряд
    buf: list[InlineKeyboardButton] = []
    for item, text, cb in ITEMS:
        if not is_visible(item):
            continue
        buf.append(InlineKeyboardButton(text=text, callback_data=cb))
        if len(buf) == 2:
            rows.append(buf)
            buf = []
    if buf:
        rows.append(buf)

    # администрирование — отдельной строкой внизу (если разрешено)
    if is_visible(MenuItem.admin):
        rows.append([InlineKeyboardButton(text="⚙️ Администрирование", callback_data="admin")])

    return InlineKeyboardMarkup(inline_keyboard=rows)
