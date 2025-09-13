# keyboards/main_menu.py
from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database.db import get_session
from database.menu_visibility import get_visible_menu_items_for_role
from database.models import UserRole, MenuItem

# Человеко-читаемые тексты
TEXTS = {
    MenuItem.stocks:        "📦 Остатки",
    MenuItem.receiving:     "➕ Поступление",
    MenuItem.supplies:      "🚚 Поставки",
    MenuItem.packing:       "🎁 Упаковка",
    MenuItem.picking:       "🧰 Сборка",
    MenuItem.reports:       "📈 Отчёты",
    MenuItem.purchase_cn:   "🇨🇳 Закупка CN",
    MenuItem.msk_warehouse: "🏢 Склад MSK",
    MenuItem.admin:         "⚙️ Администрирование",
}

# callback_data для существующих пунктов (как было)
CB = {
    MenuItem.stocks:        "stocks",
    MenuItem.receiving:     "receiving",
    MenuItem.supplies:      "supplies",
    MenuItem.packing:       "packing",
    MenuItem.picking:       "picking",
    MenuItem.reports:       "reports",
    MenuItem.purchase_cn:   "cn:root",
    MenuItem.msk_warehouse: "msk:root",
    MenuItem.admin:         "admin",
}

# Группы подкаталогов
# 1) «Закупки-поступления»
PROCURE_GROUP = [
    MenuItem.purchase_cn,
    MenuItem.msk_warehouse,
    MenuItem.receiving,
]

# 2) «Упаковка-поставки»
PACK_GROUP = [
    MenuItem.packing,
    MenuItem.supplies,
    MenuItem.picking,
    MenuItem.stocks,
]

# Тексты верхних категорий
ROOT_PROCURE_TEXT = "🧾 Закупки-поступления"
ROOT_PACK_TEXT    = "📦 Упаковка-поставки"


# -------------------- helpers --------------------
async def _get_visible_set(role: UserRole) -> set[MenuItem]:
    async with get_session() as session:
        visible = await get_visible_menu_items_for_role(session, role)
    return set(visible)

def _any_visible(visible: set[MenuItem], items: list[MenuItem]) -> bool:
    return any(i in visible for i in items)

def _rows_from_items(
        visible: set[MenuItem],
        items: list[MenuItem],
        per_row: int = 2,
) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    buf: list[InlineKeyboardButton] = []
    for it in items:
        if it not in visible:
            continue
        buf.append(InlineKeyboardButton(text=TEXTS[it], callback_data=CB[it]))
        if len(buf) == per_row:
            rows.append(buf)
            buf = []
    if buf:
        rows.append(buf)
    return rows


# -------------------- публичные билдеры --------------------
async def get_main_menu(role: UserRole) -> InlineKeyboardMarkup:
    """
    Корневое меню:
      • 2 категории (показываем только если внутри есть видимые пункты);
      • Отчёты — отдельной кнопкой;
      • Администрирование — отдельной кнопкой.
    """
    visible = await _get_visible_set(role)

    rows: list[list[InlineKeyboardButton]] = []

    if _any_visible(visible, PROCURE_GROUP):
        rows.append([InlineKeyboardButton(text=ROOT_PROCURE_TEXT, callback_data="root:procure")])

    if _any_visible(visible, PACK_GROUP):
        rows.append([InlineKeyboardButton(text=ROOT_PACK_TEXT, callback_data="root:pack")])

    if MenuItem.reports in visible:
        rows.append([InlineKeyboardButton(text=TEXTS[MenuItem.reports], callback_data=CB[MenuItem.reports])])

    if MenuItem.admin in visible:
        rows.append([InlineKeyboardButton(text=TEXTS[MenuItem.admin], callback_data=CB[MenuItem.admin])])

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def get_procure_submenu(role: UserRole) -> InlineKeyboardMarkup:
    """Подменю «Закупки-поступления»: Закупка CN, Склад MSK, Поступление."""
    visible = await _get_visible_set(role)
    rows = _rows_from_items(visible, PROCURE_GROUP)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="root:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def get_pack_submenu(role: UserRole) -> InlineKeyboardMarkup:
    """Подменю «Упаковка-поставки»: Упаковка, Поставки, Сборка, Остатки."""
    visible = await _get_visible_set(role)
    rows = _rows_from_items(visible, PACK_GROUP)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="root:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
