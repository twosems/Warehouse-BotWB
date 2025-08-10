# keyboards/inline.py
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List
from database.models import Warehouse, Product


def warehouses_kb(warehouses: List[Warehouse]) -> InlineKeyboardMarkup:
    # Сортируем: СПб, затем Томск, затем остальные
    order = {"Санкт-Петербург": 0, "Томск": 1}
    warehouses = sorted(warehouses, key=lambda w: order.get(w.name, 99))

    rows = []
    for w in warehouses:
        label = ("🏙️ " if w.name == "Санкт-Петербург" else "🏔️ " if w.name == "Томск" else "") + w.name
        rows.append([InlineKeyboardButton(text=label, callback_data=f"pr_wh:{w.id}")])

    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="pr_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_kb(prefix: str = "pr") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"{prefix}_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"{prefix}_cancel")],
    ])


def incoming_mode_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Выбрать товар из базы", callback_data="pr_mode_choose")],
        [InlineKeyboardButton(text="⌨️ Ввести артикул вручную", callback_data="pr_mode_enter")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="pr_cancel")],
    ])


def products_page_kb(products: List[Product], page: int, page_size: int, total: int) -> InlineKeyboardMarkup:
    rows = []
    for p in products:
        rows.append([InlineKeyboardButton(text=f"{p.name} (арт. {p.article})", callback_data=f"pr_prod:{p.id}")])

    # Пагинация
    total_pages = max(1, (total + page_size - 1) // page_size)
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="« Назад", callback_data=f"pr_prod_page:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"pr_prod_page:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="⬅️ Режимы", callback_data="pr_mode")])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="pr_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
