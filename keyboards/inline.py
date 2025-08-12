# keyboards/inline.py
from typing import List, Optional
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database.models import Warehouse, Product
from utils.pagination import build_pagination_keyboard


def confirm_kb(prefix: str = "rcv") -> InlineKeyboardMarkup:
    """Универсальная клавиатура подтверждения (Подтвердить / Назад)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"{prefix}_confirm")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{prefix}_back")],
    ])


def warehouses_kb(warehouses: List[Warehouse], prefix: str = "rcv_wh") -> InlineKeyboardMarkup:
    """
    Список складов (СПб и Томск — первыми), плюс кнопка назад.
    callback_data: rcv_wh:{id}  # Уникальный префикс для receiving
    """
    order = {"Санкт-Петербург": 0, "Томск": 1}
    warehouses_sorted = sorted(warehouses, key=lambda w: order.get(w.name, 99))

    rows: List[List[InlineKeyboardButton]] = []
    for w in warehouses_sorted:
        label = ("🏙️ " if w.name == "Санкт-Петербург" else "🏔️ " if w.name == "Томск" else "") + w.name
  #     rows.append([InlineKeyboardButton(text=label, callback_data=f"rcv_wh:{w.id}")])
        rows.append([InlineKeyboardButton(text=label, callback_data=f"{prefix}:{w.id}")])

    rows.append([InlineKeyboardButton(text="⬅️ Назад к меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def products_page_kb(
        products: List[Product],
        page: int,
        page_size: int,
        total: int,
        back_to: Optional[str] = None
) -> InlineKeyboardMarkup:
    """
    Список товаров с пагинацией.
    callback_data:
      - rcv_prod:{product_id}  # Уникальный префикс для receiving
      - rcv_prod_page:{page}
      - back_to (например, rcv_back_wh)
    """
    rows: List[List[InlineKeyboardButton]] = []

    # Кнопки товаров
    for p in products:
        rows.append([InlineKeyboardButton(
            text=f"{p.name} (арт. {p.article})",
            callback_data=f"rcv_prod:{p.id}"
        )])

    # Пагинация
    pag_row = build_pagination_keyboard(
        page=page,
        page_size=page_size,
        total=total,
        prev_cb_prefix="rcv_prod_page",
        next_cb_prefix="rcv_prod_page",
        prev_text="◀ Предыдущая",
        next_text="Следующая ▶"
    )
    if pag_row:
        rows.append(pag_row)

    # Назад (если задано, иначе назад к меню)
    if back_to:
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)])
    else:
        rows.append([InlineKeyboardButton(text="⬅️ Назад к меню", callback_data="back_to_menu")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def qty_kb(back_to: str) -> InlineKeyboardMarkup:
    """
    Клавиатура для шага ввода количества.
    back_to: callback_data для шага "назад" (например, rcv_back_products)
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)],
    ])


def comment_kb(back_to: str) -> InlineKeyboardMarkup:
    """
    Клавиатура для комментария (Пропустить / Назад).
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ Пропустить комментарий", callback_data="rcv_skip_comment")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)],
    ])


def receiving_confirm_kb(confirm_prefix: str, back_to: str) -> InlineKeyboardMarkup:
    """
    Клавиатура подтверждения поступления.
    confirm_prefix="rcv" → "rcv_confirm"
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Добавить", callback_data=f"{confirm_prefix}_confirm")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)],
    ])