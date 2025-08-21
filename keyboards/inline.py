# keyboards/inline.py
from typing import List, Optional
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database.models import Warehouse, Product
from utils.pagination import build_pagination_keyboard


def confirm_kb(prefix: str = "rcv") -> InlineKeyboardMarkup:
    """
    Универсальная клавиатура подтверждения (Подтвердить / Назад).
    Клик: <prefix>_confirm / <prefix>_back
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"{prefix}_confirm")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{prefix}_back")],
    ])


def warehouses_kb(
        warehouses: List[Warehouse],
        prefix: str = "rcv_wh",
) -> InlineKeyboardMarkup:
    """
    Список складов (СПб и Томск — первыми), плюс кнопка «Назад к меню».
    callback_data: <prefix>:<id>
      - для Receiving используйте prefix="rcv_wh"
      - для Stocks/Reports — свой (например, "pr_wh", "rep_wh")
    """
    order = {"Санкт-Петербург": 0, "Томск": 1}
    warehouses_sorted = sorted(warehouses, key=lambda w: order.get(w.name, 99))

    rows: List[List[InlineKeyboardButton]] = []
    for w in warehouses_sorted:
        label = ("🏙️ " if w.name == "Санкт-Петербург" else "🏔️ " if w.name == "Томск" else "") + w.name
        rows.append([InlineKeyboardButton(text=label, callback_data=f"{prefix}:{w.id}")])

    rows.append([InlineKeyboardButton(text="⬅️ Назад к меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def products_page_kb(
        products: List[Product],
        page: int,
        page_size: int,
        total: int,
        back_to: Optional[str] = None,
        item_prefix: str = "rcv_prod",
        page_prefix: str = "rcv_prod_page",
) -> InlineKeyboardMarkup:
    """
    Список товаров с пагинацией.
    callback_data:
      - <item_prefix>:<product_id>   (по умолчанию rcv_prod:<id>)
      - <page_prefix>:<page>         (по умолчанию rcv_prod_page:<n>)
      - back_to (например, rcv_back_wh / stocks_back_wh / reports_back)
    Для отчётов укажите, например: item_prefix="report_art", page_prefix="report_art_page".
    """
    rows: List[List[InlineKeyboardButton]] = []

    # Кнопки товаров
    for p in products:
        rows.append([InlineKeyboardButton(
            text=f"{p.name} (арт. {p.article})",
            callback_data=f"{item_prefix}:{p.id}"
        )])

    # Пагинация
    pag_row = build_pagination_keyboard(
        page=page,
        page_size=page_size,
        total=total,
        prev_cb_prefix=page_prefix,
        next_cb_prefix=page_prefix,
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


def qty_kb(back_to: str, cancel_to: Optional[str] = None) -> InlineKeyboardMarkup:
    """
    Клавиатура для шага ввода количества.
    back_to: callback_data для шага «назад»
    cancel_to: (опционально) callback_data для «Отмена»
    """
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)],
    ]
    if cancel_to:
        rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_to)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def comment_kb(back_to: str, cancel_to: Optional[str] = None, skip_cb: str = "rcv_skip_comment") -> InlineKeyboardMarkup:
    """
    Клавиатура для комментария (Пропустить / Назад / (опц.) Отмена).
    skip_cb: callback_data для «Пропустить комментарий»
    """
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="⏭ Пропустить комментарий", callback_data=skip_cb)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)],
    ]
    if cancel_to:
        rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_to)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def receiving_confirm_kb(
        confirm_prefix: str,
        back_to: str,
        cancel_to: Optional[str] = None,
        confirm_text: str = "✅ Добавить",
) -> InlineKeyboardMarkup:
    """
    Клавиатура подтверждения поступления/действия.
    confirm_prefix="rcv" → "rcv_confirm"
    back_to: callback «назад»
    cancel_to: (опц.) callback «отмена»
    """
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text=confirm_text, callback_data=f"{confirm_prefix}_confirm")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)],
    ]
    if cancel_to:
        rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_to)])
    return InlineKeyboardMarkup(inline_keyboard=rows)
