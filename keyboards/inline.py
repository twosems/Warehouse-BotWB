# keyboards/inline.py
from typing import List, Optional, Dict
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database.models import Warehouse, Product
from utils.pagination import build_pagination_keyboard  # ожидаем: -> List[InlineKeyboardButton]


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
        priorities_by_id: Optional[Dict[int, int]] = None,
        priorities_by_name: Optional[Dict[str, int]] = None,
        show_menu_back: bool = True,
) -> InlineKeyboardMarkup:
    """
    Список складов. По умолчанию — без спец-сортировки.
    Можно задать:
      - priorities_by_id={warehouse_id: priority}
      - priorities_by_name={"Санкт-Петербург": 0, "Томск": 1}
    callback_data: <prefix>:<id>
    """
    def prio(w: Warehouse) -> int:
        if priorities_by_id and w.id in priorities_by_id:
            return priorities_by_id[w.id]
        if priorities_by_name and w.name in priorities_by_name:
            return priorities_by_name[w.name]
        return 9999

    warehouses_sorted = sorted(warehouses, key=prio)

    rows: List[List[InlineKeyboardButton]] = []
    for w in warehouses_sorted:
        label = w.name
        rows.append([InlineKeyboardButton(text=label, callback_data=f"{prefix}:{w.id}")])

    if show_menu_back:
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
        show_cancel: bool = False,
        cancel_to: str = "cancel",
        trim_len: int = 48,
) -> InlineKeyboardMarkup:
    """
    Список товаров с пагинацией.
    callback_data:
      - <item_prefix>:<product_id>
      - <page_prefix>:<page>
      - back_to (например, rcv_back_wh / stocks_back_wh / reports_back)
    """
    rows: List[List[InlineKeyboardButton]] = []

    def short_text(name: str) -> str:
        return name if len(name) <= trim_len else (name[:trim_len - 1] + "…")

    for p in products:
        title = short_text(p.name or f"ID {p.id}")
        art = f" (арт. {p.article})" if getattr(p, "article", None) else ""
        rows.append([InlineKeyboardButton(text=f"{title}{art}", callback_data=f"{item_prefix}:{p.id}")])

    # Пагинация — ожидаем, что build_pagination_keyboard вернёт одну строку кнопок
    pag_row = build_pagination_keyboard(
        page=page,
        page_size=page_size,
        total=total,
        prev_cb_prefix=page_prefix,
        next_cb_prefix=page_prefix,
        prev_text="◀ Предыдущая",
        next_text="Следующая ▶",
        # Если библиотека поддерживает no-op, можно пробросить:
        # noop_cb="noop"
    )
    if pag_row:
        rows.append(pag_row)

    # Назад / Отмена
    if back_to:
        last_row: List[InlineKeyboardButton] = [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)]
        if show_cancel:
            last_row.append(InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_to))
        rows.append(last_row)
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


def comment_kb(
        back_to: str,
        cancel_to: Optional[str] = None,
        skip_cb: str = "rcv_skip_comment"
) -> InlineKeyboardMarkup:
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
