from typing import List
from aiogram.types import InlineKeyboardButton


def build_pagination_keyboard(
        page: int,
        page_size: int,
        total: int,
        prev_cb_prefix: str,
        next_cb_prefix: str,
        prev_text: str = "◀ Предыдущая",
        next_text: str = "Следующая ▶",
        noop_cb: str = "noop",
) -> List[InlineKeyboardButton]:
    """
    Возвращает ОДНУ строку кнопок пагинации (List[InlineKeyboardButton]).
    Если страниц нет — возвращает пустой список [].

    Правила:
      - aiogram v3 требует именованные аргументы у InlineKeyboardButton.
      - Центральная кнопка (N/M) и "заблокированные" стрелки используют callback 'noop'.
      - Ожидается, что вызывающая сторона добавляет полученную строку в inline_keyboard:
          row = build_pagination_keyboard(...);  if row: rows.append(row)
    """
    if page_size <= 0:
        raise ValueError("page_size должен быть > 0")

    # ceil(total / page_size) без math
    total_pages = max(1, -(-total // page_size))

    # Пагинация не нужна
    if total <= page_size or total_pages <= 1:
        return []

    # В какую сторону можем листать
    has_prev = page > 1
    has_next = page < total_pages

    prev_cb = f"{prev_cb_prefix}:{page-1}" if has_prev else noop_cb
    next_cb = f"{next_cb_prefix}:{page+1}" if has_next else noop_cb

    row: List[InlineKeyboardButton] = [
        InlineKeyboardButton(
            text=(prev_text if has_prev else "⛔"),
            callback_data=prev_cb,
        ),
        InlineKeyboardButton(
            text=f"{page}/{total_pages}",
            callback_data=noop_cb,
        ),
        InlineKeyboardButton(
            text=(next_text if has_next else "⛔"),
            callback_data=next_cb,
        ),
    ]
    return row
