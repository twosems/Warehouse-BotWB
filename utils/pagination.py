from typing import List, Optional
from aiogram.types import InlineKeyboardButton


def build_pagination_keyboard(
        page: int,
        page_size: int,
        total: int,
        prev_cb_prefix: str,
        next_cb_prefix: str,
        prev_text: str = "◀ Предыдущая",
        next_text: str = "Следующая ▶",
) -> List[InlineKeyboardButton]:
    """
    Создает универсальные кнопки пагинации (в одну строку).
    Если элементов на одной странице >= общего количества — возвращает [].

    :param page: текущая страница (>=1)
    :param page_size: элементов на странице
    :param total: общее количество элементов
    :param prev_cb_prefix: префикс callback_data для кнопки "Предыдущая"
    :param next_cb_prefix: префикс callback_data для кнопки "Следующая"
    :param prev_text: текст кнопки "Предыдущая"
    :param next_text: текст кнопки "Следующая"
    :return: список InlineKeyboardButton для одной строки
    """
    if page_size <= 0:
        raise ValueError("page_size должен быть > 0")

    total_pages = max(1, -(-total // page_size))  # math.ceil без импорта

    # Пагинация не нужна
    if total <= page_size or total_pages <= 1:
        return []

    row = []
    if page > 1:
        row.append(InlineKeyboardButton(
            prev_text,
            callback_data=f"{prev_cb_prefix}:{page-1}"
        ))
    if page < total_pages:
        row.append(InlineKeyboardButton(
            next_text,
            callback_data=f"{next_cb_prefix}:{page+1}"
        ))

    return row
