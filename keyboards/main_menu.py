from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def get_main_menu(role: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="Остатки товара на складах", callback_data="stocks")],
        [InlineKeyboardButton(text="Поступление товара на склад", callback_data="receiving")],
        [InlineKeyboardButton(text="Поставки товара на МП", callback_data="supplies")],
        [InlineKeyboardButton(text="Упаковка товаров для МП", callback_data="packing")],
        [InlineKeyboardButton(text="Отчеты", callback_data="reports")]
    ]
    if role == "admin":
        rows.append([InlineKeyboardButton(text="Администрирование", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
