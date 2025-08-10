# handlers/packing.py
from aiogram import Dispatcher, types
from aiogram.fsm.context import FSMContext
from database.models import User
from handlers.common import send_content

async def packing_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer(); await state.clear()
    await send_content(cb, "«Упаковка товаров для МП»: модуль в разработке.")

def register_packing_handlers(dp: Dispatcher):
    dp.callback_query.register(packing_root, lambda c: c.data == "packing")
