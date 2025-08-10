# handlers/supplies.py
from aiogram import Dispatcher, types
from aiogram.fsm.context import FSMContext
from database.models import User
from handlers.common import send_content

async def supplies_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer(); await state.clear()
    await send_content(cb, "«Поставки товара на МП»: модуль в разработке.",
                       reply_markup=None)

def register_supplies_handlers(dp: Dispatcher):
    dp.callback_query.register(supplies_root, lambda c: c.data == "supplies")
