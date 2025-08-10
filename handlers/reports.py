# handlers/reports.py
from aiogram import Dispatcher, types
from aiogram.fsm.context import FSMContext
from database.models import User
from handlers.common import send_content

async def reports_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer(); await state.clear()
    await send_content(cb, "«Отчеты»: модуль в разработке.")

def register_reports_handlers(dp: Dispatcher):
    dp.callback_query.register(reports_root, lambda c: c.data == "reports")
