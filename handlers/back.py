# handlers/back.py
from aiogram import Router, F
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, ReplyKeyboardRemove

router = Router()

@router.callback_query(StateFilter("*"), F.data == "back_to_menu")
async def back_to_menu_cb(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await cb.message.edit_reply_markup()
    except Exception:
        pass
    # здесь покажи своё главное меню
    await cb.message.answer("Главное меню.", reply_markup=ReplyKeyboardRemove())
    await cb.answer()

@router.message(StateFilter("*"), F.text.casefold().in_({"назад", "⬅️ назад"}))
async def back_to_menu_msg(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Главное меню.", reply_markup=ReplyKeyboardRemove())
