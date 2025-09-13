# handlers/common_compat.py (новый файл)
from aiogram import Router
from aiogram.types import CallbackQuery
from database.models import User
from aiogram import F

router = Router()

# Русские старые коллбэки → новые
COMPAT = {
    "ostatki": "stocks",
    "prihod": "receiving",
    "postavki": "supplies",
    "otchety": "reports",
    "korr_ost": None,   # если больше нет — можно показать сообщение
    "back_to_menu": "root:main",
}

@router.callback_query(F.data.in_(list(COMPAT.keys())))
async def compat_router(cb: CallbackQuery, user: User):
    target = COMPAT.get(cb.data)
    if not target:
        await cb.answer("Раздел временно недоступен.", show_alert=True)
        return
    # Просто переотправим как будто нажали новую кнопку
    await cb.answer()
    await cb.message.bot.dispatch("callback_query", data=type("Q", (), {"data": target, "from_user": cb.from_user, "message": cb.message})())
