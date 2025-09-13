# handlers/menu_info.py
from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery

from database.models import MenuItem
from database import menu_visibility as mv  # ⬅️ меняем импорт

router = Router()

@router.callback_query(F.data.startswith("info:"))
async def show_item_info(cb: CallbackQuery):
    try:
        _, raw = cb.data.split(":", 1)
        item = MenuItem[raw]
    except Exception:
        await cb.answer("Неизвестный пункт меню.", show_alert=True)
        return

    title = mv.LABELS.get(item, item.name)                           # ⬅️ через mv
    desc = getattr(mv, "DESCRIPTIONS", {}).get(item, "Описание отсутствует.")  # ⬅️ через mv с fallback
    await cb.answer(f"{title}\n\n{desc}", show_alert=True)
