# handlers/common.py
import contextlib
import logging
from typing import Dict, Optional

from aiogram import Dispatcher, types, BaseMiddleware, Bot
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from sqlalchemy import select

from config import ADMIN_TELEGRAM_ID
from keyboards.main_menu import get_main_menu
from database.db import get_session, set_audit_user
from database.models import User, UserRole

# Память процесса
pending_requests: Dict[int, str] = {}
last_content_msg: Dict[int, int] = {}


class RoleCheckMiddleware(BaseMiddleware):
    """
    Пускаем всех на /start. Для остальных сообщений/колбэков — только авторизованных.
    В data прокидываем current User (database.models.User).
    Также отмечаем текущего пользователя для аудита (set_audit_user).
    """
    async def __call__(self, handler, event, data: dict):
        # /start пропускаем без проверки авторизации
        if isinstance(event, types.Message) and event.text and event.text.startswith("/start"):
            # На всякий случай сбросим текущего аудит-пользователя — в /start могут не быть в системе
            set_audit_user(None)
            return await handler(event, data)

        # Все остальные апдейты: Message / CallbackQuery
        if isinstance(event, (types.Message, types.CallbackQuery)):
            user_id = event.from_user.id
            async with get_session() as session:
                res = await session.execute(select(User).where(User.telegram_id == user_id))
                user: Optional[User] = res.scalar()

            if not user:
                # никто не авторизован -> сбрасываем user_id для аудита
                set_audit_user(None)
                text = "Пожалуйста, авторизуйтесь через /start."
                if isinstance(event, types.Message):
                    await event.answer(text)
                else:
                    await event.message.answer(text)
                return

            # Авторизованы: сохраняем в контекст обработчика и выставляем user_id для аудита
            data["user"] = user
            set_audit_user(user.id)
            return await handler(event, data)

        # На прочие типы событий просто сбросим user_id
        set_audit_user(None)
        return await handler(event, data)


async def send_content(cb: types.CallbackQuery, text: str, reply_markup=None, parse_mode: Optional[str] = None):
    """
    Удаляем прошлый контент и отправляем новый текст отдельным сообщением — ниже клавиатуры.
    parse_mode опционален (Markdown/HTML).
    """
    uid = cb.from_user.id
    mid = last_content_msg.get(uid)
    if mid:
        with contextlib.suppress(Exception):
            await cb.bot.delete_message(chat_id=cb.message.chat.id, message_id=mid)

    if parse_mode:
        m = await cb.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
    else:
        m = await cb.message.answer(text, reply_markup=reply_markup)

    last_content_msg[uid] = m.message_id


# ===== /start: заявка админу или меню =====
async def cmd_start(message: types.Message, bot: Bot):
    user_id = message.from_user.id
    async with get_session() as session:
        res = await session.execute(select(User).where(User.telegram_id == user_id))
        user = res.scalar()

    if user:
        # Пользователь авторизован — можно выставить аудит-пользователя здесь тоже (на случай действий в /start)
        set_audit_user(user.id)
        # меню теперь асинхронное и принимает enum UserRole
        await message.answer("Главное меню:", reply_markup=await get_main_menu(user.role))
        return

    # Заявка админу (неавторизован)
    set_audit_user(None)
    pending_requests[user_id] = message.from_user.full_name or str(user_id)
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="✅ Принять",  callback_data=f"approve:{user_id}"),
        types.InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject:{user_id}"),
    ]])
    try:
        await bot.send_message(
            ADMIN_TELEGRAM_ID,
            f"Пользователь {message.from_user.full_name} (@{message.from_user.username or 'без username'}) запросил доступ.",
            reply_markup=kb,
        )
    except Exception as e:
        logging.exception("Не удалось отправить запрос админу: %s", e)

    await message.answer("Ваш запрос отправлен администратору. Ожидайте одобрения.")


# Решение админа по заявке (approve/reject)
async def handle_admin_decision(cb: types.CallbackQuery, bot: Bot):
    try:
        action, uid_str = cb.data.split(":")
        uid = int(uid_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True)
        return

    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        await cb.answer("У вас нет прав для этого действия.", show_alert=True)
        return
    if uid not in pending_requests:
        await cb.answer("Запрос уже обработан или не найден.", show_alert=True)
        return

    if action == "approve":
        async with get_session() as session:
            new_user = User(
                telegram_id=uid,
                name=pending_requests[uid],
                role=UserRole.user,
                password_hash="approved",
            )
            session.add(new_user)
            await session.commit()
        with contextlib.suppress(Exception):
            await bot.send_message(uid, "Вас добавили в систему! Введите /start для входа.")
        await cb.answer("Пользователь добавлен.")
    else:
        with contextlib.suppress(Exception):
            await bot.send_message(uid, "Ваш запрос на доступ отклонён.")
        await cb.answer("Пользователь отклонён.")

    pending_requests.pop(uid, None)


# Базовые разделы-заглушки (если где-то ещё используются)
async def on_ostatki(cb: types.CallbackQuery, user: User):
    await cb.answer()
    await send_content(cb, "«Остатки»: модуль в разработке.")

async def on_prihod(cb: types.CallbackQuery, user: User):
    await cb.answer()
    await send_content(cb, "«Приход товара»: модуль в разработке.")

async def on_korr_ost(cb: types.CallbackQuery, user: User):
    await cb.answer()
    await send_content(cb, "«Корректировка остатков»: модуль в разработке.")

async def on_postavki(cb: types.CallbackQuery, user: User):
    await cb.answer()
    await send_content(cb, "«Поставки»: модуль в разработке.")

async def on_otchety(cb: types.CallbackQuery, user: User):
    await cb.answer()
    await send_content(cb, "«Отчеты»: модуль в разработке.")

async def back_to_main_menu(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if state:
        await state.clear()
    # здесь тоже асинхронный вызов
    await cb.message.answer("Главное меню:", reply_markup=await get_main_menu(user.role))


def register_common_handlers(dp: Dispatcher):
    dp.message.register(cmd_start, CommandStart())
    dp.callback_query.register(handle_admin_decision, lambda c: c.data.startswith(("approve:", "reject:")))
    dp.callback_query.register(on_ostatki, lambda c: c.data == "ostatki")
    dp.callback_query.register(on_prihod,  lambda c: c.data == "prihod")
    dp.callback_query.register(on_korr_ost, lambda c: c.data == "korr_ost")
    dp.callback_query.register(on_postavki, lambda c: c.data == "postavki")
    dp.callback_query.register(on_otchety,  lambda c: c.data == "otchety")
    dp.callback_query.register(back_to_main_menu, lambda c: c.data == "back_to_menu")
