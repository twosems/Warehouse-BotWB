# handlers/common.py
import contextlib
import logging
from types import SimpleNamespace
from typing import Dict, Optional

from aiogram import Dispatcher, types, BaseMiddleware, Bot, Router, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select

from config import ADMIN_TELEGRAM_ID
from keyboards.main_menu import get_main_menu
from database.db import get_session, set_audit_user, init_db
from database.models import User, UserRole

# Память процесса (локально в процессе бота)
pending_requests: Dict[int, str] = {}
last_content_msg: Dict[int, int] = {}


# ---------------------------
# UI helpers
# ---------------------------
def _kb_emergency_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Бэкапы / Восстановление", callback_data="admin:backup")],
        [InlineKeyboardButton(text="Закрыть", callback_data="noop")],
    ])


async def send_content(
        cb: types.CallbackQuery,
        text: str,
        reply_markup: Optional[InlineKeyboardMarkup] = None,
        parse_mode: Optional[str] = None,
):
    """
    Удаляем прошлый контент и отправляем новый текст отдельным сообщением — ниже клавиатуры.
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


def _is_emergency_allowed(event: types.TelegramObject) -> bool:
    """
    В аварийном режиме (нет БД/нет записи admin) разрешаем только:
      • экран бэкапов (admin:backup)
      • все шаги восстановления/бэкапа (bk:*)
      • любые сообщения (нужны для отправки файла и подтверждения фразы)
    """
    if isinstance(event, types.CallbackQuery):
        data = event.data or ""
        return data == "admin:backup" or data.startswith("bk:")
    if isinstance(event, types.Message):
        return True
    return False


# ---------------------------
# Middleware с аварийным режимом
# ---------------------------
class RoleCheckMiddleware(BaseMiddleware):
    """
    /start — пропускаем всем.

    Остальные события:
      • если пользователь найден в БД — обычный режим;
      • если пользователь не найден и это ADMIN_TELEGRAM_ID —
          включаем аварийный режим: пропускаем ТОЛЬКО бэкапы/restore;
      • не админ — просим /start, либо сообщаем, что БД недоступна.
    """
    async def __call__(self, handler, event, data: dict):
        # /start — всегда можно
        if isinstance(event, types.Message) and event.text and event.text.startswith("/start"):
            set_audit_user(None)
            return await handler(event, data)

        # Обрабатываем только Message/CallbackQuery
        if not isinstance(event, (types.Message, types.CallbackQuery)):
            set_audit_user(None)
            return await handler(event, data)

        user_id = event.from_user.id

        # Пробуем найти пользователя в БД (если БД доступна)
        user: Optional[User] = None
        db_ok = True
        try:
            async with get_session() as session:
                res = await session.execute(select(User).where(User.telegram_id == user_id))
                user = res.scalar()
        except Exception:
            db_ok = False
            user = None

        # Нашёлся пользователь — обычный режим
        if user is not None:
            data["user"] = user
            set_audit_user(user.id)
            return await handler(event, data)

        # Нет пользователя: если это админ — аварийный режим (только бэкапы)
        if user_id == ADMIN_TELEGRAM_ID:
            fallback_admin = SimpleNamespace(
                id=None, telegram_id=user_id, name="Emergency Admin", role=UserRole.admin
            )
            data["user"] = fallback_admin
            data["emergency"] = True
            set_audit_user(None)

            if _is_emergency_allowed(event):
                return await handler(event, data)
            else:
                msg = "Аварийный режим: доступны только действия «Бэкапы/Восстановление». Откройте экран бэкапов."
                if isinstance(event, types.Message):
                    await event.answer(msg, reply_markup=_kb_emergency_root())
                else:
                    await event.message.answer(msg, reply_markup=_kb_emergency_root())
                return

        # Не админ: либо БД недоступна, либо нет записи — просим /start
        set_audit_user(None)
        text = "База данных недоступна. Повторите позже." if not db_ok else "Пожалуйста, авторизуйтесь через /start."
        if isinstance(event, types.Message):
            await event.answer(text)
        else:
            await event.message.answer(text)
        return


# ---------------------------
# /start: с безопасным бутстрапом админа
# ---------------------------
async def cmd_start(message: types.Message, bot: Bot):
    user_id = message.from_user.id

    # 1) Админ: пытаемся поднять схему и самозавести запись админа.
    if user_id == ADMIN_TELEGRAM_ID:
        try:
            # если таблиц нет — создаст
            await init_db()
        except Exception:
            pass

        try:
            async with get_session() as session:
                res = await session.execute(select(User).where(User.telegram_id == user_id))
                admin_user = res.scalar()
                if not admin_user:
                    admin_user = User(
                        telegram_id=user_id,
                        name=message.from_user.full_name or "Admin",
                        role=UserRole.admin,
                        password_hash="bootstrap",
                    )
                    session.add(admin_user)
                    await session.commit()

            set_audit_user(admin_user.id)
            await message.answer("Главное меню:", reply_markup=await get_main_menu(UserRole.admin))
            return

        except Exception:
            # Схема/БД не доступна — показываем аварийное меню
            set_audit_user(None)
            await message.answer(
                "Аварийный режим: база недоступна. Доступны только «Бэкапы/Восстановление».",
                reply_markup=_kb_emergency_root(),
            )
            return

    # 2) Обычный пользователь
    try:
        async with get_session() as session:
            res = await session.execute(select(User).where(User.telegram_id == user_id))
            user = res.scalar()
    except Exception:
        user = None

    if user:
        set_audit_user(user.id)
        await message.answer("Главное меню:", reply_markup=await get_main_menu(user.role))
        return

    # Заявка админу
    set_audit_user(None)
    pending_requests[user_id] = message.from_user.full_name or str(user_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Принять",  callback_data=f"approve:{user_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject:{user_id}"),
    ]])
    with contextlib.suppress(Exception):
        await bot.send_message(
            ADMIN_TELEGRAM_ID,
            f"Пользователь {message.from_user.full_name} (@{message.from_user.username or 'без username'}) запросил доступ.",
            reply_markup=kb,
        )
    await message.answer("Ваш запрос отправлен администратору. Ожидайте одобрения.")


# ---------------------------
# Approve / Reject
# ---------------------------
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
        # На случай wipe — поднимем схему и сохраним пользователя
        with contextlib.suppress(Exception):
            await init_db()

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


# ---------------------------
# Разделы-заглушки (если где-то используются)
# ---------------------------
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
    await send_content(cb, "«Отчёты»: модуль в разработке.")

async def back_to_main_menu(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if state:
        await state.clear()
    await cb.message.answer("Главное меню:", reply_markup=await get_main_menu(user.role))


# ---------------------------
# NOOP router (закрыть "часики")
# ---------------------------
noop_router = Router()

@noop_router.callback_query(F.data == "noop")
async def noop_cb(cb: types.CallbackQuery):
    await cb.answer()


# ---------------------------
# Register
# ---------------------------
def register_common_handlers(dp: Dispatcher):
    dp.message.register(cmd_start, CommandStart())
    dp.callback_query.register(handle_admin_decision, lambda c: c.data.startswith(("approve:", "reject:")))

    dp.callback_query.register(on_ostatki,  lambda c: c.data == "ostatki")
    dp.callback_query.register(on_prihod,   lambda c: c.data == "prihod")
    dp.callback_query.register(on_korr_ost, lambda c: c.data == "korr_ost")
    dp.callback_query.register(on_postavki, lambda c: c.data == "postavki")
    dp.callback_query.register(on_otchety,  lambda c: c.data == "otchety")
    dp.callback_query.register(back_to_main_menu, lambda c: c.data == "back_to_menu")

    # Подключаем noop ПОСЛЕДНИМ
    dp.include_router(noop_router)
