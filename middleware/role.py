from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from database.db import get_session
from database.models import User, UserRole
from sqlalchemy import select
from handlers.common import AuthState

class RoleMiddleware(BaseMiddleware):
    def __init__(self):
        self.cache = {}  # Простой dict для кэша ролей

    async def get_user_role(self, user_id: int) -> UserRole:
        if user_id in self.cache:
            return self.cache[user_id]
        async with get_session() as session:
            user = await session.execute(select(User).where(User.telegram_id == user_id))
            user_obj = user.scalar()
            role = user_obj.role if user_obj else None
            self.cache[user_id] = role  # Кэшируем
            return role

    async def __call__(self, handler, event, data):
        if isinstance(event, Message):
            if event.text and event.text.startswith("/start"):
                return await handler(event, data)
            state: FSMContext = data.get("state")
            current_state = await state.get_state()
            if current_state == AuthState.password.state:
                return await handler(event, data)
        user_id = event.from_user.id
        user_role = await self.get_user_role(user_id)
        if not user_role:
            await event.answer("Вы не авторизованы. Используйте /start.")
            return
        if isinstance(event, CallbackQuery) and event.data == "admin" and user_role != UserRole.admin:
            await event.answer("Доступ запрещен: только для админов.")
            return
        data["user_role"] = user_role
        return await handler(event, data)