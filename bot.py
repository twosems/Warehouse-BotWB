# bot.py
import asyncio
import logging
from aiogram import Bot, Dispatcher
from handlers.user import register_user_handlers
from config import BOT_TOKEN
from handlers.common import RoleCheckMiddleware
from database.db import init_db
from handlers.admin import register_admin_handlers
from handlers.stocks import register_stocks_handlers
from handlers.receiving import register_receiving_handlers
from handlers.supplies import register_supplies_handlers
from handlers.packing import register_packing_handlers
from handlers.reports import register_reports_handlers
from handlers.common import register_common_handlers


logging.basicConfig(level=logging.INFO)

async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    # Одна мидлварь ролей/авторизации
    dp.message.middleware(RoleCheckMiddleware())
    dp.callback_query.middleware(RoleCheckMiddleware())

    # БД: создадим таблицы (если нет). Для продакшена — Alembic.
    await init_db()

    # Сначала админка, затем — общее
    register_admin_handlers(dp)
    register_user_handlers(dp)
    register_common_handlers(dp)
    register_stocks_handlers(dp)
    register_receiving_handlers(dp)
    register_supplies_handlers(dp)
    register_packing_handlers(dp)
    register_reports_handlers(dp)

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
