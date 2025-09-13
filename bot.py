# bot.py
import asyncio
import logging
from aiogram import Bot, Dispatcher
from handlers import admin_menu_visibility
from config import BOT_TOKEN, DB_URL
from database.db import init_db
from handlers.common import RoleCheckMiddleware, register_common_handlers
from handlers.admin import register_admin_handlers
from handlers.stocks import register_stocks_handlers
from handlers.receiving import register_receiving_handlers
from handlers.supplies import register_supplies_handlers
from handlers.reports import register_reports_handlers
from handlers.back import router as back_router
from handlers.manager import router as manager_router
from handlers.packing import router as packing_router
from handlers.cn_purchase import router as cn_router
from handlers.msk_inbound import router as msk_router
from handlers.menu_info import router as menu_info_router

# === Бэкапы ===
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from scheduler.backup_scheduler import reschedule_backup
from handlers.admin_backup import router as admin_backup_router

logging.basicConfig(level=logging.INFO)

TIMEZONE = "Europe/Berlin"  # можно вынести в .env


async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    # Авторизация/роли
    dp.message.middleware(RoleCheckMiddleware())
    dp.callback_query.middleware(RoleCheckMiddleware())

    # Инициализация БД (без фатального падения при отсутствии базы)
    try:
        await init_db()
    except Exception as e:
        logging.exception("DB init failed – starting in EMERGENCY mode. Reason: %r", e)

    # === Планировщик (бэкапы) ===
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.start()

    # Проброс в bot-контекст (v3: через атрибуты)
    bot.scheduler = scheduler
    bot.db_url = DB_URL

    # Поднять задачи бэкапа по текущим настройкам при старте бота
    async def on_startup():
        try:
            await reschedule_backup(scheduler, TIMEZONE, DB_URL)
        except Exception as e:
            logging.exception("Backup scheduler init skipped (DB may be down): %r", e)

    dp.startup.register(on_startup)

    # === Роутеры/регистраторы ===
    register_admin_handlers(dp)
    dp.include_router(admin_backup_router)
    dp.include_router(admin_menu_visibility.router)

    register_receiving_handlers(dp)
    register_stocks_handlers(dp)
    register_supplies_handlers(dp)

    dp.include_router(back_router)
    dp.include_router(packing_router)
    dp.include_router(manager_router)
    dp.include_router(cn_router)
    dp.include_router(msk_router)
    dp.include_router(menu_info_router)

    register_reports_handlers(dp)
    register_common_handlers(dp)  # общий — ПОСЛЕДНИМ

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        scheduler.shutdown(wait=False)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
