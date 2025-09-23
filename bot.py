# bot.py
import asyncio
import logging

from aiogram import Bot, Dispatcher
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select

from config import BOT_TOKEN, DB_URL, TIMEZONE, ADMIN_TELEGRAM_ID

from database.db import init_db, get_session
from database.models import User, UserRole

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

from handlers import admin_menu_visibility
from handlers.admin_backup import router as admin_backup_router

from scheduler.backup_scheduler import reschedule_backup
from database.menu_visibility import ensure_menu_visibility_defaults

logging.basicConfig(level=logging.INFO)


async def seed_defaults() -> None:
    """
    Делает проект «подъёмным» на чистой БД:
    - создаёт дефолты видимости меню для всех ролей/пунктов;
    - гарантирует наличие администратора по ADMIN_TELEGRAM_ID.
    """
    try:
        # 1) дефолты видимости меню
        async with get_session() as s:
            await ensure_menu_visibility_defaults(s)

        # 2) админ-пользователь (если ещё не создан)
        if ADMIN_TELEGRAM_ID:
            async with get_session() as s:
                existing = (await s.execute(
                    select(User).where(User.telegram_id == ADMIN_TELEGRAM_ID)
                )).scalar_one_or_none()
                if not existing:
                    u = User(
                        telegram_id=ADMIN_TELEGRAM_ID,
                        username="admin",
                        role=UserRole.admin,
                        is_active=True,
                        password_hash="",  # авторизация по Telegram ID
                    )
                    s.add(u)
                    await s.commit()
    except Exception:
        logging.exception("Seed defaults failed")


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty")

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    # --- Middleware (роли/доступ) ---
    dp.message.middleware(RoleCheckMiddleware())
    dp.callback_query.middleware(RoleCheckMiddleware())

    # --- БД (инициализация без фатального падения) ---
    try:
        await init_db()
    except Exception as e:
        logging.exception("DB init failed – starting in EMERGENCY mode. Reason: %r", e)

    # --- Посев дефолтов для чистой БД (важно сделать до /start) ---
    await seed_defaults()

    # --- Планировщик (бэкапы) ---
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.start()

    # Пробрасываем в контекст бота (если где-то понадобится)
    bot.scheduler = scheduler  # type: ignore[attr-defined]
    bot.db_url = DB_URL        # type: ignore[attr-defined]

    async def on_startup():
        """Переназначить джобу бэкапа согласно настройкам в БД."""
        try:
            # Повторно подстрахуемся: если кто-то очистил БД до рестарта сервиса
            await seed_defaults()
            await reschedule_backup(scheduler, TIMEZONE, DB_URL)
        except Exception as e:
            logging.exception("Backup scheduler init skipped (DB may be down): %r", e)

    dp.startup.register(on_startup)

    # --- Роутеры/регистраторы хэндлеров ---
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

    # --- Запуск поллинга ---
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        # Корректное завершение
        scheduler.shutdown(wait=False)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
