from __future__ import annotations
import os
import sys
from pathlib import Path
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config
from dotenv import load_dotenv

# --- Alembic config & logging ---
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --- Путь к проекту, чтобы видеть ваш пакет "database" ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- Загружаем .env и DB_URL ---
load_dotenv(PROJECT_ROOT / ".env")
db_url = os.getenv("DB_URL", "").strip()

# Если DB_URL не задан, можно fallback на alembic.ini (но лучше задайте в .env)
if not db_url:
    db_url = config.get_main_option("sqlalchemy.url", "").strip()

if not db_url:
    raise RuntimeError("DB_URL is not set (neither in .env nor in alembic.ini)")

# Alembic может работать по async-url; оставляем +asyncpg
config.set_main_option("sqlalchemy.url", db_url)

# --- Импортируем metadata ваших моделей ---
# Предполагаю, что у вас Base = DeclarativeBase в database/models.py
from database.models import Base  # noqa: E402

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Запуск миграций в offline-режиме."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,      # сравнивать типы колонок
        render_as_batch=True,   # удобно для SQLite; не мешает для PG
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        render_as_batch=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Запуск миграций в online-режиме (async)."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    import asyncio
    asyncio.run(run_migrations_online())
