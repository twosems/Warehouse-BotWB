# Сбор кода проекта

## Файл: alembic.ini

```python
# A generic, single database configuration.

[alembic]
# path to migration scripts
# Use forward slashes (/) also on windows to provide an os agnostic path
script_location = alembic

# template used to generate migration file names; The default value is %%(rev)s_%%(slug)s
# Uncomment the line below if you want the files to be prepended with date and time
# see https://alembic.sqlalchemy.org/en/latest/tutorial.html#editing-the-ini-file
# for all available tokens
# file_template = %%(year)d_%%(month).2d_%%(day).2d_%%(hour).2d%%(minute).2d-%%(rev)s_%%(slug)s

# sys.path path, will be prepended to sys.path if present.
# defaults to the current working directory.
prepend_sys_path = .

# timezone to use when rendering the date within the migration file
# as well as the filename.
# If specified, requires the python>=3.9 or backports.zoneinfo library.
# Any required deps can installed by adding `alembic[tz]` to the pip requirements
# string value is passed to ZoneInfo()
# leave blank for localtime
# timezone =

# max length of characters to apply to the "slug" field
# truncate_slug_length = 40

# set to 'true' to run the environment during
# the 'revision' command, regardless of autogenerate
# revision_environment = false

# set to 'true' to allow .pyc and .pyo files without
# a source .py file to be detected as revisions in the
# versions/ directory
# sourceless = false

# version location specification; This defaults
# to alembic/versions.  When using multiple version
# directories, initial revisions must be specified with --version-path.
# The path separator used here should be the separator specified by "version_path_separator" below.
# version_locations = %(here)s/bar:%(here)s/bat:alembic/versions

# version path separator; As mentioned above, this is the character used to split
# version_locations. The default within new alembic.ini files is "os", which uses os.pathsep.
# If this key is omitted entirely, it falls back to the legacy behavior of splitting on spaces and/or commas.
# Valid values for version_path_separator are:
#
# version_path_separator = :
# version_path_separator = ;
# version_path_separator = space
version_path_separator = os  # Use os.pathsep. Default configuration used for new projects.

# set to 'true' to search source files recursively
# in each "version_locations" directory
# new in Alembic version 1.10
# recursive_version_locations = false

# the output encoding used when revision files
# are written from script.py.mako
# output_encoding = utf-8

sqlalchemy.url =


[post_write_hooks]
# post_write_hooks defines scripts or Python functions that are run
# on newly generated revision scripts.  See the documentation for further
# detail and examples

# format using "black" - use the console_scripts runner, against the "black" entrypoint
# hooks = black
# black.type = console_scripts
# black.entrypoint = black
# black.options = -l 79 REVISION_SCRIPT_FILENAME

# lint with attempts to fix using "ruff" - use the exec runner, execute a binary
# hooks = ruff
# ruff.type = exec
# ruff.executable = %(here)s/.venv/bin/ruff
# ruff.options = --fix REVISION_SCRIPT_FILENAME

# Logging configuration
[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S

```

## Файл: bot.py

```python
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

```

## Файл: collect_code.py

```python
import os
import argparse
from pathlib import Path

# Исправленный блок импорта
DOCX_AVAILABLE = False
try:
    from docx import Document
    from docx.shared import Pt
    DOCX_AVAILABLE = True
except ImportError:
    print("Предупреждение: Пакет python-docx не установлен. Используйте --format md.")

def collect_code(project_path, output_format='md'):
    project_path = Path(project_path).resolve()
    if not project_path.is_dir():
        raise ValueError("Указанный путь не является директорией.")

    ignore_dirs = {'venv', '.git', '__pycache__', 'migrations/versions', '.venv'}
    extensions = {'.py', '.md', '.txt', '.ini'}

    content = []
    file_count = 0

    for root, dirs, files in os.walk(project_path):
        dirs[:] = [d for d in dirs if d not in ignore_dirs]
        for file in files:
            if Path(file).suffix in extensions:
                file_path = Path(root) / file
                rel_path = file_path.relative_to(project_path)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        code = f.read()
                    content.append(f"## Файл: {rel_path}\n\n```python\n{code}\n```\n\n")
                    file_count += 1
                    print(f"Обработан: {rel_path}")
                except Exception as e:
                    print(f"Ошибка чтения {rel_path}: {e}")

    if not content:
        raise ValueError("Нет файлов для сбора.")

    output_file = f"all_code.{output_format}"
    if output_format == 'md':
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# Сбор кода проекта\n\n" + ''.join(content))
    elif output_format == 'docx' and DOCX_AVAILABLE:
        doc = Document()
        doc.add_heading('Сбор кода проекта', 0)
        for section in content:
            doc.add_heading(section.split('\n')[0][3:], level=2)
            p = doc.add_paragraph(section.split('```python\n')[1].split('\n```')[0])
            p.style.font.name = 'Courier New'
            p.style.font.size = Pt(10)
        doc.save(output_file)
    else:
        raise ValueError("Формат 'docx' требует python-docx или используйте 'md'.")

    print(f"Готово! Создан файл: {output_file} ({file_count} файлов).")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Сбор кода из проекта.')
    parser.add_argument('path', nargs='?', help='Путь к директории проекта')
    parser.add_argument('--format', default='md', choices=['md', 'docx'], help='Формат вывода: md или docx')
    args = parser.parse_args()

    path = args.path or input("Введите путь к директории проекта: ")
    try:
        collect_code(path, args.format)
    except Exception as e:
        print(f"Ошибка: {e}")
```

## Файл: config.py

```python
# config.py
import os
from dotenv import load_dotenv

# Загружаем переменные из .env (локально); на сервере их задаёт systemd
load_dotenv()


def getenv_bool(name: str, default: bool = False) -> bool:
    """
    Прочитать переменную окружения как bool.
    '1', 'true', 'yes', 'y', 'on' -> True.
    """
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


# --- Telegram ---
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
try:
    ADMIN_TELEGRAM_ID = int((os.getenv("ADMIN_TELEGRAM_ID") or "0").strip())
except Exception:
    ADMIN_TELEGRAM_ID = 0

# --- Database ---
DB_URL = os.getenv(
    "DB_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/warehouse_db",
)

# --- Google (Sheets / Drive, опционально) ---
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")       # путь к credentials.json (если используете Sheets)
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")                        # ID таблицы для экспорта (если нужно)
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")          # Папка в Google Drive (для драйверов oauth/sa)
GOOGLE_DRIVE_PUBLIC = getenv_bool("GOOGLE_DRIVE_PUBLIC", False)

# Совместимость для Google Drive (если когда-нибудь вернёмся к нему)
GOOGLE_AUTH_MODE = (os.getenv("GOOGLE_AUTH_MODE", "oauth") or "oauth").strip().lower()
GOOGLE_OAUTH_CLIENT_PATH = os.getenv("GOOGLE_OAUTH_CLIENT_PATH", "/etc/botwb/google/client_secret_tv.json")
GOOGLE_OAUTH_TOKEN_PATH  = os.getenv("GOOGLE_OAUTH_TOKEN_PATH",  "/etc/botwb/google/token.json")

# --- Backups / pg_dump ---
BACKUP_DIR = os.getenv("BACKUP_DIR", "backups")
PG_DUMP_PATH = os.getenv("PG_DUMP_PATH")  # явный путь к pg_dump, если не в PATH (чаще нужно на Windows)

# --- Выбор драйвера резервного копирования ---
# "webdav" (Яндекс.Диск/Nextcloud), "oauth" (Google OAuth), "sa" (Google Service Account).
BACKUP_DRIVER = (os.getenv("BACKUP_DRIVER", "webdav") or "webdav").strip().lower()

# --- WebDAV (Яндекс.Диск) ---
WEBDAV_BASE_URL = os.getenv("WEBDAV_BASE_URL", "https://webdav.yandex.ru")
WEBDAV_USERNAME = os.getenv("WEBDAV_USERNAME")          # логин (обычно почта)
WEBDAV_PASSWORD = os.getenv("WEBDAV_PASSWORD")          # пароль/пароль приложения
WEBDAV_ROOT     = os.getenv("WEBDAV_ROOT", "/botwb")     # удалённая папка на диске

# --- Timezone / Logging ---
TIMEZONE = os.getenv("TIMEZONE") or os.getenv("timezone") or "Europe/Berlin"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# --- Доп. переменные (если где-то используются) ---
DB_NAME = os.getenv("DB_NAME", "warehouse_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
PG_BIN  = os.getenv("PG_BIN")  # только для Windows, если нужно явно задать bin-папку

```

## Файл: get_token.py

```python
# get_token.py — генерирует token.json для Google Drive (scope drive.file)
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

flow = InstalledAppFlow.from_client_secrets_file(
    "client_secret.json", SCOPES
)

# Вариант с локальным браузером (удобно на Windows):
creds = flow.run_local_server(port=0)

# Если вдруг браузер не открылся — поменяй строку выше на:
# creds = flow.run_console()

with open("token.json", "w", encoding="utf-8") as f:
    f.write(creds.to_json())

print("✅ token.json создан рядом со скриптом.")

```

## Файл: mypy.ini

```python

```

## Файл: README.md

```python

```

## Файл: requirements.txt

```python
aiogram==3.13.1
sqlalchemy==2.0.35
asyncpg==0.30.0
python-dotenv==1.0.1
bcrypt==4.2.0
greenlet==3.2.3
apscheduler==3.10.4
google-api-python-client==2.143.0
google-auth==2.34.0
google-auth-httplib2==0.2.0
alembic>=1.16
python-dotenv
httpx>=0.27,<0.28
requests==2.32.3
python-docx==1.1.2  # Для DOCX, опционально
```

## Файл: alembic\env.py

```python
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

```

## Файл: alembic\versions\20250922_supplies_v1.py

```python
"""supplies v1: statuses, boxes, files, audit fields

Revision ID: 20250922_supplies_v1
Revises: 4b69a9e3e759
Create Date: 2025-09-22 10:00:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20250922_supplies_v1"
down_revision = "4b69a9e3e759"
branch_labels = None
depends_on = None


def upgrade():
    # 1) ENUM supply_status
    op.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'supply_status') THEN
                CREATE TYPE supply_status AS ENUM (
                    'draft','queued','assembling','assembled','in_transit',
                    'archived_delivered','archived_returned','cancelled'
                );
            END IF;
        END$$;
    """)

    # 2) supplies: статус + реквизиты + таймстемпы
    with op.batch_alter_table("supplies") as batch:
        batch.add_column(sa.Column("status", sa.Enum(name="supply_status", create_type=False),
                                   nullable=False, server_default="draft"))
        batch.add_column(sa.Column("mp", sa.String(16), nullable=True))               # 'wb' | 'ozon'
        batch.add_column(sa.Column("mp_warehouse", sa.String(128), nullable=True))    # строкой для MVP
        batch.add_column(sa.Column("assigned_picker_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("comment", sa.String(), nullable=True))
        batch.add_column(sa.Column("queued_at", sa.DateTime(timezone=False)))
        batch.add_column(sa.Column("assembled_at", sa.DateTime(timezone=False)))
        batch.add_column(sa.Column("posted_at", sa.DateTime(timezone=False)))
        batch.add_column(sa.Column("delivered_at", sa.DateTime(timezone=False)))
        batch.add_column(sa.Column("returned_at", sa.DateTime(timezone=False)))
        batch.add_column(sa.Column("unposted_at", sa.DateTime(timezone=False)))

    op.create_index("ix_supplies_status", "supplies", ["status"])
    op.create_index("ix_supplies_warehouse", "supplies", ["warehouse_id"])

    # 3) supply_boxes
    op.create_table(
        "supply_boxes",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("supply_id", sa.Integer, sa.ForeignKey("supplies.id", ondelete="CASCADE"),
                  index=True, nullable=False),
        sa.Column("box_number", sa.Integer, nullable=False),  # 1..N
        sa.Column("sealed", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.UniqueConstraint("supply_id", "box_number", name="uq_supply_box_number")
    )

    # 4) supply_items: привязка к коробу и индекс на supply_id
    with op.batch_alter_table("supply_items") as batch:
        batch.add_column(sa.Column("box_id", sa.Integer,
                                   sa.ForeignKey("supply_boxes.id", ondelete="CASCADE"),
                                   nullable=True))
    op.create_index("ix_supply_items_supply", "supply_items", ["supply_id"])

    # 5) supply_files (PDF)
    op.create_table(
        "supply_files",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("supply_id", sa.Integer, sa.ForeignKey("supplies.id", ondelete="CASCADE"),
                  index=True, nullable=False),
        sa.Column("file_id", sa.String(256), nullable=False),
        sa.Column("filename", sa.String(255)),
        sa.Column("uploaded_by", sa.Integer, sa.ForeignKey("users.id")),
        sa.Column("uploaded_at", sa.DateTime(timezone=False),
                  server_default=sa.text("CURRENT_TIMESTAMP"))
    )


def downgrade():
    op.drop_table("supply_files")
    op.drop_index("ix_supply_items_supply", table_name="supply_items")
    with op.batch_alter_table("supply_items") as batch:
        batch.drop_column("box_id")
    op.drop_table("supply_boxes")

    op.drop_index("ix_supplies_status", table_name="supplies")
    op.drop_index("ix_supplies_warehouse", table_name="supplies")
    with op.batch_alter_table("supplies") as batch:
        for col in ["status","mp","mp_warehouse","assigned_picker_id","comment",
                    "queued_at","assembled_at","posted_at","delivered_at","returned_at","unposted_at"]:
            batch.drop_column(col)

    op.execute("DROP TYPE IF EXISTS supply_status")

```

## Файл: alembic\versions\4b69a9e3e759_cn_photos_msk_to_our_at.py

```python
from alembic import op
import sqlalchemy as sa

# --- identifiers ---
revision = "4b69a9e3e759"
down_revision = "d3783dd38de1"  # merge-head
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # 1) Таблица фото для CN (создадим, если её ещё нет)
    if "cn_purchase_photos" not in insp.get_table_names():
        op.create_table(
            "cn_purchase_photos",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column(
                "cn_purchase_id",
                sa.Integer(),
                sa.ForeignKey("cn_purchases.id", ondelete="CASCADE"),
                nullable=False,
                index=True,
            ),
            sa.Column("file_id", sa.String(length=256), nullable=False),
            sa.Column("caption", sa.String(length=512), nullable=True),
            sa.Column(
                "uploaded_at",
                sa.DateTime(),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            ),
            sa.Column("uploaded_by_user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=True),
        )
        op.create_index(
            "ix_cn_purchase_photos_purchase_id",
            "cn_purchase_photos",
            ["cn_purchase_id"],
        )

    # 2) Колонка to_our_at в msk_inbound_docs (если ещё нет)
    cols = [c["name"] for c in insp.get_columns("msk_inbound_docs")]
    if "to_our_at" not in cols:
        op.add_column("msk_inbound_docs", sa.Column("to_our_at", sa.DateTime(), nullable=True))


def downgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # Откатываем колонку
    cols = [c["name"] for c in insp.get_columns("msk_inbound_docs")]
    if "to_our_at" in cols:
        op.drop_column("msk_inbound_docs", "to_our_at")

    # Откатываем таблицу фото (и индекс), если есть
    if "cn_purchase_photos" in insp.get_table_names():
        # индекс мог и не существовать, поэтому аккуратно
        try:
            op.drop_index("ix_cn_purchase_photos_purchase_id", table_name="cn_purchase_photos")
        except Exception:
            pass
        op.drop_table("cn_purchase_photos")

```

## Файл: alembic\versions\cfe2e63e5c02_add_menu_items_to_menu_item_enum.py

```python
from alembic import op

# ревизии
revision = "add_menu_items_enum_2cats"
down_revision = "d07a5ed359a8"
 # возьми из вывода `alembic current`

def upgrade():
    for val in ("picking", "purchase_cn", "msk_warehouse"):
        op.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_type t
                JOIN pg_enum e ON t.oid = e.enumtypid
                WHERE t.typname = 'menu_item_enum' AND e.enumlabel = '{val}'
            ) THEN
                ALTER TYPE menu_item_enum ADD VALUE '{val}';
            END IF;
        END$$;
        """)

def downgrade():
    # удалять значения из ENUM нельзя — оставляем пустым
    pass

```

## Файл: alembic\versions\d07a5ed359a8_init_schema.py

```python
"""init schema

Revision ID: d07a5ed359a8
Revises: 
Create Date: 2025-09-11 21:12:40.948850

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'd07a5ed359a8'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('cn_purchase_items', schema=None) as batch_op:
        batch_op.alter_column('comment',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               existing_nullable=True)
        batch_op.drop_index('ix_cn_purchase_items_purchase')

    with op.batch_alter_table('cn_purchases', schema=None) as batch_op:
        batch_op.alter_column('comment',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               existing_nullable=True)
        batch_op.drop_index('ix_cn_purchases_status_created')

    with op.batch_alter_table('msk_inbound_docs', schema=None) as batch_op:
        batch_op.alter_column('comment',
               existing_type=sa.TEXT(),
               type_=sa.String(),
               existing_nullable=True)
        batch_op.drop_index('ix_msk_inbound_status_created')
        batch_op.drop_constraint('msk_inbound_docs_target_warehouse_id_fkey', type_='foreignkey')
        batch_op.drop_constraint('msk_inbound_docs_warehouse_id_fkey', type_='foreignkey')
        batch_op.create_foreign_key(None, 'warehouses', ['target_warehouse_id'], ['id'])
        batch_op.drop_column('warehouse_id')

    with op.batch_alter_table('msk_inbound_items', schema=None) as batch_op:
        batch_op.drop_index('ix_msk_inbound_items_doc')

    with op.batch_alter_table('pack_doc_items', schema=None) as batch_op:
        batch_op.drop_index('ix_pack_doc_items_doc')
        batch_op.drop_constraint('pack_doc_items_doc_id_fkey', type_='foreignkey')
        batch_op.create_foreign_key(None, 'pack_docs', ['doc_id'], ['id'])

    with op.batch_alter_table('pack_docs', schema=None) as batch_op:
        batch_op.alter_column('created_at',
               existing_type=postgresql.TIMESTAMP(timezone=True),
               type_=sa.DateTime(),
               existing_nullable=False,
               existing_server_default=sa.text('now()'))
        batch_op.alter_column('status',
               existing_type=postgresql.ENUM('draft', 'posted', name='packdocstatus'),
               type_=sa.Enum('draft', 'posted', name='pack_doc_status_enum'),
               existing_nullable=False,
               existing_server_default=sa.text("'draft'::packdocstatus"))
        batch_op.drop_index('ix_pack_docs_created')
        batch_op.drop_index('ux_pack_docs_wh_number')

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('pack_docs', schema=None) as batch_op:
        batch_op.create_index('ux_pack_docs_wh_number', ['warehouse_id', 'number'], unique=True)
        batch_op.create_index('ix_pack_docs_created', ['created_at'], unique=False)
        batch_op.alter_column('status',
               existing_type=sa.Enum('draft', 'posted', name='pack_doc_status_enum'),
               type_=postgresql.ENUM('draft', 'posted', name='packdocstatus'),
               existing_nullable=False,
               existing_server_default=sa.text("'draft'::packdocstatus"))
        batch_op.alter_column('created_at',
               existing_type=sa.DateTime(),
               type_=postgresql.TIMESTAMP(timezone=True),
               existing_nullable=False,
               existing_server_default=sa.text('now()'))

    with op.batch_alter_table('pack_doc_items', schema=None) as batch_op:
        batch_op.drop_constraint(None, type_='foreignkey')
        batch_op.create_foreign_key('pack_doc_items_doc_id_fkey', 'pack_docs', ['doc_id'], ['id'], ondelete='CASCADE')
        batch_op.create_index('ix_pack_doc_items_doc', ['doc_id'], unique=False)

    with op.batch_alter_table('msk_inbound_items', schema=None) as batch_op:
        batch_op.create_index('ix_msk_inbound_items_doc', ['msk_inbound_id'], unique=False)

    with op.batch_alter_table('msk_inbound_docs', schema=None) as batch_op:
        batch_op.add_column(sa.Column('warehouse_id', sa.INTEGER(), autoincrement=False, nullable=True))
        batch_op.drop_constraint(None, type_='foreignkey')
        batch_op.create_foreign_key('msk_inbound_docs_warehouse_id_fkey', 'warehouses', ['warehouse_id'], ['id'])
        batch_op.create_foreign_key('msk_inbound_docs_target_warehouse_id_fkey', 'warehouses', ['target_warehouse_id'], ['id'], onupdate='CASCADE', ondelete='SET NULL')
        batch_op.create_index('ix_msk_inbound_status_created', ['status', sa.text('created_at DESC')], unique=False)
        batch_op.alter_column('comment',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               existing_nullable=True)

    with op.batch_alter_table('cn_purchases', schema=None) as batch_op:
        batch_op.create_index('ix_cn_purchases_status_created', ['status', sa.text('created_at DESC')], unique=False)
        batch_op.alter_column('comment',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               existing_nullable=True)

    with op.batch_alter_table('cn_purchase_items', schema=None) as batch_op:
        batch_op.create_index('ix_cn_purchase_items_purchase', ['cn_purchase_id'], unique=False)
        batch_op.alter_column('comment',
               existing_type=sa.String(),
               type_=sa.TEXT(),
               existing_nullable=True)

    # ### end Alembic commands ###

```

## Файл: alembic\versions\d3783dd38de1_merge_heads.py

```python
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "d3783dd38de1"
down_revision = ("add_menu_items_enum_2cats", "fd0dbb27ea7a")
branch_labels = None
depends_on = None


def upgrade():
    # merge point; схема не меняется
    pass


def downgrade():
    # откат merge'а ничего не делает
    pass

```

## Файл: alembic\versions\fd0dbb27ea7a_pack_docs_add_notes_column.py

```python
"""pack_docs: add notes column

Revision ID: fd0dbb27ea7a
Revises: d07a5ed359a8
Create Date: 2025-09-11
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "fd0dbb27ea7a"
down_revision: str | None = "d07a5ed359a8"
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column("pack_docs", sa.Column("notes", sa.String(length=255), nullable=True))

def downgrade() -> None:
    op.drop_column("pack_docs", "notes")

```

## Файл: database\db.py

```python
# database/db.py
# Async engine + session, авто-создание БД, устойчивый пул, JSON-safe аудит,
# дефолты видимости меню и хелперы для restore.

from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncGenerator, Optional

import enum
from datetime import datetime, date, time
from decimal import Decimal

from sqlalchemy import select, event, text, func
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import Session

from config import DB_URL
from database.models import Base, Warehouse, AuditLog, AuditAction
from database.menu_visibility import ensure_menu_visibility_defaults

# для хелпера available_packed
from database.models import (
    StockMovement, ProductStage,
    Supply, SupplyItem,
)


# ---------------------------
# Engine & session factory (устойчивый пул)
# ---------------------------
engine = create_async_engine(
    DB_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,   # прозрачно чинит "connection is closed"
    pool_recycle=1800,    # раз в 30 минут обновляет соединения
)
SessionFactory = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


async def reset_db_engine() -> None:
    """
    Полностью пересобрать engine/SessionFactory (например, сразу после restore).
    """
    global engine, SessionFactory
    try:
        await engine.dispose(close=True)
    except Exception:
        pass
    engine = create_async_engine(
        DB_URL,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_recycle=1800,
    )
    SessionFactory = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


async def ping_db() -> None:
    """
    Лёгкая проверка доступности БД и «прогрев» пула.
    """
    async with SessionFactory() as s:
        await s.execute(text("SELECT 1"))

async def ensure_database_exists() -> None:
    """
    Если целевой базы (из DB_URL) нет — создаёт её.
    Если template1 не в UTF8 (как на Windows с WIN1251), создаём из template0
    c LC_COLLATE/LC_CTYPE = 'C', чтобы не было конфликта кодировок.
    """
    url = make_url(DB_URL)
    target_db = url.database
    owner = url.username or "postgres"

    admin_url = url.set(database="postgres")
    admin_engine = create_async_engine(
        admin_url,
        echo=False,
        future=True,
        pool_pre_ping=True,
    )
    try:
        async with admin_engine.connect() as conn:
            # Уже есть?
            exists = await conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :n"),
                {"n": target_db},
            )
            if exists:
                return

            # Смотрим кодировку template1
            row = await conn.execute(
                text("""
                    SELECT pg_encoding_to_char(encoding) AS enc,
                           datcollate, datctype
                    FROM pg_database
                    WHERE datname = 'template1'
                """)
            )
            enc, collate, ctype = row.first()

            def _create_db(sync_conn):
                # отдельное sync-подключение в AUTOCOMMIT
                with sync_conn.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as ac:
                    if (enc or "").upper() == "UTF8":
                        # можно спокойно от template1
                        ac.exec_driver_sql(
                            f'CREATE DATABASE "{target_db}" '
                            f'OWNER "{owner}" ENCODING \'UTF8\' TEMPLATE template1'
                        )
                    else:
                        # универсально: от template0 с нейтральной локалью
                        ac.exec_driver_sql(
                            f'CREATE DATABASE "{target_db}" '
                            f'OWNER "{owner}" ENCODING \'UTF8\' '
                            f'LC_COLLATE \'C\' LC_CTYPE \'C\' '
                            f'TEMPLATE template0'
                        )

            await conn.run_sync(_create_db)
    finally:
        await admin_engine.dispose(close=True)



# ---------------------------
# Текущий пользователь для аудита (ставим из middleware/handler)
# ---------------------------
_current_audit_user_id: ContextVar[Optional[int]] = ContextVar("current_audit_user_id", default=None)


def set_audit_user(user_id: Optional[int]) -> None:
    """
    Установить текущего пользователя для записи в AuditLog.user_id.
    Вызывай, например, в middleware перед обработкой апдейта.
    """
    _current_audit_user_id.set(user_id)


# ---------------------------
# Public API
# ---------------------------
async def init_db() -> None:
    """
    Создаёт БД при её отсутствии, затем таблицы, регистрирует аудит
    и гарантирует дефолтные настройки видимости меню.
    """
    # 1) Если базы нет после DROP DATABASE — создадим её
    await ensure_database_exists()

    # 2) Создадим таблицы
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 3) Аудит (после create_all, чтобы таблица audit_logs точно была)
    register_audit_listeners()

    # 4) Дефолтные настройки видимости меню (безопасно вызывать повторно)
    async with get_session() as session:
        await ensure_menu_visibility_defaults(session)


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with SessionFactory() as session:
        try:
            yield session
        finally:
            await session.close()


async def ensure_core_data() -> None:
    """
    Создаём базовые склады, если их нет.
    """
    needed = ["Санкт-Петербург", "Томск"]
    async with get_session() as session:
        existing = (await session.execute(select(Warehouse))).scalars().all()
        existing_names = {w.name for w in existing}
        to_add = [Warehouse(name=name, is_active=True) for name in needed if name not in existing_names]
        if to_add:
            session.add_all(to_add)
            await session.commit()


# ---------------------------
# Stock helpers (важно: supplies.status — VARCHAR)
# ---------------------------
async def available_packed(session: AsyncSession, warehouse_id: int, product_id: int) -> int:
    """
    Доступный PACKED = фактический PACKED - сумма qty в активных поставках
    (status in 'assembling'|'assembled'|'in_transit') по этому складу/товару.
    """
    fact = await session.scalar(
        select(func.coalesce(func.sum(StockMovement.qty), 0))
        .where(
            StockMovement.warehouse_id == warehouse_id,
            StockMovement.product_id == product_id,
            StockMovement.stage == ProductStage.packed,
            )
    )

    # колонка supplies.status у вас VARCHAR → сравниваем со строками
    active_status = ("assembling", "assembled", "in_transit")
    reserved = await session.scalar(
        select(func.coalesce(func.sum(SupplyItem.qty), 0))
        .join(Supply, Supply.id == SupplyItem.supply_id)
        .where(
            Supply.warehouse_id == warehouse_id,
            Supply.status.in_(active_status),
            SupplyItem.product_id == product_id,
            )
    )
    return int((fact or 0) - (reserved or 0))


# ---------------------------
# Audit helpers (JSON-safe)
# ---------------------------
def _to_plain(value):
    """Рекурсивно приводит к JSON-совместимым типам (enum -> .value, даты -> ISO, Decimal -> float и т.п.)."""
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_plain(v) for v in value]
    # На всякий случай: Row -> dict
    try:
        from sqlalchemy.engine import Row
        if isinstance(value, Row):
            return {k: _to_plain(value[k]) for k in value.keys()}
    except Exception:
        pass
    return value


def _row_as_dict_plain(obj) -> dict:
    insp = sa_inspect(obj)
    data = {}
    for attr in insp.mapper.column_attrs:
        key = attr.key
        data[key] = _to_plain(getattr(obj, key))
    return data


def _diff_for_update_plain(obj) -> dict:
    insp = sa_inspect(obj)
    dif = {}
    for attr in insp.mapper.column_attrs:
        hist = insp.attrs[attr.key].history
        if hist.has_changes():
            old_val = hist.deleted[0] if hist.deleted else None
            new_val = hist.added[0] if hist.added else getattr(obj, attr.key)
            old_val = _to_plain(old_val)
            new_val = _to_plain(new_val)
            if old_val != new_val:
                dif[attr.key] = {"old": old_val, "new": new_val}
    return dif


def register_audit_listeners() -> None:
    """
    Подписка на ORM-события, чтобы писать AuditLog для INSERT/UPDATE/DELETE.
    Работает и с AsyncSession, т.к. слушатель висит на sync Session-классе.
    """
    @event.listens_for(Session, "after_flush")
    def _audit_after_flush(session: Session, flush_context) -> None:
        # чтобы не зациклиться
        def skip(obj) -> bool:
            return obj.__class__.__name__ == "AuditLog"

        conn = session.connection()
        uid = _current_audit_user_id.get()

        # INSERT
        for obj in session.new:
            if skip(obj):
                continue
            table = getattr(obj, "__tablename__", obj.__class__.__name__)
            pk = sa_inspect(obj).identity
            conn.execute(
                AuditLog.__table__.insert().values(
                    user_id=uid,
                    action=AuditAction.insert,
                    table_name=table,
                    record_pk=str(pk),
                    old_data=None,
                    new_data=_row_as_dict_plain(obj),  # JSON-safe
                    diff=None,
                )
            )

        # UPDATE
        for obj in session.dirty:
            if skip(obj) or not session.is_modified(obj, include_collections=False):
                continue
            table = getattr(obj, "__tablename__", obj.__class__.__name__)
            pk = sa_inspect(obj).identity
            dif = _diff_for_update_plain(obj)  # JSON-safe
            if not dif:
                continue
            conn.execute(
                AuditLog.__table__.insert().values(
                    user_id=uid,
                    action=AuditAction.update,
                    table_name=table,
                    record_pk=str(pk),
                    old_data={k: v["old"] for k, v in dif.items()},
                    new_data={k: v["new"] for k, v in dif.items()},
                    diff=dif,
                )
            )

        # DELETE
        for obj in session.deleted:
            if skip(obj):
                continue
            table = getattr(obj, "__tablename__", obj.__class__.__name__)
            pk = sa_inspect(obj).identity
            conn.execute(
                AuditLog.__table__.insert().values(
                    user_id=uid,
                    action=AuditAction.delete,
                    table_name=table,
                    record_pk=str(pk),
                    old_data=_row_as_dict_plain(obj),  # JSON-safe
                    new_data=None,
                    diff=None,
                )
            )

```

## Файл: database\menu_visibility.py

```python
from __future__ import annotations
from typing import Optional, Set, Dict, List

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import RoleMenuVisibility, UserRole, MenuItem

# Человекочитаемые подписи и порядок показа в редакторе (единая точка правды)
LABELS: Dict[MenuItem, str] = {
    MenuItem.stocks:        "📦 Остатки",
    MenuItem.receiving:     "➕ Поступление",
    MenuItem.supplies:      "🚚 Поставки",
    MenuItem.packing:       "🎁 Упаковка",
    MenuItem.picking:       "🧰 Сборка",
    MenuItem.reports:       "📈 Отчёты",
    MenuItem.purchase_cn:   "🇨🇳 Закупка CN",
    MenuItem.msk_warehouse: "🏢 Склад MSK",

    MenuItem.admin:         "⚙️ Администрирование",
}
# Короткие описания пунктов меню (единая точка правды)
DESCRIPTIONS: Dict[MenuItem, str] = {
    MenuItem.stocks:        "Показать текущие остатки по складам и товарам.",
    MenuItem.receiving:     "Оформить приход товара на выбранный склад.",
    MenuItem.supplies:      "Сформировать поставку на склады маркетплейса.",
    MenuItem.packing:       "Подготовить и упаковать позиции к поставке.",
    MenuItem.picking:       "Скомплектовать товары перед упаковкой/поставкой.",
    MenuItem.reports:       "Сформировать отчёты по остаткам, движениям и истории.",
    MenuItem.purchase_cn:   "Учёт закупок/поступлений из Китая (CN).",
    MenuItem.msk_warehouse: "Операции и движения на московский склад.",
    MenuItem.admin:         "Администрирование: пользователи, склады, настройки.",
}

MENU_ORDER: List[MenuItem] = [
    MenuItem.stocks,
    MenuItem.receiving,
    MenuItem.supplies,
    MenuItem.packing,
    MenuItem.picking,
    MenuItem.reports,
    MenuItem.purchase_cn,
    MenuItem.msk_warehouse,
    MenuItem.admin,
]

# Базовые дефолты (явно заданные пункты)
DEFAULT_VISIBILITY: Dict[UserRole, Dict[MenuItem, bool]] = {
    # Админ: вообще всё (включая будущие пункты — см. fallback ниже)
    UserRole.admin: {
        # можно ничего не перечислять — fallback покроет True на любые новые пункты
    },
    # Пользователь
    UserRole.user: {
        MenuItem.stocks:        True,
        MenuItem.receiving:     True,
        MenuItem.supplies:      True,
        MenuItem.packing:       True,
        MenuItem.picking:       True,
        MenuItem.reports:       True,
        MenuItem.purchase_cn:   True,
        MenuItem.msk_warehouse: True,
        MenuItem.admin:         False,
    },
    # Менеджер
    UserRole.manager: {
        MenuItem.stocks:        True,
        MenuItem.receiving:     False,
        MenuItem.supplies:      True,
        MenuItem.packing:       True,
        MenuItem.picking:       True,
        MenuItem.reports:       True,
        MenuItem.purchase_cn:   False,
        MenuItem.msk_warehouse: False,
        MenuItem.admin:         False,
    },
}

def _default_visible(role: UserRole, item: MenuItem) -> bool:
    """
    Фолбэк-дефолт для незаданных явно пунктов:
      - admin -> True (видит всё)
      - user/manager -> False (безопасный дефолт)
    """
    if role == UserRole.admin:
        return True
    return DEFAULT_VISIBILITY.get(role, {}).get(item, False)

async def ensure_menu_visibility_defaults(session: AsyncSession) -> None:
    """
    Гарантируем, что в role_menu_visibility есть записи для всех (role, item):
      - для admin: True по умолчанию,
      - для остальных ролей: False, если не задано иное в DEFAULT_VISIBILITY.
    Ничего не перезаписываем — только добавляем отсутствующие пары.
    """
    # Читаем все существующие пары (role, item)
    res = await session.execute(
        select(RoleMenuVisibility.role, RoleMenuVisibility.item)
    )
    existing = {(row[0], row[1]) for row in res.all()}

    to_add: list[RoleMenuVisibility] = []
    for role in UserRole:
        for item in MenuItem:
            if (role, item) not in existing:
                default = DEFAULT_VISIBILITY.get(role, {}).get(item, _default_visible(role, item))
                to_add.append(RoleMenuVisibility(role=role, item=item, visible=default))

    if to_add:
        session.add_all(to_add)
        await session.commit()

async def get_visible_menu_items_for_role(
        session: AsyncSession,
        role: UserRole,
) -> Set[MenuItem]:
    """
    Возвращает множество пунктов меню, видимых для роли.
    """
    res = await session.execute(
        select(RoleMenuVisibility.item).where(
            RoleMenuVisibility.role == role,
            RoleMenuVisibility.visible.is_(True),
            )
    )
    return {row[0] for row in res.all()}

async def get_visibility_map_for_role(
        session: AsyncSession,
        role: UserRole,
) -> Dict[MenuItem, bool]:
    """
    Возвращает словарь {MenuItem: visible} для роли — удобно для экрана редактора.
    Если каких-то пунктов внезапно нет — не беда, вернём False (но ensure_* лучше вызвать).
    """
    res = await session.execute(
        select(RoleMenuVisibility.item, RoleMenuVisibility.visible).where(
            RoleMenuVisibility.role == role
        )
    )
    data = {row[0]: row[1] for row in res.all()}
    # на всякий случай заполним отсутствующие ключи фолбэком
    for mi in MenuItem:
        data.setdefault(mi, _default_visible(role, mi))
    return data

async def toggle_menu_visibility(
        session: AsyncSession,
        role: UserRole,
        item: MenuItem,
        value: Optional[bool] = None,
) -> bool:
    """
    Переключить видимость или поставить явное значение.
    Возвращает текущее значение после операции.
    """
    res = await session.execute(
        select(RoleMenuVisibility).where(
            RoleMenuVisibility.role == role,
            RoleMenuVisibility.item == item,
            ).limit(1)
    )
    vm = res.scalar_one_or_none()

    if vm is None:
        # если записи нет — создадим с корректным дефолтом или переданным value
        default = _default_visible(role, item)
        vm = RoleMenuVisibility(
            role=role, item=item, visible=default if value is None else bool(value)
        )
        session.add(vm)
        await session.commit()
        return vm.visible

    new_val = (not vm.visible) if value is None else bool(value)
    if new_val == vm.visible:
        return vm.visible

    await session.execute(
        update(RoleMenuVisibility)
        .where(RoleMenuVisibility.id == vm.id)
        .values(visible=new_val)
    )
    await session.commit()
    return new_val

```

## Файл: database\models.py

```python
from __future__ import annotations

import enum
from datetime import datetime
from typing import Optional, List

from sqlalchemy import (
    Column, Integer, String, Enum, BigInteger, TIMESTAMP, Boolean,
    ForeignKey, UniqueConstraint, Numeric, DateTime,
)
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql import func

Base = declarative_base()

# ===== Enums =====

class UserRole(enum.Enum):
    admin = "admin"
    user = "user"
    manager = "manager"


class MovementType(enum.Enum):
    prihod = "prihod"
    korrekt = "korrekt"
    postavka = "postavka"
    upakovka = "upakovka"  # для упаковки


class AuditAction(enum.Enum):
    insert = "insert"
    update = "update"
    delete = "delete"


class MenuItem(enum.Enum):
    stocks = "stocks"
    receiving = "receiving"
    supplies = "supplies"
    packing = "packing"
    picking = "picking"
    reports = "reports"
    purchase_cn = "purchase_cn"
    msk_warehouse = "msk_warehouse"
    admin = "admin"


class ProductStage(enum.Enum):
    raw = "raw"
    packed = "packed"


# ——— Новые enum'ы для закупки CN и входящих МСК ———

class CnPurchaseStatus(enum.Enum):
    PURCHASED = "1_purchased"
    SENT_TO_CARGO = "2_sent_to_cargo"
    SENT_TO_MSK = "3_sent_to_msk"
    DELIVERED_TO_MSK = "4_delivered_to_msk"


class MskInboundStatus(enum.Enum):
    PENDING = "pending"
    RECEIVED = "received"


# ===== Core tables =====

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    name = Column(String(255))
    role = Column(Enum(UserRole, name="user_role_enum"), nullable=False)
    password_hash = Column(String(255), nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())


class Warehouse(Base):
    __tablename__ = "warehouses"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    is_active = Column(Boolean, default=True)


class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    article = Column(String(100), unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    is_active = Column(Boolean, default=True)


class StockMovement(Base):
    __tablename__ = "stock_movements"
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"))
    product_id = Column(Integer, ForeignKey("products.id"))
    qty = Column(Integer, nullable=False)
    type = Column(Enum(MovementType, name="movement_type_enum"), nullable=False)
    date = Column(TIMESTAMP, server_default=func.current_timestamp())
    doc_id = Column(Integer)
    user_id = Column(Integer, ForeignKey("users.id"))
    comment = Column(String)
    stage = Column(
        Enum(ProductStage, name="product_stage_enum"),
        nullable=False,
        default=ProductStage.packed
    )


# ===== SUPPLIES (ТЗ v1.0) =====

class SupplyStatus(enum.Enum):
    draft = "draft"
    queued = "queued"
    assembling = "assembling"
    assembled = "assembled"
    in_transit = "in_transit"
    archived_delivered = "archived_delivered"
    archived_returned = "archived_returned"
    cancelled = "cancelled"


class Supply(Base):
    __tablename__ = "supplies"
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"))
    created_by = Column(Integer, ForeignKey("users.id"))
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())

    status = Column(SAEnum(SupplyStatus, name="supply_status", create_type=False),
                    nullable=False, default=SupplyStatus.draft)

    mp = Column(String(16))                # 'wb' | 'ozon'
    mp_warehouse = Column(String(128))     # код/имя МП-склада
    assigned_picker_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    comment = Column(String)

    queued_at = Column(DateTime)
    assembled_at = Column(DateTime)
    posted_at = Column(DateTime)
    delivered_at = Column(DateTime)
    returned_at = Column(DateTime)
    unposted_at = Column(DateTime)

    items: Mapped[List["SupplyItem"]] = relationship(
        back_populates="supply", cascade="all, delete-orphan"
    )


class SupplyBox(Base):
    __tablename__ = "supply_boxes"
    id = Column(Integer, primary_key=True)
    supply_id = Column(Integer, ForeignKey("supplies.id", ondelete="CASCADE"), index=True, nullable=False)
    box_number = Column(Integer, nullable=False)
    sealed = Column(Boolean, nullable=False, default=False)
    # items связь не обязательна в ORM, используем из SupplyItem.box_id


class SupplyItem(Base):
    __tablename__ = "supply_items"
    id = Column(Integer, primary_key=True)
    supply_id = Column(Integer, ForeignKey("supplies.id"))
    product_id = Column(Integer, ForeignKey("products.id"))
    qty = Column(Integer, nullable=False)
    box_id = Column(Integer, ForeignKey("supply_boxes.id", ondelete="CASCADE"), nullable=True)

    supply: Mapped["Supply"] = relationship(back_populates="items")


class SupplyFile(Base):
    __tablename__ = "supply_files"
    id = Column(Integer, primary_key=True)
    supply_id = Column(Integer, ForeignKey("supplies.id", ondelete="CASCADE"), index=True, nullable=False)
    file_id = Column(String(256), nullable=False)  # Telegram file_id
    filename = Column(String(255))
    uploaded_by = Column(Integer, ForeignKey("users.id"))
    uploaded_at = Column(DateTime, server_default=func.current_timestamp())


# ===== Role-based menu visibility =====

class RoleMenuVisibility(Base):
    __tablename__ = "role_menu_visibility"
    __table_args__ = (UniqueConstraint("role", "item", name="uq_role_menu_item"),)

    id = Column(Integer, primary_key=True)
    role = Column(Enum(UserRole, name="user_role_enum"), nullable=False)
    item = Column(Enum(MenuItem, name="menu_item_enum"), nullable=False)
    visible = Column(Boolean, nullable=False, default=True)


# ===== Audit log =====

class AuditLog(Base):
    __tablename__ = "audit_logs"
    id = Column(Integer, primary_key=True)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp(), nullable=False)

    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    action = Column(Enum(AuditAction, name="audit_action_enum"), nullable=False)
    table_name = Column(String(64), nullable=False)
    record_pk = Column(String(128))
    old_data = Column(JSONB)
    new_data = Column(JSONB)
    diff = Column(JSONB)


# ===== Упаковка (Pack Docs) =====

class PackDocStatus(enum.Enum):
    draft = "draft"
    posted = "posted"


class PackDoc(Base):
    __tablename__ = "pack_docs"

    id: Mapped[int] = mapped_column(primary_key=True)
    number: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))

    # Новая колонка
    notes: Mapped[Optional[str]] = mapped_column(String(255))

    status: Mapped[PackDocStatus] = mapped_column(
        SAEnum(
            PackDocStatus,
            name="packdocstatus",
            create_type=False,
            native_enum=True
        ),
        default=PackDocStatus.draft,
        nullable=False
    )

    comment: Mapped[Optional[str]] = mapped_column(String(255))

    warehouse: Mapped["Warehouse"] = relationship()
    items: Mapped[List["PackDocItem"]] = relationship(
        back_populates="doc",
        cascade="all, delete-orphan"
    )


class PackDocItem(Base):
    __tablename__ = "pack_doc_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    doc_id: Mapped[int] = mapped_column(ForeignKey("pack_docs.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    qty: Mapped[int]

    doc: Mapped["PackDoc"] = relationship(back_populates="items")
    product: Mapped["Product"] = relationship()


# ===== Закупка CN =====

class CnPurchase(Base):
    __tablename__ = "cn_purchases"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(40), unique=True, nullable=False)
    status: Mapped[CnPurchaseStatus] = mapped_column(
        Enum(
            CnPurchaseStatus,
            name="cn_purchase_status",
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
        ),
        default=CnPurchaseStatus.PURCHASED,
        nullable=False,
    )

    comment: Mapped[Optional[str]]
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    created_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))
    updated_at: Mapped[Optional[datetime]] = mapped_column(default=None, onupdate=datetime.utcnow)
    updated_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))

    sent_to_cargo_at = Column(DateTime, nullable=True)
    sent_to_msk_at   = Column(DateTime, nullable=True)
    archived_at      = Column(DateTime, nullable=True)

    items: Mapped[List["CnPurchaseItem"]] = relationship(
        back_populates="purchase", cascade="all, delete-orphan"
    )

    photos: Mapped[List["CnPurchasePhoto"]] = relationship(
        back_populates="purchase", cascade="all, delete-orphan", lazy="selectin"
    )

    msk_inbound: Mapped[Optional["MskInboundDoc"]] = relationship(
        back_populates="cn_purchase", uselist=False, cascade="all, delete-orphan"
    )


class CnPurchaseItem(Base):
    __tablename__ = "cn_purchase_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    cn_purchase_id: Mapped[int] = mapped_column(ForeignKey("cn_purchases.id", ondelete="CASCADE"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    qty: Mapped[int]
    unit_cost_rub: Mapped[Numeric] = mapped_column(Numeric(12, 2))
    comment: Mapped[Optional[str]]
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)

    purchase: Mapped["CnPurchase"] = relationship(back_populates="items")
    product: Mapped["Product"] = relationship()


class CnPurchasePhoto(Base):
    __tablename__ = "cn_purchase_photos"

    id: Mapped[int] = mapped_column(primary_key=True)
    cn_purchase_id: Mapped[int] = mapped_column(
        ForeignKey("cn_purchases.id", ondelete="CASCADE"),
        index=True, nullable=False
    )
    file_id: Mapped[str] = mapped_column(String(256), nullable=False)
    caption: Mapped[Optional[str]] = mapped_column(String(512))
    uploaded_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    uploaded_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))

    purchase: Mapped["CnPurchase"] = relationship(back_populates="photos")


# ===== Входящие МСК =====

class MskInboundDoc(Base):
    __tablename__ = "msk_inbound_docs"

    id: Mapped[int] = mapped_column(primary_key=True)
    cn_purchase_id: Mapped[int] = mapped_column(
        ForeignKey("cn_purchases.id", ondelete="CASCADE"),
        unique=True, nullable=False
    )
    status: Mapped[MskInboundStatus] = mapped_column(
        Enum(
            MskInboundStatus,
            name="msk_inbound_status",
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
        ),
        default=MskInboundStatus.PENDING,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    created_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))

    warehouse_id: Mapped[Optional[int]] = mapped_column(
        "target_warehouse_id", ForeignKey("warehouses.id"), nullable=True
    )

    to_our_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    received_at: Mapped[Optional[datetime]]
    received_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))
    comment: Mapped[Optional[str]]

    cn_purchase: Mapped["CnPurchase"] = relationship(back_populates="msk_inbound")
    items: Mapped[List["MskInboundItem"]] = relationship(
        back_populates="doc", cascade="all, delete-orphan"
    )

    warehouse: Mapped[Optional["Warehouse"]] = relationship(
        "Warehouse", foreign_keys=[warehouse_id], lazy="joined"
    )


class MskInboundItem(Base):
    __tablename__ = "msk_inbound_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    msk_inbound_id: Mapped[int] = mapped_column(ForeignKey("msk_inbound_docs.id", ondelete="CASCADE"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    qty: Mapped[int]
    unit_cost_rub: Mapped[Numeric] = mapped_column(Numeric(12, 2))
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)

    doc: Mapped["MskInboundDoc"] = relationship(back_populates="items")
    product: Mapped["Product"] = relationship()


# ===== Настройки бэкапов =====

class BackupFrequency(enum.Enum):
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"


class BackupSettings(Base):
    __tablename__ = "backup_settings"
    id = Column(Integer, primary_key=True, default=1)
    enabled = Column(Boolean, nullable=False, default=False)
    frequency = Column(Enum(BackupFrequency, name="backup_frequency_enum"), nullable=False, default=BackupFrequency.daily)
    time_hour = Column(Integer, nullable=False, default=3)
    time_minute = Column(Integer, nullable=False, default=15)
    retention_days = Column(Integer, nullable=False, default=30)
    gdrive_folder_id = Column(String(128))
    gdrive_sa_json = Column(JSONB)
    last_run_at = Column(TIMESTAMP)
    last_status = Column(String(255))

```

## Файл: database\__init__.py

```python

```

## Файл: handlers\admin.py

```python
# handlers/admin.py
import logging
from typing import Optional, List, Tuple

from aiogram import types, Dispatcher, Bot
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select, func, update, desc

from database.db import get_session
from database.models import (
    User, UserRole,
    Warehouse, Product,
    StockMovement, Supply, SupplyItem,
    AuditLog,
)

from handlers.common import send_content

# =========================
#          FSM
# =========================
class AdminState(StatesGroup):
    selecting_action = State()
    selecting_user = State()
    entering_message = State()

class ProductState(StatesGroup):
    entering_article = State()
    entering_name = State()
    confirming = State()

class WarehouseCreateState(StatesGroup):
    entering_name = State()

class WarehouseRenameState(StatesGroup):
    entering_name = State()

class ProductEditState(StatesGroup):
    selecting = State()
    renaming = State()


# =========================
#       KEYBOARDS
# =========================
def kb_admin_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏬 Склады", callback_data="admin_wh")],
        [InlineKeyboardButton(text="📦 Товары", callback_data="admin_prod")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton(text="🧾 Журнал действий", callback_data="admin_audit")],
        [InlineKeyboardButton(text="💾 Бэкапы", callback_data="admin:backup")],
        [InlineKeyboardButton(text="🧩 Настройки меню", callback_data="menuvis:roles")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")],
    ])

def kb_admin_users() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Список пользователей", callback_data="admin_list_users")],
        [InlineKeyboardButton(text="Сменить роль пользователя", callback_data="admin_change_role")],
        [InlineKeyboardButton(text="Удалить пользователя", callback_data="admin_delete_user")],
        [InlineKeyboardButton(text="Отправить сообщение", callback_data="admin_send_message")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
    ])

def kb_admin_wh_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Список", callback_data="admin_wh_list")],
        [InlineKeyboardButton(text="Добавить", callback_data="admin_wh_add")],
        [InlineKeyboardButton(text="Редактировать", callback_data="admin_wh_edit")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
    ])

def kb_wh_edit_pick(whs: List[Warehouse]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{w.name} ({'✅' if w.is_active else '🚫'})", callback_data=f"admin_wh_pick:{w.id}")]
        for w in whs
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_wh")])
    return kb

def kb_wh_actions(wh: Warehouse) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"admin_wh_rename:{wh.id}")],
        [InlineKeyboardButton(
            text=("🟢 Активировать" if not getattr(wh, "is_active", True) else "🔴 Деактивировать"),
            callback_data=f"admin_wh_toggle:{wh.id}"
        )],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"admin_wh_del:{wh.id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_wh_edit")],
    ])

def kb_admin_prod_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить", callback_data="admin_product_add")],
        [InlineKeyboardButton(text="Редактировать", callback_data="admin_product_edit")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
    ])

def kb_prod_pick(products: List[Product]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{p.name} (арт. {p.article}) {'✅' if p.is_active else '🚫'}", callback_data=f"adm_prod_pick:{p.id}")]
        for p in products
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_prod")])
    return kb

def kb_prod_actions(p: Product) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"adm_prod_rename:{p.id}")],
        [InlineKeyboardButton(text=("🟢 Активировать" if not p.is_active else "🔴 Деактивировать"),
                              callback_data=f"adm_prod_toggle:{p.id}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"adm_prod_del:{p.id}")],
        InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_prod"),
    ])

def kb_back(data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=data)]])

def kb_confirm(prefix: str, id_: int, back: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"{prefix}_confirm:{id_}")],
        [InlineKeyboardButton(text="⬅️ Отмена",     callback_data=back)],
    ])

# --- Пользователи: смена роли ---
def kb_pick_user_for_role(users: List[User]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{u.name or u.telegram_id} — {u.role.value}", callback_data=f"role_user:{u.telegram_id}")]
            for u in users
        ]
    )
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")])
    return kb

def kb_pick_role(telegram_id: int, current: UserRole) -> InlineKeyboardMarkup:
    def mark(r: UserRole) -> str:
        return "✅ " if r == current else ""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{mark(UserRole.admin)}Администратор", callback_data=f"role_set:{telegram_id}:admin")],
        [InlineKeyboardButton(text=f"{mark(UserRole.user)}Пользователь",   callback_data=f"role_set:{telegram_id}:user")],
        [InlineKeyboardButton(text=f"{mark(UserRole.manager)}Менеджер",     callback_data=f"role_set:{telegram_id}:manager")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_change_role")],
    ])


# =========================
#       ROOT
# =========================
async def on_admin(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен: только для администраторов.", show_alert=True); return
    await cb.answer()
    await send_content(cb, "Администрирование: выберите раздел", reply_markup=kb_admin_root())
    await state.set_state(AdminState.selecting_action)


# =========================
#       USERS
# =========================
async def admin_users_menu(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await send_content(cb, "Пользователи: выберите действие", reply_markup=kb_admin_users())

async def admin_list_users(cb: types.CallbackQuery, user: User):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        result = await session.execute(select(User).order_by(User.id))
        users = result.scalars().all()
    if not users:
        await send_content(cb, "Пользователи не найдены.", reply_markup=kb_admin_users()); return
    lines = []
    for u in users:
        lines.append(f"ID: {u.id} | TG: {u.telegram_id} | Имя: {u.name or '-'} | Роль: {u.role.value}")
    await send_content(cb, "Список пользователей:\n" + "\n".join(lines), reply_markup=kb_admin_users())

async def admin_delete_user(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        result = await session.execute(select(User).where(User.telegram_id != user.telegram_id).order_by(User.id))
        users = result.scalars().all()
    if not users:
        await send_content(cb, "Нет пользователей для удаления.", reply_markup=kb_admin_users()); return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{u.name or u.telegram_id} ({u.role.value})", callback_data=f"delete_user:{u.telegram_id}")]
        for u in users
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")])
    await send_content(cb, "Выберите пользователя для удаления:", reply_markup=kb)
    await state.set_state(AdminState.selecting_user)

async def admin_confirm_delete_user(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    try:
        _, user_id_str = cb.data.split(":"); tg_id = int(user_id_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        target_q = await session.execute(select(User).where(User.telegram_id == tg_id))
        target_user = target_q.scalar()
        if not target_user:
            await cb.answer("Пользователь не найден.", show_alert=True); return
        await session.delete(target_user); await session.commit()
    await cb.answer("Пользователь удален.")
    await send_content(cb, "Пользователь успешно удален.", reply_markup=kb_admin_users())
    await state.clear()

async def admin_send_message(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        result = await session.execute(select(User).where(User.telegram_id != user.telegram_id))
        users = result.scalars().all()
    if not users:
        await send_content(cb, "Нет пользователей для отправки сообщения.", reply_markup=kb_admin_users()); return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{u.name or u.telegram_id} ({u.role.value})", callback_data=f"send_msg:{u.telegram_id}")]
        for u in users
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")])
    await send_content(cb, "Выберите получателя:", reply_markup=kb)
    await state.set_state(AdminState.selecting_user)

async def admin_enter_message(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, user_id_str = cb.data.split(":"); tg_id = int(user_id_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    await state.update_data(target_user_tg=tg_id)
    await send_content(cb, "Введите текст сообщения:")
    await state.set_state(AdminState.entering_message)

async def admin_send_message_text(message: types.Message, user: User, state: FSMContext, bot: Bot):
    if user.role != UserRole.admin:
        await message.answer("Доступ запрещен."); return
    data = await state.get_data(); target_tg = data.get("target_user_tg")
    if not target_tg:
        await message.answer("Ошибка: пользователь не выбран."); return
    try:
        await bot.send_message(target_tg, f"Сообщение от администратора:\n{message.text}")
        await message.answer("Сообщение успешно отправлено.")
    except Exception as e:
        logging.exception("Ошибка отправки сообщения: %s", e)
        await message.answer("Не удалось отправить сообщение.")
    await state.clear()

# --- Управление ролями ---
async def admin_change_role(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        res = await session.execute(select(User).order_by(User.id))
        users = res.scalars().all()
    if not users:
        await send_content(cb, "Пользователей нет.", reply_markup=kb_admin_users()); return
    await send_content(cb, "Выберите пользователя для смены роли:", reply_markup=kb_pick_user_for_role(users))

async def admin_pick_user_for_role(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, tg_id_str = cb.data.split(":")
        target_tg_id = int(tg_id_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return

    if target_tg_id == user.telegram_id:
        await cb.answer("Нельзя менять роль самому себе.", show_alert=True); return

    async with get_session() as session:
        q = await session.execute(select(User).where(User.telegram_id == target_tg_id))
        target = q.scalar()
    if not target:
        await cb.answer("Пользователь не найден.", show_alert=True); return

    await send_content(cb, f"Текущая роль: {target.role.value}. Выберите новую:",
                       reply_markup=kb_pick_role(target.telegram_id, target.role))

async def admin_apply_role(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, tg_id_str, role_str = cb.data.split(":")
        target_tg_id = int(tg_id_str)
        new_role = UserRole(role_str)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return

    if target_tg_id == user.telegram_id:
        await cb.answer("Нельзя менять роль самому себе.", show_alert=True); return

    async with get_session() as session:
        q = await session.execute(select(User).where(User.telegram_id == target_tg_id))
        target = q.scalar()
        if not target:
            await cb.answer("Пользователь не найден.", show_alert=True); return
        old_role = target.role
        if old_role == new_role:
            await send_content(cb, f"Роль не изменилась: по-прежнему {new_role.value}.", reply_markup=kb_admin_users()); return
        target.role = new_role
        await session.commit()

    await send_content(cb, f"Готово. Роль пользователя {target_tg_id} изменена: {old_role.value} → {new_role.value}.",
                       reply_markup=kb_admin_users())


# =========================
#       WAREHOUSES
# =========================
async def admin_wh_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await send_content(cb, "Склады: выберите действие", reply_markup=kb_admin_wh_root())

async def admin_wh_list(cb: types.CallbackQuery, user: User):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        res = await session.execute(select(Warehouse).order_by(Warehouse.name))
        whs = res.scalars().all()
    if not whs:
        await send_content(cb, "Складов нет.", reply_markup=kb_admin_wh_root()); return
    text = "Склады:\n" + "\n".join(f"- {w.name} (id={w.id}, active={w.is_active})" for w in whs)
    await send_content(cb, text, reply_markup=kb_admin_wh_root())

async def admin_wh_add(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await state.clear()
    await state.set_state(WarehouseCreateState.entering_name)
    await send_content(cb, "Введите название склада:", reply_markup=kb_back("admin_wh"))

async def admin_wh_add_apply(message: types.Message, user: User, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Название пустое. Введите ещё раз:"); return
    async with get_session() as session:
        exists = (await session.execute(select(Warehouse).where(Warehouse.name == name))).scalar()
        if exists:
            await message.answer("Склад с таким названием уже существует. Введите другое имя:"); return
        w = Warehouse(name=name, is_active=True)
        session.add(w)
        await session.commit()
    await state.clear()
    await message.answer("✅ Склад создан.", reply_markup=kb_back("admin_wh"))

async def admin_wh_edit(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        res = await session.execute(select(Warehouse).order_by(Warehouse.name))
        whs = res.scalars().all()
    if not whs:
        await send_content(cb, "Складов нет.", reply_markup=kb_admin_wh_root()); return
    await send_content(cb, "Выберите склад для редактирования:", reply_markup=kb_wh_edit_pick(whs))

async def admin_wh_pick(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); wh_id = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Warehouse).where(Warehouse.id == wh_id))
        wh = q.scalar()
    if not wh:
        await cb.answer("Склад не найден.", show_alert=True); return
    await state.update_data(wh_id=wh.id)
    await send_content(cb, f"Склад: {wh.name} (active={wh.is_active}). Выберите действие:", reply_markup=kb_wh_actions(wh))

async def admin_wh_rename_start(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); wh_id = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    await state.clear()
    await state.set_state(WarehouseRenameState.entering_name)
    await state.update_data(wh_id=wh_id)
    await send_content(cb, "Введите новое название склада:", reply_markup=kb_back("admin_wh_edit"))

async def admin_wh_rename_apply(message: types.Message, user: User, state: FSMContext):
    data = await state.get_data()
    wh_id = data.get("wh_id")
    name = (message.text or "").strip()
    if not wh_id:
        await message.answer("Не выбран склад для переименования."); return
    if not name:
        await message.answer("Название пустое. Введите ещё раз:"); return
    async with get_session() as session:
        exists = (await session.execute(select(Warehouse).where(Warehouse.name == name, Warehouse.id != wh_id))).scalar()
        if exists:
            await message.answer("Склад с таким названием уже существует. Введите другое имя:"); return
        await session.execute(update(Warehouse).where(Warehouse.id == wh_id).values(name=name))
        await session.commit()
    await state.clear()
    await message.answer("✅ Название склада обновлено.", reply_markup=kb_back("admin_wh_edit"))

async def admin_wh_toggle(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); wh_id = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Warehouse).where(Warehouse.id == wh_id))
        wh = q.scalar()
        if not wh:
            await cb.answer("Склад не найден.", show_alert=True); return
        wh.is_active = not wh.is_active
        await session.commit()
    await send_content(cb, f"✅ Готово. Активность склада теперь: {'True' if wh.is_active else 'False'}",
                       reply_markup=kb_admin_wh_root())

async def admin_wh_del(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); wh_id = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return

    async with get_session() as session:
        sm_count = (await session.execute(
            select(func.count()).select_from(StockMovement).where(StockMovement.warehouse_id == wh_id)
        )).scalar_one()
        sup_count = (await session.execute(
            select(func.count()).select_from(Supply).where(Supply.warehouse_id == wh_id)
        )).scalar_one()

    if sm_count > 0 or sup_count > 0:
        await send_content(cb, "Нельзя удалить склад: есть связанные движения/поставки.\nВы можете его деактивировать.",
                           reply_markup=kb_back("admin_wh_edit"))
        return

    await send_content(cb, "Удалить этот склад безвозвратно?",
                       reply_markup=kb_confirm("admin_wh_del", wh_id, "admin_wh_edit"))

async def admin_wh_del_confirm(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); wh_id = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Warehouse).where(Warehouse.id == wh_id))
        wh = q.scalar()
        if not wh:
            await cb.answer("Склад уже отсутствует."); return
        await session.delete(wh)
        await session.commit()
    await send_content(cb, "✅ Склад удалён.", reply_markup=kb_admin_wh_root())


# =========================
#        PRODUCTS
# =========================
async def admin_prod_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await state.clear()   # сбрасываем FSM
    await send_content(cb, "Товары: выберите действие", reply_markup=kb_admin_prod_root())


async def admin_product_add(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    await state.clear()
    await state.set_state(ProductState.entering_article)
    await send_content(cb, "Введите артикул (уникальный):", reply_markup=kb_back("admin_prod"))

async def admin_product_enter_article(message: types.Message, user: User, state: FSMContext):
    article = (message.text or "").strip()
    if not article:
        await message.answer("Артикул пустой. Введите ещё раз:"); return
    async with get_session() as session:
        exists_q = await session.execute(select(Product).where(Product.article == article))
        exists = exists_q.scalar()
    if exists:
        await message.answer("Такой артикул уже существует. Введите другой:"); return
    await state.update_data(article=article)
    await state.set_state(ProductState.entering_name)
    await message.answer("Введите название товара:")

async def admin_product_enter_name(message: types.Message, state: FSMContext, user: User | None = None):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Название пустое. Введите ещё раз:"); return
    data = await state.get_data(); article = data["article"]
    text = ("Подтвердите создание товара:\n"
            f"Артикул: {article}\n"
            f"Название: {name}\n"
            f"Активный: Да\n")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Создать", callback_data="adm_prod_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_prod")],
    ])
    await state.update_data(name=name)
    await state.set_state(ProductState.confirming)
    await message.answer(text, reply_markup=kb)

async def admin_product_confirm(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    async with get_session() as session:
        p = Product(article=data["article"], name=data["name"], is_active=True)
        session.add(p); await session.commit()
    await state.clear()
    await send_content(cb, "✅ Товар создан.", reply_markup=kb_admin_prod_root())

async def admin_product_edit(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    async with get_session() as session:
        total = (await session.execute(select(func.count()).select_from(Product))).scalar_one()
        res = await session.execute(select(Product).order_by(Product.name).limit(30))
        products = res.scalars().all()
    if not products:
        await send_content(cb, "Товаров нет.", reply_markup=kb_admin_prod_root()); return
    await send_content(cb, f"Выберите товар (первые 30, всего {total}):", reply_markup=kb_prod_pick(products))
    await state.set_state(ProductEditState.selecting)

async def admin_product_pick(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); pid = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Product).where(Product.id == pid))
        p = q.scalar()
    if not p:
        await cb.answer("Товар не найден.", show_alert=True); return
    await state.update_data(prod_id=p.id)
    await send_content(cb, f"Товар: {p.name} (арт. {p.article}) active={p.is_active}. Выберите действие:",
                       reply_markup=kb_prod_actions(p))

async def admin_product_rename_start(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); pid = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    await state.update_data(prod_id=pid)
    await send_content(cb, "Введите новое название товара:", reply_markup=kb_back("admin_product_edit"))
    await state.set_state(ProductEditState.renaming)

async def admin_product_rename_apply(message: types.Message, user: User, state: FSMContext):
    data = await state.get_data()
    name = (message.text or "").strip()
    if not name:
        await message.answer("Название пустое. Введите ещё раз:"); return
    pid = data.get("prod_id")
    async with get_session() as session:
        await session.execute(update(Product).where(Product.id == pid).values(name=name))
        await session.commit()
    await state.clear()
    await message.answer("✅ Название товара обновлено.", reply_markup=kb_back("admin_product_edit"))

async def admin_product_toggle(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); pid = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Product).where(Product.id == pid))
        p = q.scalar()
        if not p:
            await cb.answer("Товар не найден.", show_alert=True); return
        p.is_active = not p.is_active
        await session.commit()
    await send_content(cb, f"✅ Готово. Активность товара теперь: {'True' if p.is_active else 'False'}",
                       reply_markup=kb_admin_prod_root())

async def admin_prod_del(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); pid = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return

    async with get_session() as session:
        mv_count = (await session.execute(
            select(func.count()).select_from(StockMovement).where(StockMovement.product_id == pid)
        )).scalar_one()
        si_count = (await session.execute(
            select(func.count()).select_from(SupplyItem).where(SupplyItem.product_id == pid)
        )).scalar_one()

    if mv_count > 0 or si_count > 0:
        await send_content(cb, "Нельзя удалить товар: есть связанные движения/позиции поставок.\nВы можете его деактивировать.",
                           reply_markup=kb_back("admin_product_edit"))
        return

    await send_content(cb, "Удалить этот товар безвозвратно?", reply_markup=kb_confirm("adm_prod_del", pid, "admin_product_edit"))

async def admin_prod_del_confirm(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, sid = cb.data.split(":"); pid = int(sid)
    except Exception:
        await cb.answer("Некорректные данные.", show_alert=True); return
    async with get_session() as session:
        q = await session.execute(select(Product).where(Product.id == pid))
        p = q.scalar()
        if not p:
            await cb.answer("Товар уже отсутствует."); return
        await session.delete(p)
        await session.commit()
    await send_content(cb, "✅ Товар удалён.", reply_markup=kb_admin_prod_root())


# =========================
#        AUDIT LOG
# =========================
AUDIT_PAGE = 10

def _format_audit_row(row: Tuple[AuditLog, Optional[User]]) -> str:
    log, usr = row
    who = f"{usr.name} (id={usr.id})" if usr else "system"
    parts = [
        f"[{log.created_at}]",
        f"user: {who}",
        f"action: {log.action.value}",
        f"table: {log.table_name}",
        f"pk: {log.record_pk}",
    ]
    if log.diff:
        parts.append(f"diff: {str(log.diff)[:200]}")
    elif log.new_data and not log.old_data:
        parts.append(f"new: {str(log.new_data)[:200]}")
    elif log.old_data and not log.new_data:
        parts.append(f"old: {str(log.old_data)[:200]}")
    return " | ".join(parts)

async def admin_audit_root(cb: types.CallbackQuery, user: User, state: FSMContext, page: int = 1):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()

    async with get_session() as session:
        total = (await session.execute(select(func.count()).select_from(AuditLog))).scalar_one()
        res = await session.execute(
            select(AuditLog, User)
            .join(User, User.id == AuditLog.user_id, isouter=True)
            .order_by(desc(AuditLog.id))
            .offset((page - 1) * AUDIT_PAGE)
            .limit(AUDIT_PAGE)
        )
        rows = res.all()

    if not rows:
        await send_content(cb, "Журнал пуст.", reply_markup=kb_admin_root())
        return

    lines = [_format_audit_row(r) for r in rows]
    text = "Журнал действий (последние записи):\n\n" + "\n".join(lines)

    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="◀ Предыдущая", callback_data=f"admin_audit_page:{page-1}"))
    if page * AUDIT_PAGE < total:
        buttons.append(InlineKeyboardButton(text="Следующая ▶", callback_data=f"admin_audit_page:{page+1}"))

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    if buttons:
        kb.inline_keyboard.append(buttons)
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")])

    await send_content(cb, text, reply_markup=kb)

async def admin_audit_page(cb: types.CallbackQuery, user: User, state: FSMContext):
    if user.role != UserRole.admin:
        await cb.answer("Доступ запрещен.", show_alert=True); return
    await cb.answer()
    try:
        _, p = cb.data.split(":")
        page = int(p)
    except Exception:
        page = 1
    await admin_audit_root(cb, user, state, page=page)


# =========================
#     REGISTER ROUTES
# =========================
def register_admin_handlers(dp: Dispatcher):
    dp.callback_query.register(on_admin,                   lambda c: c.data == "admin")

    # Пользователи
    dp.callback_query.register(admin_users_menu,           lambda c: c.data == "admin_users")
    dp.callback_query.register(admin_list_users,           lambda c: c.data == "admin_list_users")
    dp.callback_query.register(admin_delete_user,          lambda c: c.data == "admin_delete_user")
    dp.callback_query.register(admin_confirm_delete_user,  lambda c: c.data.startswith("delete_user:"))
    dp.callback_query.register(admin_send_message,         lambda c: c.data == "admin_send_message")
    dp.callback_query.register(admin_enter_message,        lambda c: c.data.startswith("send_msg:"))
    dp.message.register(admin_send_message_text,           AdminState.entering_message)

    # Управление ролями
    dp.callback_query.register(admin_change_role,          lambda c: c.data == "admin_change_role")
    dp.callback_query.register(admin_pick_user_for_role,   lambda c: c.data.startswith("role_user:"))
    dp.callback_query.register(admin_apply_role,           lambda c: c.data.startswith("role_set:"))

    # Склады
    dp.callback_query.register(admin_wh_root,              lambda c: c.data == "admin_wh")
    dp.callback_query.register(admin_wh_list,              lambda c: c.data == "admin_wh_list")
    dp.callback_query.register(admin_wh_add,               lambda c: c.data == "admin_wh_add")
    dp.message.register(admin_wh_add_apply,                WarehouseCreateState.entering_name)
    dp.callback_query.register(admin_wh_edit,              lambda c: c.data == "admin_wh_edit")
    dp.callback_query.register(admin_wh_pick,              lambda c: c.data.startswith("admin_wh_pick:"))
    dp.callback_query.register(admin_wh_rename_start,      lambda c: c.data.startswith("admin_wh_rename:"))
    dp.message.register(admin_wh_rename_apply,             WarehouseRenameState.entering_name)
    dp.callback_query.register(admin_wh_toggle,            lambda c: c.data.startswith("admin_wh_toggle:"))
    dp.callback_query.register(admin_wh_del,               lambda c: c.data.startswith("admin_wh_del:"))
    dp.callback_query.register(admin_wh_del_confirm,       lambda c: c.data.startswith("admin_wh_del_confirm:"))

    # Товары
    dp.callback_query.register(admin_prod_root,            lambda c: c.data == "admin_prod")
    dp.callback_query.register(admin_product_add,          lambda c: c.data == "admin_product_add")
    dp.message.register(admin_product_enter_article,       ProductState.entering_article)
    dp.message.register(admin_product_enter_name,          ProductState.entering_name)
    dp.callback_query.register(admin_product_confirm,      ProductState.confirming)

    dp.callback_query.register(admin_product_edit,         lambda c: c.data == "admin_product_edit")
    dp.callback_query.register(admin_product_pick,         lambda c: c.data.startswith("adm_prod_pick:"))
    dp.callback_query.register(admin_product_rename_start, lambda c: c.data.startswith("adm_prod_rename:"))
    dp.message.register(admin_product_rename_apply,        ProductEditState.renaming)
    dp.callback_query.register(admin_product_toggle,       lambda c: c.data.startswith("adm_prod_toggle:"))
    dp.callback_query.register(admin_prod_del,             lambda c: c.data.startswith("adm_prod_del:"))
    dp.callback_query.register(admin_prod_del_confirm,     lambda c: c.data.startswith("adm_prod_del_confirm:"))

    # Журнал
    dp.callback_query.register(admin_audit_root,           lambda c: c.data == "admin_audit")
    dp.callback_query.register(admin_audit_page,           lambda c: c.data.startswith("admin_audit_page:"))

```

## Файл: handlers\admin_backup.py

```python
# handlers/admin_backup.py
from __future__ import annotations

import asyncio
import html
import json
import os
import shutil
import tempfile
import time
from typing import Union, Tuple

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    CallbackQuery,
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ContentType,
    FSInputFile,
)
from sqlalchemy import select, text
from sqlalchemy.engine.url import make_url

import httpx  # pip install httpx

from config import (
    ADMIN_TELEGRAM_ID,
    TIMEZONE,
    DB_URL,
)

from database.db import get_session, init_db, reset_db_engine, ping_db
from database.models import BackupSettings, BackupFrequency
from scheduler.backup_scheduler import reschedule_backup
from utils.backup import run_backup, build_restore_cmd

router = Router()

# --------- Константы путей и сервиса ---------
GOOGLE_TOKEN_PATH = os.environ.get("GOOGLE_OAUTH_TOKEN_PATH", "/etc/botwb/google/token.json")
GOOGLE_CLIENT_PATH = os.environ.get("GOOGLE_OAUTH_CLIENT_PATH", "/etc/botwb/google/client_secret_tv.json")
SERVICE_NAME = "warehouse-botwb.service"

# --------- OAuth Device Flow эндпоинты/скоуп ---------
OAUTH_SCOPE = "https://www.googleapis.com/auth/drive.file"
DEVICE_CODE_URL = "https://oauth2.googleapis.com/device/code"
TOKEN_URL = "https://oauth2.googleapis.com/token"


# ===== FSM states =====
class BackupState(StatesGroup):
    waiting_folder_id = State()
    waiting_time = State()
    waiting_retention = State()
    waiting_restore_file = State()
    waiting_restore_confirm = State()
    # OAuth/Token
    waiting_oauth_poll = State()      # ожидание подтверждения на сайте Google
    waiting_token_upload = State()    # ожидание загрузки token.json
    # Wipe DB
    waiting_wipe_phrase = State()
    waiting_wipe_dbname = State()


# ===== helpers =====
async def _load_settings() -> BackupSettings | None:
    async with get_session() as s:
        return (await s.execute(select(BackupSettings).where(BackupSettings.id == 1))).scalar_one_or_none()


async def _ensure_settings_exists(msg_or_cb: Union[Message, CallbackQuery]) -> BackupSettings | None:
    """
    Гарантируем, что backup_settings (id=1) существует.
    Если таблицы нет (после wipe) — создаём схему через init_db() и вставляем запись.
    """
    st = None
    try:
        st = await _load_settings()
    except Exception:
        st = None

    if not st:
        try:
            await init_db()
            async with get_session() as s:
                st = await s.get(BackupSettings, 1)
                if not st:
                    st = BackupSettings(
                        id=1,
                        enabled=False,
                        frequency=BackupFrequency.daily,
                        time_hour=3,
                        time_minute=30,
                        retention_days=30,
                    )
                    s.add(st)
                    await s.commit()
        except Exception:
            st = None

    if not st:
        out = msg_or_cb.message if isinstance(msg_or_cb, CallbackQuery) else msg_or_cb
        await out.answer("⚠️ БД ещё не готова. Попробуйте позже или используйте Emergency Restore.")
        return None
    return st


def _kb_main(st: BackupSettings) -> InlineKeyboardMarkup:
    onoff = "🟢 Включено" if st.enabled else "🔴 Выключено"
    freq_map = {"daily": "Ежедневно", "weekly": "Еженедельно", "monthly": "Ежемесячно"}
    freq_title = freq_map.get(st.frequency.value, st.frequency.value)

    rows = [
        [InlineKeyboardButton(text=f"{onoff} — переключить", callback_data="bk:toggle")],
        [
            InlineKeyboardButton(
                text=f"⏰ Расписание: {freq_title} {st.time_hour:02d}:{st.time_minute:02d}",
                callback_data="bk:schedule",
            )
        ],
        [InlineKeyboardButton(text=f"🧹 Retention: {st.retention_days} дн.", callback_data="bk:retention")],
        [InlineKeyboardButton(text=f"📁 Folder ID: {st.gdrive_folder_id or '—'}", callback_data="bk:folder")],
        [InlineKeyboardButton(text="🔗 Подключить Google (OAuth)", callback_data="bk:oauth")],
        [InlineKeyboardButton(text="⬆️ Загрузить token.json", callback_data="bk:token_upload")],
        [InlineKeyboardButton(text="🧪 Сделать бэкап сейчас", callback_data="bk:run")],
        [InlineKeyboardButton(text="♻️ Восстановить БД", callback_data="bk:restore")],
        [InlineKeyboardButton(text="🧨 Очистить базу", callback_data="bk:wipe")],
        [InlineKeyboardButton(text="🆘 Emergency Restore", callback_data="bk:restore_emergency")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:root")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render(target: Union[CallbackQuery, Message], st: BackupSettings) -> None:
    text = (
        "<b>Бэкапы БД → Google Drive</b>\n\n"
        f"Статус: {'🟢 Включено' if st.enabled else '🔴 Выключено'}\n"
        f"Расписание: <code>{st.frequency.value}</code> @ {st.time_hour:02d}:{st.time_minute:02d} ({TIMEZONE})\n"
        f"Retention: {st.retention_days} дней\n"
        f"Folder ID: <code>{st.gdrive_folder_id or '—'}</code>\n"
        "Авторизация: <b>OAuth</b> (client_secret.json + token.json, пути берутся из .env)\n"
        f"Последний запуск: {st.last_run_at.strftime('%Y-%m-%d %H:%M:%S') if st.last_run_at else '—'}\n"
        f"Статус последнего: {st.last_status or '—'}"
    )
    kb = _kb_main(st)
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")


async def _auto_back_to_menu(target: Union[CallbackQuery, Message]) -> None:
    """Через 2 секунды вернёмся на экран бэкапов."""
    await asyncio.sleep(2)
    st = await _load_settings()
    if not st:
        return
    await _render(target, st)


async def _restart_service() -> Tuple[bool, str]:
    try:
        proc = await asyncio.create_subprocess_shell(
            f"sudo systemctl restart {SERVICE_NAME}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await proc.communicate()
        ok = proc.returncode == 0
        msg = (out or b"").decode(errors="ignore")
        return ok, msg
    except Exception as e:
        return False, repr(e)


async def _save_token_json(raw_json: str) -> None:
    os.makedirs(os.path.dirname(GOOGLE_TOKEN_PATH), exist_ok=True)
    with open(GOOGLE_TOKEN_PATH, "w", encoding="utf-8") as f:
        f.write(raw_json)
    # права/владелец — best-effort
    try:
        uid = __import__("pwd").getpwnam("malinabotwh").pw_uid
        gid = __import__("grp").getgrnam("malinabotwh").gr_gid
        os.chown(GOOGLE_TOKEN_PATH, uid, gid)
    except Exception:
        pass
    os.chmod(GOOGLE_TOKEN_PATH, 0o600)


def _load_client_id_secret() -> tuple[str, str]:
    with open(GOOGLE_CLIENT_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    data = cfg.get("installed") or cfg  # client_secret.json обычно под ключом "installed"
    return data["client_id"], data["client_secret"]


# ===== entry points =====
@router.message(Command("backup"))
async def backup_cmd(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.clear()
    st = await _ensure_settings_exists(message)
    if not st:
        return
    await _render(message, st)


@router.callback_query(F.data == "admin:backup")
async def open_backup(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.clear()
    st = await _ensure_settings_exists(cb)
    if not st:
        await cb.answer()
        return
    await _render(cb, st)
    await cb.answer()


# ===== toggle on/off =====
@router.callback_query(F.data == "bk:toggle")
async def bk_toggle(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    st = await _ensure_settings_exists(cb)
    if not st:
        await cb.answer()
        return

    async with get_session() as s:
        st.enabled = not st.enabled
        s.add(st)
        await s.commit()

    await reschedule_backup(cb.bot.scheduler, TIMEZONE, cb.bot.db_url)
    st = await _load_settings()
    await _render(cb, st)
    await cb.answer("Сохранено.")


# ===== schedule (частота + время) =====
def _kb_schedule_time(st: BackupSettings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Ежедневно", callback_data="bk:f:daily"),
                InlineKeyboardButton(text="Еженедельно", callback_data="bk:f:weekly"),
                InlineKeyboardButton(text="Ежемесячно", callback_data="bk:f:monthly"),
            ],
            [InlineKeyboardButton(text=f"🕒 Время: {st.time_hour:02d}:{st.time_minute:02d}", callback_data="bk:time")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:backup")],
        ]
    )


@router.callback_query(F.data == "bk:schedule")
async def bk_schedule(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    st = await _ensure_settings_exists(cb)
    if not st:
        await cb.answer()
        return
    await cb.message.edit_text("Настройка расписания:", reply_markup=_kb_schedule_time(st))
    await cb.answer()


@router.callback_query(F.data.startswith("bk:f:"))
async def bk_set_freq(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    st = await _ensure_settings_exists(cb)
    if not st:
        await cb.answer()
        return

    freq = cb.data.split(":")[-1]
    if freq not in ("daily", "weekly", "monthly"):
        await cb.answer("Неверное значение частоты.")
        return

    async with get_session() as s:
        st.frequency = {
            "daily": BackupFrequency.daily,
            "weekly": BackupFrequency.weekly,
            "monthly": BackupFrequency.monthly,
        }[freq]
        s.add(st)
        await s.commit()

    await reschedule_backup(cb.bot.scheduler, TIMEZONE, cb.bot.db_url)
    st = await _load_settings()
    await cb.message.edit_text("Настройка расписания:", reply_markup=_kb_schedule_time(st))
    await cb.answer("Частота сохранена.")


@router.callback_query(F.data == "bk:time")
async def bk_time(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.set_state(BackupState.waiting_time)
    await cb.message.edit_text(
        "Введите время в формате <b>HH:MM</b> (24ч), например <code>03:15</code>.",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(BackupState.waiting_time)
async def bk_time_set(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    t = (msg.text or "").strip()
    try:
        hh_str, mm_str = t.split(":")
        hh = int(hh_str)
        mm = int(mm_str)
        assert 0 <= hh <= 23 and 0 <= mm <= 59
    except Exception:
        await msg.answer("Неверный формат. Пример: 03:15")
        return

    st = await _ensure_settings_exists(msg)
    if not st:
        return

    async with get_session() as s:
        st.time_hour = hh
        st.time_minute = mm
        s.add(st)
        await s.commit()

    await reschedule_backup(msg.bot.scheduler, TIMEZONE, msg.bot.db_url)
    await state.clear()

    st = await _load_settings()
    await msg.answer("Время сохранено.")
    await _render(msg, st)


# ===== retention =====
@router.callback_query(F.data == "bk:retention")
async def bk_retention(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.set_state(BackupState.waiting_retention)
    await cb.message.edit_text(
        "Сколько дней хранить бэкапы на Google Drive? Введите число, например <code>30</code>.",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(BackupState.waiting_retention)
async def bk_retention_set(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    try:
        days = int((msg.text or "").strip())
        assert days >= 0
    except Exception:
        await msg.answer("Введите целое число ≥ 0.")
        return

    st = await _ensure_settings_exists(msg)
    if not st:
        return

    async with get_session() as s:
        st.retention_days = days
        s.add(st)
        await s.commit()

    await state.clear()
    st = await _load_settings()
    await msg.answer("Retention сохранён.")
    await _render(msg, st)


# ===== Folder ID =====
@router.callback_query(F.data == "bk:folder")
async def bk_folder(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.set_state(BackupState.waiting_folder_id)
    await cb.message.edit_text(
        "Пришлите <b>Folder ID</b> папки Google Drive, куда складывать бэкапы.\n"
        "Пример: <code>1abcDEFghij...XYZ</code>",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(BackupState.waiting_folder_id)
async def bk_folder_set(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    folder_id = (msg.text or "").strip()
    if not folder_id:
        await msg.answer("Folder ID не должен быть пустым.")
        return

    st = await _ensure_settings_exists(msg)
    if not st:
        return

    async with get_session() as s:
        st.gdrive_folder_id = folder_id
        s.add(st)
        await s.commit()

    await state.clear()
    st = await _load_settings()
    await msg.answer("Folder ID сохранён.")
    await _render(msg, st)


# ===== OAuth: Device Flow =====
@router.callback_query(F.data == "bk:oauth")
async def bk_oauth(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return

    # 1) читаем client_id/secret
    try:
        client_id, client_secret = _load_client_id_secret()
    except Exception as e:
        await cb.message.edit_text(
            f"Не найден или неверный <code>{html.escape(GOOGLE_CLIENT_PATH)}</code>:\n"
            f"<pre>{html.escape(repr(e))}</pre>",
            parse_mode="HTML",
        )
        await cb.answer()
        await _auto_back_to_menu(cb)
        return

    # 2) запрашиваем device_code
    try:
        async with httpx.AsyncClient(timeout=20) as cli:
            r = await cli.post(DEVICE_CODE_URL, data={"client_id": client_id, "scope": OAUTH_SCOPE})
            r.raise_for_status()
            dev = r.json()
    except Exception as e:
        await cb.message.edit_text(f"Не удалось запросить device code:\n<pre>{html.escape(repr(e))}</pre>", parse_mode="HTML")
        await cb.answer()
        await _auto_back_to_menu(cb)
        return

    await state.update_data(
        device_code=dev["device_code"],
        interval=int(dev.get("interval", 5)),
        client_id=client_id,
        client_secret=client_secret,
    )

    text = (
        "🔗 <b>Подключение Google OAuth</b>\n\n"
        "1) Откройте ссылку подтверждения:\n"
        f"<code>{dev['verification_url']}</code>\n"
        "2) Вставьте этот код:\n"
        f"<b><code>{dev['user_code']}</code></b>\n\n"
        "После подтверждения подождите — бот сам заберёт токен и перезапустит сервис."
    )
    await cb.message.edit_text(text, parse_mode="HTML")
    await cb.answer()

    # 3) поллим токен
    await state.set_state(BackupState.waiting_oauth_poll)
    deadline = time.monotonic() + 600  # до 10 минут
    while time.monotonic() < deadline:
        await asyncio.sleep(int(dev.get("interval", 5)))
        try:
            async with httpx.AsyncClient(timeout=20) as cli:
                tr = await cli.post(
                    TOKEN_URL,
                    data={
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "device_code": dev["device_code"],
                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    },
                )
            if tr.status_code == 200:
                tok = tr.json()
                token_json = json.dumps(
                    {
                        "token": tok.get("access_token"),
                        "refresh_token": tok.get("refresh_token"),
                        "token_uri": TOKEN_URL,
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "scopes": [OAUTH_SCOPE],
                        "universe_domain": "googleapis.com",
                    },
                    ensure_ascii=False,
                )
                await _save_token_json(token_json)
                ok, log = await _restart_service()
                msg = "✅ Токен получен и сохранён. Сервис перезапущен." if ok else \
                    f"✅ Токен получен, но рестарт не удался:\n<pre>{html.escape(log)}</pre>"
                await cb.message.edit_text(msg, parse_mode="HTML")
                await state.clear()
                await _auto_back_to_menu(cb)
                return
            else:
                # ошибки ожидания
                try:
                    err = tr.json().get("error")
                except Exception:
                    err = None
                if err in ("authorization_pending", "slow_down"):
                    continue
                if err in ("access_denied", "expired_token"):
                    await cb.message.edit_text(f"❌ Авторизация прервана: {err}")
                    await state.clear()
                    await _auto_back_to_menu(cb)
                    return
                await cb.message.edit_text(f"❌ Ошибка обмена токена:\n<pre>{html.escape(tr.text)}</pre>", parse_mode="HTML")
                await state.clear()
                await _auto_back_to_menu(cb)
                return
        except Exception:
            # подождём и попробуем снова
            continue

    await cb.message.edit_text("⏳ Время ожидания истекло. Попробуйте ещё раз.")
    await state.clear()
    await _auto_back_to_menu(cb)


# ===== Загрузка token.json файлом =====
@router.callback_query(F.data == "bk:token_upload")
async def bk_token_upload(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await state.set_state(BackupState.waiting_token_upload)
    await cb.message.edit_text("Пришлите <b>token.json</b> документом. Я сохраню его и перезапущу сервис.", parse_mode="HTML")
    await cb.answer()


@router.message(BackupState.waiting_token_upload, F.content_type == ContentType.DOCUMENT)
async def bk_token_file(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    if not (msg.document and msg.document.file_name and msg.document.file_name.lower().endswith(".json")):
        await msg.answer("Это не .json. Пришлите файл token.json.")
        return

    tg_file = await msg.bot.get_file(msg.document.file_id)
    content = await msg.bot.download_file(tg_file.file_path)
    raw = content.read().decode("utf-8", errors="ignore")

    # минимальная проверка структуры
    try:
        data = json.loads(raw)
        assert "client_id" in data and "client_secret" in data
        assert "refresh_token" in data or "token" in data
    except Exception:
        await msg.answer("Похоже, это не валидный token.json от Google OAuth.")
        return

    await _save_token_json(raw)
    ok, log = await _restart_service()
    await state.clear()
    if ok:
        await msg.answer("✅ Токен загружен и сохранён. Сервис перезапущен.")
    else:
        safe = html.escape(log)
        await msg.answer(f"✅ Токен сохранён, но рестарт не удался:\n<pre>{safe}</pre>", parse_mode="HTML")
    await _auto_back_to_menu(msg)


# ===== Run backup now =====
@router.callback_query(F.data == "bk:run")
async def bk_run(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    await cb.message.edit_text("Делаю бэкап… это может занять до нескольких минут.")
    ok, msg = await run_backup(cb.bot.db_url)
    text = f"✅ {msg}" if ok else f"❌ {msg}"
    await cb.message.edit_text(text, parse_mode="HTML")
    await _auto_back_to_menu(cb)


# ===== Restore =====
ALLOWED_EXT = {".backup", ".backup.gz", ".dump", ".sql", ".sql.gz"}
MAX_BACKUP_SIZE_MB = 2048


async def _restore_open_common(target: Union[CallbackQuery, Message], state: FSMContext):
    await state.clear()
    await state.set_state(BackupState.waiting_restore_file)
    text = (
        "♻️ Восстановление БД\n\n"
        "Пришлите файл бэкапа <b>документом</b>.\n"
        "Поддерживаемые форматы: <code>.backup</code>, <code>.backup.gz</code>, <code>.dump</code>, "
        "<code>.sql</code>, <code>.sql.gz</code>\n"
        "⚠️ ВНИМАНИЕ: действующая БД будет перезаписана."
    )
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, parse_mode="HTML")
        await target.answer()
    else:
        await target.answer(text, parse_mode="HTML")


# Обычный сценарий (через меню)
@router.callback_query(F.data == "bk:restore")
async def bk_restore_open(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    # Блокируем восстановление не на сервере
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        await cb.message.edit_text("Восстановление разрешено только на сервере (HOST_ROLE != server).")
        await cb.answer()
        await _auto_back_to_menu(cb)
        return
    await _ensure_settings_exists(cb)  # не критично, но подтянем настройки
    await _restore_open_common(cb, state)


# Emergency Restore (без обращения к БД)
@router.callback_query(F.data == "bk:restore_emergency")
async def bk_restore_emergency(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        await cb.message.edit_text("Восстановление разрешено только на сервере (HOST_ROLE != server).")
        await cb.answer()
        await _auto_back_to_menu(cb)
        return
    await _restore_open_common(cb, state)


@router.message(BackupState.waiting_restore_file)
async def bk_restore_file(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    if not msg.document:
        await msg.answer("Пришлите файл <b>документом</b>.", parse_mode="HTML")
        return

    name = (msg.document.file_name or "").lower()
    if not any(name.endswith(ext) for ext in ALLOWED_EXT):
        await msg.answer("Неподдерживаемый формат. Разрешены: .backup, .backup.gz, .dump, .sql, .sql.gz")
        return
    if msg.document.file_size and msg.document.file_size > MAX_BACKUP_SIZE_MB * 1024 * 1024:
        await msg.answer(f"Файл слишком большой (> {MAX_BACKUP_SIZE_MB} МБ).")
        return

    tmpdir = tempfile.mkdtemp(prefix="wb_restore_")
    filepath = os.path.join(tmpdir, msg.document.file_name)
    await msg.bot.download(msg.document, destination=filepath)

    await state.update_data(tmpdir=tmpdir, filepath=filepath)
    await state.set_state(BackupState.waiting_restore_confirm)
    await msg.answer(
        "Файл получен: <code>{}</code>\n\n"
        "⚠️ Чтобы продолжить, напишите фразу: <b>Я ОТДАЮ СЕБЕ ОТЧЁТ</b>\n"
        "Иначе отправьте /cancel".format(html.escape(msg.document.file_name)),
        parse_mode="HTML",
    )


@router.message(BackupState.waiting_restore_confirm)
async def bk_restore_confirm(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return

    # Требуем точную фразу-подтверждение
    if (msg.text or "").strip() != "Я ОТДАЮ СЕБЕ ОТЧЁТ":
        await msg.answer("Нужно написать ровно: Я ОТДАЮ СЕБЕ ОТЧЁТ")
        return

    # Охранный флаг: только сервер
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        await msg.answer("Восстановление разрешено только на сервере (HOST_ROLE != server).")
        await state.clear()
        await _auto_back_to_menu(msg)
        return

    data = await state.get_data()
    filepath = data["filepath"]

    # --- Preflight RESTORE_SCRIPT_PATH (строго серверный скрипт из ENV) ---
    restore_path = os.environ.get("RESTORE_SCRIPT_PATH")
    if not restore_path or not (os.path.isfile(restore_path) and os.access(restore_path, os.X_OK)):
        await msg.answer(
            "♻️ Восстановление недоступно: RESTORE_SCRIPT_PATH не задан "
            "или файл не существует/не исполняем на сервере."
        )
        try:
            shutil.rmtree(data.get("tmpdir", ""), ignore_errors=True)
        finally:
            await state.clear()
        await _auto_back_to_menu(msg)
        return
    # --- /Preflight ---

    await msg.answer("Запускаю восстановление… Пришлю лог выполнения.")

    # Команда: централизованная сборка (sudo -n $RESTORE_SCRIPT_PATH <file>)
    cmd = build_restore_cmd(filepath)

    # Пробрасываем PG-переменные из DB_URL (удобно для инструментов)
    env = os.environ.copy()
    try:
        u = make_url(DB_URL)
        if u.password:
            env["PGPASSWORD"] = u.password
        if u.username:
            env["PGUSER"] = u.username
        if u.host:
            env["PGHOST"] = u.host
        if u.port:
            env["PGPORT"] = str(u.port)
        if u.database:
            env["PGDATABASE"] = u.database
    except Exception:
        pass

    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )
        out, _ = await proc.communicate()
        code = proc.returncode
        log = out.decode(errors="ignore") if out else "(нет вывода)"

        # Если лог длинный — отправляем файлом
        if len(log) > 3500:
            logpath = os.path.join(data["tmpdir"], "restore.log")
            with open(logpath, "w", encoding="utf-8") as f:
                f.write(log)
            await msg.answer_document(FSInputFile(logpath), caption=f"Код завершения: {code}")
        else:
            safe = html.escape(log)
            text = f"Код завершения: {code}\n\n<pre>{safe}</pre>"
            try:
                await msg.answer(text, parse_mode="HTML")
            except TelegramBadRequest:
                logpath = os.path.join(data["tmpdir"], "restore.log")
                with open(logpath, "w", encoding="utf-8") as f:
                    f.write(log)
                await msg.answer_document(FSInputFile(logpath), caption=f"Код завершения: {code}")

        # Пересобираем пул и пингуем БД ТОЛЬКО при успешном восстановлении
        if code == 0:
            try:
                await reset_db_engine()
                await ping_db()
                await msg.answer("✅ Пул подключений к БД пересоздан, соединение проверено.")
            except Exception as e:
                err = html.escape(repr(e))
                await msg.answer(
                    f"⚠️ Бэкап восстановлен, но не удалось проверить соединение:\n<pre>{err}</pre>",
                    parse_mode="HTML",
                )

    except Exception as e:
        try:
            await msg.answer(f"<b>Ошибка запуска восстановления</b>:\n<pre>{html.escape(repr(e))}</pre>", parse_mode="HTML")
        except TelegramBadRequest:
            await msg.answer(f"Ошибка запуска восстановления: {repr(e)}")
    finally:
        try:
            shutil.rmtree(data.get("tmpdir", ""), ignore_errors=True)
        except Exception:
            pass
        await state.clear()
        await _auto_back_to_menu(msg)


# ===== Очистка базы (wipe) =====
def _mask_db_url() -> tuple[str, str]:
    """Вернёт (маскированный URI, имя БД)."""
    try:
        u = make_url(DB_URL)
        safe = u.render_as_string(hide_password=True)
        dbname = u.database or ""
        return safe, dbname
    except Exception:
        return "(не удалось разобрать DB_URL)", ""

@router.callback_query(F.data == "bk:wipe")
async def bk_wipe(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_TELEGRAM_ID:
        return

    # Только на сервере
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        await cb.message.edit_text("Очистка базы разрешена только на сервере (HOST_ROLE != server).")
        await cb.answer()
        await _auto_back_to_menu(cb)
        return

    safe_url, dbname = _mask_db_url()
    await state.set_state(BackupState.waiting_wipe_phrase)
    await state.update_data(dbname=dbname)
    await cb.message.edit_text(
        "🧨 <b>ОЧИСТКА БАЗЫ ДАННЫХ</b>\n\n"
        f"Текущая БД: <code>{html.escape(safe_url)}</code>\n\n"
        "⚠️ Будет удалено ВСЁ содержимое схемы <code>public</code>.\n\n"
        "Чтобы продолжить, напишите ровно: <b>Я ПОДТВЕРЖДАЮ ОЧИСТКУ БД</b>\n"
        "Или нажмите /cancel",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(BackupState.waiting_wipe_phrase)
async def bk_wipe_phrase(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return
    if (msg.text or "").strip() != "Я ПОДТВЕРЖДАЮ ОЧИСТКУ БД":
        await msg.answer("Нужно написать ровно: Я ПОДТВЕРЖДАЮ ОЧИСТКУ БД")
        return

    data = await state.get_data()
    dbname = data.get("dbname") or ""
    if not dbname:
        _, dbname = _mask_db_url()

    await state.set_state(BackupState.waiting_wipe_dbname)
    await msg.answer(
        "Последний шаг.\n"
        f"Введите имя БД для подтверждения: <code>{html.escape(dbname or '(не удалось определить)')}</code>",
        parse_mode="HTML",
    )


@router.message(BackupState.waiting_wipe_dbname)
async def bk_wipe_do(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        return

    data = await state.get_data()
    expected_db = data.get("dbname") or _mask_db_url()[1]
    provided = (msg.text or "").strip()

    if not expected_db or provided != expected_db:
        await msg.answer("Имя БД не совпало. Отменено.")
        await state.clear()
        await _auto_back_to_menu(msg)
        return

    # Чистим БЕЗ пересоздания схемы: TRUNCATE всех таблиц public с каскадом и сбросом идентификаторов
    sql_truncate_all = text("""
DO $$
DECLARE
    stmt text;
BEGIN
    SELECT 'TRUNCATE TABLE ' ||
           string_agg(format('%I.%I', n.nspname, c.relname), ', ')
           || ' RESTART IDENTITY CASCADE'
      INTO stmt
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname = 'public';

    IF stmt IS NOT NULL THEN
        EXECUTE stmt;
    END IF;
END $$;
""")

    try:
        async with get_session() as s:
            await s.execute(sql_truncate_all)
            await s.commit()

        # Пул на всякий случай пересоздадим и проверим подключение
        await reset_db_engine()
        await ping_db()

        await msg.answer("✅ База очищена (TRUNCATE … RESTART IDENTITY CASCADE).")
    except Exception as e:
        safe = html.escape(repr(e))
        await msg.answer(f"❌ Ошибка очистки: <pre>{safe}</pre>", parse_mode="HTML")
    finally:
        await state.clear()
        await _auto_back_to_menu(msg)

```

## Файл: handlers\admin_menu_visibility.py

```python
from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from database.db import get_session
from database.models import UserRole, MenuItem
from database.menu_visibility import (
    LABELS,
    MENU_ORDER,
    ensure_menu_visibility_defaults,
    get_visibility_map_for_role,
    toggle_menu_visibility,
)

router = Router()
# Короткие описания пунктов меню (единая точка правды)
DESCRIPTIONS = {
    MenuItem.stocks:        "Показать текущие остатки по складам и товарам.",
    MenuItem.receiving:     "Оформить приход товара на выбранный склад.",
    MenuItem.supplies:      "Сформировать поставку на склады маркетплейса.",
    MenuItem.packing:       "Подготовить и упаковать позиции к поставке.",
    MenuItem.picking:       "Скомплектовать товары перед упаковкой/поставкой.",
    MenuItem.reports:       "Сформировать отчёты по остаткам, движениям и истории.",
    MenuItem.purchase_cn:   "Учёт закупок/поступлений из Китая (CN).",
    MenuItem.msk_warehouse: "Операции и движения на московский склад.",
    MenuItem.admin:         "Администрирование: пользователи, склады, настройки.",
}


def _kb_visibility(role: UserRole, vis_map: dict[MenuItem, bool]) -> InlineKeyboardMarkup:
    # Debug (можно убрать после проверки)
    print("DEBUG MENU_ORDER:", [mi.name for mi in MENU_ORDER])

    rows: list[list[InlineKeyboardButton]] = []
    for mi in MENU_ORDER:
        visible = vis_map.get(mi, False)
        mark = "✅" if visible else "❌"
        rows.append([
            InlineKeyboardButton(
                text=f"{LABELS[mi]} {mark}",
                callback_data=f"menuvis:{role.name}:{mi.name}:{int(not visible)}",
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ К ролям", callback_data="menuvis:roles")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "menuvis:roles")
async def menuvis_roles(cb: CallbackQuery):
    """Экран выбора роли для настройки видимости."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настройки меню: admin",   callback_data="menuvis:open:admin")],
        [InlineKeyboardButton(text="⚙️ Настройки меню: user",    callback_data="menuvis:open:user")],
        [InlineKeyboardButton(text="⚙️ Настройки меню: manager", callback_data="menuvis:open:manager")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin")],
    ])
    await cb.message.edit_text("Выберите роль для настройки видимости меню:", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("menuvis:open:"))
async def open_menu_visibility(cb: CallbackQuery):
    """Открыть экран видимости роли. Формат: menuvis:open:<ROLE>"""
    role = UserRole[cb.data.split(":")[2]]

    async with get_session() as session:
        # Гарантируем, что в БД есть все пары (role,item)
        await ensure_menu_visibility_defaults(session)
        vis_map = await get_visibility_map_for_role(session, role)

    await cb.message.edit_text(
        f"Настройки меню для роли {role.name.lower()}:",
        reply_markup=_kb_visibility(role, vis_map),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("menuvis:"))
async def toggle_visibility(cb: CallbackQuery):
    """
    Переключить флаг. Формат: menuvis:<ROLE>:<ITEM>:<NEWVAL>
    Пример: menuvis:admin:stocks:0
    """
    parts = cb.data.split(":")
    if parts[1] == "roles":
        # Раньше этот кейс глотался — теперь есть отдельный хендлер menuvis_roles
        await cb.answer()
        return

    role = UserRole[parts[1]]
    item = MenuItem[parts[2]]
    new_val = bool(int(parts[3]))

    async with get_session() as session:
        # переключаем/ставим явное значение; запись создастся, если её не было
        await toggle_menu_visibility(session, role, item, value=new_val)
        vis_map = await get_visibility_map_for_role(session, role)

    await cb.message.edit_text(
        f"Настройки меню для роли {role.name.lower()}:",
        reply_markup=_kb_visibility(role, vis_map),
    )
    await cb.answer()

```

## Файл: handlers\back.py

```python
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

```

## Файл: handlers\cn_purchase.py

```python
# handlers/cn_purchase.py
from __future__ import annotations
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional, Tuple, List

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto,
)
from sqlalchemy import select, or_, func

from database.db import get_session
from database.models import (
    CnPurchase, CnPurchaseItem, CnPurchaseStatus,
    MskInboundDoc, MskInboundItem,
    Product,
)

# ---- опциональная модель фото ----
try:
    from database.models import CnPurchasePhoto  # id, cn_purchase_id, file_id, caption, uploaded_at, uploaded_by_user_id
    HAS_PHOTO_MODEL = True
except Exception:
    HAS_PHOTO_MODEL = False

router = Router()
PAGE_SIZE = 8
PHOTO_PAGE = 8  # по сколько фото показывать за раз

# -------- safe edit ----------
async def safe_edit_text(msg: Message, text: str):
    try:
        await msg.edit_text(text)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            await msg.answer(text)

async def safe_edit_reply_markup(msg: Message, markup: InlineKeyboardMarkup | None):
    try:
        await msg.edit_reply_markup(reply_markup=markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            if markup:
                await msg.answer("⬇️", reply_markup=markup)

# -------- helpers ----------
def fmt_dt(dt: datetime | None) -> str:
    if not dt:
        return "—"
    return dt.strftime("%d.%m.%Y %H:%M")

_re_int = re.compile(r"(\d+)")

def last_int(data: str) -> Optional[int]:
    if not data:
        return None
    m = _re_int.findall(data)
    return int(m[-1]) if m else None

def last_two_ints(data: str) -> Tuple[Optional[int], Optional[int]]:
    if not data:
        return None, None
    m = _re_int.findall(data)
    if not m:
        return None, None
    if len(m) == 1:
        return None, int(m[0])
    return int(m[-2]), int(m[-1])

# -------- FSM ----------
class CnCreateState(StatesGroup):
    picking_product = State()   # список товаров
    waiting_qty = State()       # ввод количества
    waiting_cost = State()      # ввод цены
    entering_search = State()   # ввод строки поиска
    confirm_item = State()      # подтверждение (создать/добавить/назад)
    editing_comment = State()   # ✏️ изменение комментария из карточки
    uploading_photos = State()  # 📷 загрузка фото к документу

# -------- Keyboards ----------
def cn_root_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать документ",      callback_data="cn:new")],
        [InlineKeyboardButton(text="📦 Доставляется в карго", callback_data="cn:list:cargo")],
        [InlineKeyboardButton(text="🚚 Доставляется в РФ",    callback_data="cn:list:ru")],
        [InlineKeyboardButton(text="🗄️ Архив",                callback_data="cn:list:archive")],
        [InlineKeyboardButton(text="⬅️ Назад",                 callback_data="back_to_menu")],
    ])

def cn_doc_actions_kb(doc_id: int, status: CnPurchaseStatus, photos_cnt: int | None = None) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    # Фото — просмотр всегда; добавление — пока не архив
    label = "🖼 Фото" if photos_cnt is None else f"🖼 Фото ({photos_cnt})"
    rows.append([InlineKeyboardButton(text=label, callback_data=f"cn:photos:{doc_id}:1")])
    if status != CnPurchaseStatus.DELIVERED_TO_MSK:
        rows.append([InlineKeyboardButton(text="📷 Добавить фото", callback_data=f"cn:photo:add:{doc_id}")])

    if status == CnPurchaseStatus.SENT_TO_CARGO:
        rows.append([InlineKeyboardButton(
            text="➡️ Перевести: Доставка склад МСК",
            callback_data=f"cn:status:{doc_id}:to_msk"
        )])
        rows.append([InlineKeyboardButton(text="➕ Добавить позицию", callback_data=f"cn:item:add:{doc_id}")])
        rows.append([InlineKeyboardButton(text="✏️ Комментарий", callback_data=f"cn:comment:edit:{doc_id}")])
    elif status == CnPurchaseStatus.SENT_TO_MSK:
        rows.append([InlineKeyboardButton(text="🏢 Открыть в «Склад МСК»", callback_data=f"msk:open:by_cn:{doc_id}")])
        rows.append([InlineKeyboardButton(text="✏️ Комментарий", callback_data=f"cn:comment:edit:{doc_id}")])

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="cn:root")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def cn_lists_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="cn:root")]
    ])

# -------- Products picker ----------
async def fetch_products(search: Optional[str], page: int) -> tuple[list[Product], int]:
    async with get_session() as s:
        q = select(Product).where(Product.is_active.is_(True))
        if search:
            like = f"%{search.strip()}%"
            q = q.where(or_(Product.name.ilike(like), Product.article.ilike(like)))
        total = (await s.execute(select(func.count()).select_from(q.subquery()))).scalar_one()
        q = q.order_by(Product.name.asc()).offset(page * PAGE_SIZE).limit(PAGE_SIZE)
        rows = (await s.execute(q)).scalars().all()
    return rows, int(total)

def product_picker_kb(doc_id: int, page: int, total: int, rows: list[Product], search: Optional[str]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    for p in rows:
        cap = f"{p.name} · {p.article}"
        buttons.append([InlineKeyboardButton(text=cap, callback_data=f"cn:prod:choose:{doc_id}:{p.id}")])

    max_page = max((total - 1) // PAGE_SIZE, 0)
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"cn:prod:list:{doc_id}:{page-1}"))
    if page < max_page:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"cn:prod:list:{doc_id}:{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton(
        text=("🔎 Изменить поиск" if search else "🔎 Поиск"),
        callback_data=f"cn:prod:search:{doc_id}:{page}"
    )])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад к документу", callback_data=f"cn:open:{doc_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def show_product_picker(msg: Message, doc_id: int, state: FSMContext, page: int = 0):
    data = await state.get_data()
    search = data.get("cn_search_text")
    rows, total = await fetch_products(search, page)
    text = "Выберите товар из базы" + (f" (поиск: `{search}`)" if search else "") + f"\nВсего найдено: {total}"
    await safe_edit_text(msg, text)
    await safe_edit_reply_markup(msg, product_picker_kb(doc_id, page, total, rows, search))
    await state.set_state(CnCreateState.picking_product)

# -------- Entry --------
@router.message(F.text == "Закупка CN")
async def cn_entry(msg: Message):
    await msg.answer("Раздел «Закупка CN».", reply_markup=None)
    await msg.answer("Выберите:", reply_markup=cn_root_kb())

@router.callback_query(F.data == "cn:root")
async def cn_root(cb: CallbackQuery):
    await safe_edit_text(cb.message, "Раздел «Закупка CN».")
    await safe_edit_reply_markup(cb.message, cn_root_kb())
    await cb.answer()

# -------- Lists as buttons --------
@router.callback_query(F.data.startswith("cn:list:"))
async def cn_list(cb: CallbackQuery):
    mode = cb.data.split(":")[-1]  # cargo | ru | archive
    async with get_session() as s:
        rows = (await s.execute(select(CnPurchase).order_by(CnPurchase.created_at.desc()))).scalars().all()

    if mode == "cargo":
        rows = [r for r in rows if r.status == CnPurchaseStatus.SENT_TO_CARGO]
        title = "📦 Доставляется в карго"
    elif mode == "ru":
        rows = [r for r in rows if r.status == CnPurchaseStatus.SENT_TO_MSK]
        title = "🚚 Доставляется в РФ"
    else:
        rows = [r for r in rows if r.status == CnPurchaseStatus.DELIVERED_TO_MSK]
        title = "🗄️ Архив"

    if not rows:
        await safe_edit_text(cb.message, f"{title}\n\nСписок пуст.")
        await safe_edit_reply_markup(cb.message, cn_lists_kb())
        await cb.answer()
        return

    kb_rows: list[list[InlineKeyboardButton]] = []
    for r in rows:
        kb_rows.append([InlineKeyboardButton(
            text=f"📄 {r.code} — {r.status.value}",
            callback_data=f"cn:open:{r.id}"
        )])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="cn:root")])

    await safe_edit_text(cb.message, title)
    await safe_edit_reply_markup(cb.message, InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await cb.answer()

# -------- Create: initial status = SENT_TO_CARGO -> picker --------
@router.callback_query(F.data == "cn:new")
async def cn_new(cb: CallbackQuery, state: FSMContext):
    code = "CN-" + datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    async with get_session() as s:
        doc = CnPurchase(code=code, status=CnPurchaseStatus.SENT_TO_CARGO, comment=None)
        if hasattr(doc, "sent_to_cargo_at"):
            doc.sent_to_cargo_at = datetime.utcnow()
        s.add(doc)
        await s.flush()
        doc_id = doc.id
        await s.commit()

    await state.update_data(
        cn_doc_id=doc_id, cn_search_text=None,
        selected_product_id=None, qty=None, cost=None
    )

    await safe_edit_text(cb.message, f"Документ создан: #{code}\nЗагрузка списка товаров…")
    await safe_edit_reply_markup(cb.message, None)
    await show_product_picker(cb.message, doc_id, state, page=0)
    await cb.answer()

# -------- Picker / search / choose --------
@router.callback_query(F.data.startswith("cn:item:add:"))
async def cn_item_add_from_card(cb: CallbackQuery, state: FSMContext):
    doc_id = last_int(cb.data)
    if not doc_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return
    await state.update_data(cn_doc_id=doc_id, selected_product_id=None, qty=None, cost=None)
    await show_product_picker(cb.message, doc_id, state, page=0)
    await cb.answer()

@router.callback_query(F.data.startswith("cn:prod:list:"))
async def cn_prod_list(cb: CallbackQuery, state: FSMContext):
    doc_id, page = last_two_ints(cb.data)
    if doc_id is None or page is None:
        await cb.answer("Неверные параметры пагинации.", show_alert=True)
        return
    await show_product_picker(cb.message, doc_id, state, page=page)
    await cb.answer()

@router.callback_query(F.data.startswith("cn:prod:search:"))
async def cn_prod_search(cb: CallbackQuery, state: FSMContext):
    await state.set_state(CnCreateState.entering_search)
    await safe_edit_text(cb.message, "Введите строку поиска (имя или артикул). Отправьте '-' чтобы очистить фильтр.")
    await safe_edit_reply_markup(cb.message, None)
    await cb.answer()

@router.message(CnCreateState.entering_search)
async def cn_receive_search_text(msg: Message, state: FSMContext):
    text = msg.text.strip()
    search = None if text == "-" else text
    await state.update_data(cn_search_text=search)
    data = await state.get_data()
    doc_id = data["cn_doc_id"]
    out = await msg.answer("Поиск обновлён. Загрузка списка…")
    await show_product_picker(out, doc_id, state, page=0)

# -------- Choose -> qty -> cost -> confirm --------
@router.callback_query(F.data.startswith("cn:prod:choose:"))
async def cn_prod_choose(cb: CallbackQuery, state: FSMContext):
    doc_id, product_id = last_two_ints(cb.data)
    if doc_id is None or product_id is None:
        await cb.answer("Не удалось определить товар/документ.", show_alert=True)
        return
    await state.update_data(cn_doc_id=doc_id, selected_product_id=product_id)
    await safe_edit_text(cb.message, "Введите количество единиц (шт.).")
    await safe_edit_reply_markup(cb.message, None)
    await state.set_state(CnCreateState.waiting_qty)
    await cb.answer()

@router.message(CnCreateState.waiting_qty)
async def cn_item_qty(msg: Message, state: FSMContext):
    txt = msg.text.strip()
    if not txt.isdigit():
        await msg.answer("Некорректное число. Введите количество единиц (шт.).")
        return
    qty = int(txt)
    if qty <= 0:
        await msg.answer("Количество должно быть больше 0.")
        return
    await state.update_data(qty=qty)
    await msg.answer("Введите стоимость единицы товара (₽).")
    await state.set_state(CnCreateState.waiting_cost)

@router.message(CnCreateState.waiting_cost)
async def cn_item_cost(msg: Message, state: FSMContext):
    raw = msg.text.replace(",", ".").strip()
    try:
        cost = Decimal(raw)
    except (InvalidOperation, ValueError):
        await msg.answer("Некорректная цена. Введите стоимость единицы товара (₽).")
        return
    if cost <= 0:
        await msg.answer("Цена должна быть больше 0.")
        return

    await state.update_data(cost=cost)
    data = await state.get_data()
    async with get_session() as s:
        p = await s.get(Product, data["selected_product_id"])
    name = f"{p.name} · {p.article}" if p else f"product_id={data['selected_product_id']}"

    text = (
        "Добавляем позицию:\n"
        f"• {name}\n"
        f"• Кол-во: {data['qty']} шт.\n"
        f"• Цена: {data['cost']:.2f} ₽\n\n"
        "Выберите действие:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧾 Создать документ", callback_data="cn:item:commit:finish")],
        [InlineKeyboardButton(text="➕ Добавить товар",   callback_data="cn:item:commit:add_more")],
        [InlineKeyboardButton(text="⬅️ Назад",            callback_data=f"cn:prod:list:{data['cn_doc_id']}:0")],
    ])
    out = await msg.answer(text, reply_markup=kb)
    await state.update_data(confirm_msg_id=out.message_id)
    await state.set_state(CnCreateState.confirm_item)

async def _commit_item(state: FSMContext):
    data = await state.get_data()
    async with get_session() as s:
        existing = (await s.execute(
            select(CnPurchaseItem).where(
                (CnPurchaseItem.cn_purchase_id == data["cn_doc_id"]) &
                (CnPurchaseItem.product_id == data["selected_product_id"]) &
                (CnPurchaseItem.unit_cost_rub == data["cost"])
            )
        )).scalar_one_or_none()
        if existing:
            existing.qty = existing.qty + data["qty"]
        else:
            s.add(CnPurchaseItem(
                cn_purchase_id=data["cn_doc_id"],
                product_id=data["selected_product_id"],
                qty=data["qty"],
                unit_cost_rub=data["cost"],
            ))
        await s.commit()

@router.callback_query(F.data == "cn:item:commit:add_more")
async def cn_commit_add_more(cb: CallbackQuery, state: FSMContext):
    await _commit_item(state)
    await state.update_data(selected_product_id=None, qty=None, cost=None)
    await safe_edit_text(cb.message, "Позиция добавлена. Выберите следующий товар:")
    await show_product_picker(cb.message, (await state.get_data())["cn_doc_id"], state, page=0)
    await cb.answer("Добавлено.")

@router.callback_query(F.data == "cn:item:commit:finish")
async def cn_commit_finish(cb: CallbackQuery, state: FSMContext):
    await _commit_item(state)
    data = await state.get_data()
    doc_id = data.get("cn_doc_id")
    if not doc_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return
    await state.update_data(selected_product_id=None, qty=None, cost=None, confirm_msg_id=None)
    await render_doc(cb.message, doc_id)
    await cb.answer("Позиция добавлена, документ открыт.")

# -------- View / comment / status --------
async def _fetch_cn_view(doc_id: int):
    async with get_session() as s:
        doc = await s.get(CnPurchase, doc_id)
        items = (await s.execute(select(CnPurchaseItem).where(CnPurchaseItem.cn_purchase_id == doc_id))).scalars().all()
        pmap = {}
        if items:
            pids = [it.product_id for it in items]
            prows = (await s.execute(select(Product).where(Product.id.in_(pids)))).scalars().all()
            pmap = {p.id: p for p in prows}
        photos_cnt = 0
        if HAS_PHOTO_MODEL:
            photos_cnt = (await s.execute(
                select(func.count()).select_from(CnPurchasePhoto).where(CnPurchasePhoto.cn_purchase_id == doc_id)
            )).scalar_one()
        # связанный MSK-док (для шагов 4–5)
        msk = (await s.execute(select(MskInboundDoc).where(MskInboundDoc.cn_purchase_id == doc_id))).scalar_one_or_none()
        msk_to_our_at = getattr(msk, "to_our_at", None) if msk else None
        msk_received_at = getattr(msk, "received_at", None) if msk else None
    return doc, items, pmap, photos_cnt, msk_to_our_at, msk_received_at

async def render_doc(msg: Message, doc_id: int):
    doc, items, pmap, photos_cnt, msk_to_our_at, msk_received_at = await _fetch_cn_view(doc_id)

    lines = [
        f"📄 {doc.code} — {doc.status.value}",
        f"💬 Комментарий: {doc.comment or '—'}",
        f"🖼 Фото: {photos_cnt} шт.",
        "",
        "🧱 Позиции:",
    ]
    if not items:
        lines.append("— пока пусто —")
    else:
        for it in items:
            p = pmap.get(it.product_id)
            title = f"{p.name} · {p.article}" if p else f"id={it.product_id}"
            from decimal import Decimal as _D
            price = f"{(it.unit_cost_rub or _D('0')):.2f}"
            lines.append(f"• {title} — {it.qty} шт. × {price} ₽")

    # Полная хронология (1–6)
    created_at        = fmt_dt(getattr(doc, 'created_at', None))
    sent_to_cargo_at  = fmt_dt(getattr(doc, 'sent_to_cargo_at', None))
    sent_to_msk_at    = fmt_dt(getattr(doc, 'sent_to_msk_at', None))
    to_our_at_txt     = fmt_dt(msk_to_our_at)        # 4) Отправлен на региональный склад
    received_at_txt   = fmt_dt(msk_received_at)      # 5) Приходован на склад
    archived_at       = fmt_dt(getattr(doc, 'archived_at', None))

    lines += [
        "",
        "🕓 Хронология:",
        f"1) Создан: {created_at}",
        f"2) Отправлен в карго: {sent_to_cargo_at}",
        f"3) Переведён в отправку на склад МСК: {sent_to_msk_at}",
        f"4) Отправлен на региональный склад: {to_our_at_txt}",
        f"5) Приходован на склад: {received_at_txt}",
        f"6) Архивирован: {archived_at}",
    ]

    await safe_edit_text(msg, "\n".join(lines))
    await safe_edit_reply_markup(msg, cn_doc_actions_kb(doc_id, doc.status, photos_cnt))

@router.callback_query(F.data.startswith("cn:open"))
async def cn_open(cb: CallbackQuery):
    """
    Универсальный open:
    - ловит и 'cn:open:123', и случайные вариации 'cn:open'
    - если нажато под медиа — удаляет медиа и присылает карточку
    """
    doc_id = last_int(cb.data)
    if not doc_id:
        await cb.answer("Не удалось определить документ (нет ID).", show_alert=True)
        return

    # если кнопка под медиа — удаляем сообщение с медиа
    if getattr(cb.message, "photo", None) or getattr(cb.message, "video", None) \
            or getattr(cb.message, "animation", None) or getattr(cb.message, "document", None):
        try:
            await cb.message.delete()
        except TelegramBadRequest:
            pass
        out = await cb.message.answer("Открываю документ…")
        await render_doc(out, doc_id)
    else:
        await render_doc(cb.message, doc_id)

    await cb.answer()

@router.callback_query(F.data.startswith("cn:comment:edit:"))
async def cn_comment_edit(cb: CallbackQuery, state: FSMContext):
    doc_id = last_int(cb.data)
    if not doc_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return
    async with get_session() as s:
        doc = await s.get(CnPurchase, doc_id)
        if doc.status == CnPurchaseStatus.DELIVERED_TO_MSK:
            await cb.answer("Документ в архиве. Редактирование комментария недоступно.", show_alert=True)
            return
    await state.update_data(cn_doc_id=doc_id)
    await safe_edit_text(cb.message, "Введите новый комментарий (или '-' чтобы очистить):")
    await safe_edit_reply_markup(cb.message, None)
    await state.set_state(CnCreateState.editing_comment)
    await cb.answer()

@router.message(CnCreateState.editing_comment)
async def cn_comment_edit_save(msg: Message, state: FSMContext):
    data = await state.get_data()
    doc_id = data["cn_doc_id"]
    comment = None if msg.text.strip() == "-" else msg.text.strip()
    async with get_session() as s:
        doc = await s.get(CnPurchase, doc_id)
        doc.comment = comment
        await s.commit()
    out = await msg.answer("Комментарий обновлён. Открываю документ…")
    await render_doc(out, doc_id)

@router.callback_query(F.data.startswith("cn:status:"))
async def cn_set_status(cb: CallbackQuery):
    if not cb.data.endswith(":to_msk"):
        await cb.answer("Недоступный переход", show_alert=True)
        return
    doc_id = last_int(cb.data)
    if not doc_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return

    async with get_session() as s:
        doc = await s.get(CnPurchase, doc_id)
        if doc.status != CnPurchaseStatus.SENT_TO_CARGO:
            await cb.answer("Перевод возможен только из статуса «Доставляется в карго».", show_alert=True)
            return
        doc.status = CnPurchaseStatus.SENT_TO_MSK
        if hasattr(doc, "sent_to_msk_at"):
            doc.sent_to_msk_at = datetime.utcnow()
        await s.flush()

        msk = (await s.execute(select(MskInboundDoc).where(MskInboundDoc.cn_purchase_id == doc.id))).scalar_one_or_none()
        if msk is None:
            # Кладём DOCNAME в комментарий MSK-дока — дальше его можно использовать в визуализации
            msk = MskInboundDoc(
                cn_purchase_id=doc.id,
                created_at=datetime.utcnow(),
                created_by_user_id=None,
                comment=f"[DOCNAME: {doc.code}] Из CN #{doc.code}",
            )
            s.add(msk)
            await s.flush()
            items = (await s.execute(select(CnPurchaseItem).where(CnPurchaseItem.cn_purchase_id == doc.id))).scalars().all()
            for it in items:
                s.add(MskInboundItem(
                    msk_inbound_id=msk.id,
                    product_id=it.product_id,
                    qty=it.qty,
                    unit_cost_rub=it.unit_cost_rub,
                ))

        await s.commit()

    await cb.answer("Статус обновлён: документ доступен в «Склад МСК → Доставка в РФ».")
    await render_doc(cb.message, doc_id)

# -------- Фото: добавление/просмотр --------
@router.callback_query(F.data.startswith("cn:photo:add:"))
async def cn_photo_add_entry(cb: CallbackQuery, state: FSMContext):
    if not HAS_PHOTO_MODEL:
        await cb.answer("Модуль фото не активирован (нужна миграция).", show_alert=True)
        return
    doc_id = last_int(cb.data)
    if not doc_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return
    await state.update_data(cn_doc_id=doc_id)
    await state.set_state(CnCreateState.uploading_photos)
    await safe_edit_text(cb.message, "Загрузите 1–N фото (изображениями).")
    await safe_edit_reply_markup(cb.message, InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад к документу", callback_data=f"cn:open:{doc_id}")],
    ]))
    await cb.answer()

@router.message(CnCreateState.uploading_photos, F.photo)
async def cn_photo_save(msg: Message, state: FSMContext):
    if not HAS_PHOTO_MODEL:
        await msg.answer("Модуль фото не активирован (нужна миграция).")
        return
    data = await state.get_data()
    doc_id = data.get("cn_doc_id")
    if not doc_id:
        await msg.answer("Сессия потеряна. Откройте документ заново.")
        return

    file_id = msg.photo[-1].file_id
    caption = (msg.caption or "").strip() or None
    async with get_session() as s:
        s.add(CnPurchasePhoto(
            cn_purchase_id=doc_id,
            file_id=file_id,
            caption=caption,
            uploaded_at=datetime.utcnow(),
            uploaded_by_user_id=None,
        ))
        await s.commit()

    # после сохранения отправляем НАШЕ фото с кнопками
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить ещё фото", callback_data=f"cn:photo:more:{doc_id}")],
        [InlineKeyboardButton(text="✅ Готово", callback_data=f"cn:photo:done:{doc_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к документу", callback_data=f"cn:open:{doc_id}")],
    ])
    await msg.answer_photo(file_id, caption=caption or "", reply_markup=kb)

@router.callback_query(F.data.startswith("cn:photo:more:"))
async def cn_photo_more(cb: CallbackQuery, state: FSMContext):
    """Удаляем превью с кнопками и остаёмся в режиме загрузки — просим прислать следующее фото."""
    doc_id = last_int(cb.data)
    if not doc_id:
        await cb.answer("Документ не найден.", show_alert=True)
        return

    # удалить наше превью с кнопками
    try:
        await cb.message.delete()
    except TelegramBadRequest:
        pass

    # остаёмся в состоянии uploading_photos
    await state.update_data(cn_doc_id=doc_id)
    await state.set_state(CnCreateState.uploading_photos)
    await cb.message.answer("Фото сохранено. Пришлите следующее фото или нажмите «⬅️ Назад к документу».")
    await cb.answer("Ок, ждём следующее фото.")

@router.callback_query(F.data.startswith("cn:photo:done:"))
async def cn_photo_done_btn(cb: CallbackQuery, state: FSMContext):
    doc_id = last_int(cb.data)
    if not doc_id:
        await cb.answer("Документ не найден.", show_alert=True)
        return

    await state.clear()

    # закрываем (удаляем) наше сообщение с фото/кнопками и возвращаемся в карточку
    try:
        await cb.message.delete()
    except TelegramBadRequest:
        pass

    out = await cb.message.answer("Готово. Открываю документ…")
    await render_doc(out, doc_id)
    await cb.answer("Готово.")

@router.callback_query(F.data.startswith("cn:photos:"))
async def cn_photos_view(cb: CallbackQuery):
    # формат: cn:photos:{cn_id}:{page}
    if not HAS_PHOTO_MODEL:
        await cb.answer("Модуль фото не активирован (нужна миграция).", show_alert=True)
        return
    cn_id, page = last_two_ints(cb.data)
    if not cn_id or not page:
        await cb.answer("Параметры не распознаны.", show_alert=True)
        return

    async with get_session() as s:
        base_q = select(CnPurchasePhoto).where(CnPurchasePhoto.cn_purchase_id == cn_id).order_by(CnPurchasePhoto.uploaded_at.asc())
        total = (await s.execute(select(func.count()).select_from(base_q.subquery()))).scalar_one()
        # одна фотка на страницу
        row = (await s.execute(base_q.offset(page - 1).limit(1))).scalar_one_or_none()

    if not row:
        await cb.answer("Фото отсутствуют.")
        return

    prev_page = page - 1 if page > 1 else None
    next_page = page + 1 if page < total else None

    # клавиатура под фото: навигация, «Назад к документу», «Готово»
    buttons: list[list[InlineKeyboardButton]] = []
    nav_row: list[InlineKeyboardButton] = []
    if prev_page:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cn:photos:{cn_id}:{prev_page}"))
    if next_page:
        nav_row.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"cn:photos:{cn_id}:{next_page}"))
    if nav_row:
        buttons.append(nav_row)
    buttons.append([InlineKeyboardButton(text="⬅️ Назад к документу", callback_data=f"cn:open:{cn_id}")])
    buttons.append([InlineKeyboardButton(text="✅ Готово", callback_data=f"cn:photo:done:{cn_id}")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    # отправляем фото с inline-клавиатурой
    await cb.message.answer_photo(row.file_id, caption=row.caption or "", reply_markup=kb)
    await cb.answer()

# -------- register --------
def register_cn_purchase_handlers(dp):
    dp.include_router(router)

```

## Файл: handlers\common.py

```python
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
from keyboards.main_menu import (
    get_main_menu,
    get_procure_submenu,
    get_pack_submenu,
    # Для текстов и групп — чтобы строить описания под заголовком
    TEXTS, PROCURE_GROUP, PACK_GROUP,
)
from database.db import get_session, set_audit_user, init_db
from database.models import User, UserRole

# ➕ добавляем функции и описания для пунктов меню
from database.menu_visibility import get_visible_menu_items_for_role
from database import menu_visibility as mv


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
            # ЕДИНЫЙ заголовок и меню в зависимости от роли
            caption = await _root_caption_for_role(UserRole.admin)
            await message.answer(caption, reply_markup=await get_main_menu(UserRole.admin))
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
        # ЕДИНЫЙ заголовок и меню в зависимости от роли
        caption = await _root_caption_for_role(user.role)
        await message.answer(caption, reply_markup=await get_main_menu(user.role))
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


async def _root_caption_for_role(role: UserRole) -> str:
    """Красивый и информативный заголовок главного меню с описанием разделов по доступам роли."""
    async with get_session() as session:
        visible = set(await get_visible_menu_items_for_role(session, role))

    lines: list[str] = []

    # Заголовок
    role_map = {
        UserRole.admin: "Администратор",
        UserRole.user: "Пользователь",
        UserRole.manager: "Менеджер",
    }
    role_name = role_map.get(role, str(role).title())
    lines.append(f"🌿 *Главное меню*  ·  роль: *{role_name}*")

    # Подсказка
    lines.append("Выберите раздел ниже — кратко, что внутри:")

    # Закупки / Поступления
    if any(it in visible for it in PROCURE_GROUP):
        lines += [
            "",
            "🧾 *Закупки и поступления*",
            " • Приём товара на склад с комментарием и аудитом.",
            " • Корректировки остатков (дельта/новое количество, причина).",
            " • Быстрый просмотр текущих остатков по складам и товарам.",
        ]

    # Упаковка / Поставки
    if any(it in visible for it in PACK_GROUP):
        lines += [
            "",
            "📦 *Упаковка и поставки*",
            " • Формирование поставок: выбор склада-источника и товаров.",
            " • Контроль лимитов: нельзя списать больше остатка.",
            " • Экспорт в Google Sheets и отправка менеджеру на подтверждение.",
            " • История поставок и статусы (сборка/подтверждение).",
        ]

    # Отчёты
    if any(mi.name == "reports" for mi in visible):
        lines += [
            "",
            "📈 *Отчёты*",
            " • Остатки на дату, движения и история операций.",
            " • Фильтры по складу/товару/периоду, экспорт в Google Sheets.",
            " • Уведомления о низких остатках (настраиваемые пороги).",
        ]

    # Администрирование
    if any(mi.name == "admin" for mi in visible):
        lines += [
            "",
            "⚙️ *Администрирование*",
            " • Пользователи и роли, видимость пунктов меню.",
            " • Справочники: товары, склады и настройки системы.",
            " • Журнал действий и служебные операции.",
        ]

    # Хвостовая подсказка
    lines += [
        "",
        "ℹ️ Подсказка: используйте *кнопки меню* ниже. Для возврата — нажмите «Назад».",
    ]

    return "\n".join(lines)



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
    # ЕДИНЫЙ заголовок и меню
    caption = await _root_caption_for_role(user.role)
    try:
        await cb.message.edit_text(caption, reply_markup=await get_main_menu(user.role))
    except Exception:
        await cb.message.answer(caption, reply_markup=await get_main_menu(user.role))


# ---------------------------
# Навигация по корневым категориям (с описаниями)
# ---------------------------
async def show_root_menu(cb: types.CallbackQuery, user: User):
    """Корневое меню: 2 категории + Отчёты + Администрирование (если разрешены) — с единым заголовком."""
    await cb.answer()
    caption = await _root_caption_for_role(user.role)
    try:
        await cb.message.edit_text(caption, reply_markup=await get_main_menu(user.role))
    except Exception:
        await cb.message.answer(caption, reply_markup=await get_main_menu(user.role))


async def show_procure_menu(cb: types.CallbackQuery, user: User):
    """Подменю «Закупки-поступления»: описания для каждой видимой кнопки."""
    await cb.answer()
    async with get_session() as session:
        visible = set(await get_visible_menu_items_for_role(session, user.role))

    lines = ["Закупки-поступления:"]
    for it in PROCURE_GROUP:
        if it in visible:
            lines.append(f"{TEXTS[it]} — {mv.DESCRIPTIONS.get(it, 'Описание отсутствует.')}")

    await cb.message.edit_text("\n".join(lines), reply_markup=await get_procure_submenu(user.role))


async def show_pack_menu(cb: types.CallbackQuery, user: User):
    """Подменю «Упаковка-поставки»: описания для каждой видимой кнопки."""
    await cb.answer()
    async with get_session() as session:
        visible = set(await get_visible_menu_items_for_role(session, user.role))

    lines = ["Упаковка-поставки:"]
    for it in PACK_GROUP:
        if it in visible:
            lines.append(f"{TEXTS[it]} — {mv.DESCRIPTIONS.get(it, 'Описание отсутствует.')}")

    await cb.message.edit_text("\n".join(lines), reply_markup=await get_pack_submenu(user.role))


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

    # Корневая навигация (с описаниями)
    dp.callback_query.register(show_root_menu,    lambda c: c.data == "root:main")
    dp.callback_query.register(show_procure_menu, lambda c: c.data == "root:procure")
    dp.callback_query.register(show_pack_menu,    lambda c: c.data == "root:pack")

    # Прежние заглушки (если где-то используются старые callbacks)
    dp.callback_query.register(on_ostatki,  lambda c: c.data == "ostatki")
    dp.callback_query.register(on_prihod,   lambda c: c.data == "prihod")
    dp.callback_query.register(on_korr_ost, lambda c: c.data == "korr_ost")
    dp.callback_query.register(on_postavki, lambda c: c.data == "postavki")
    dp.callback_query.register(on_otchety,   lambda c: c.data == "otchety")
    dp.callback_query.register(back_to_main_menu, lambda c: c.data == "back_to_menu")

    # Подключить совместимость (последним из меню-роутеров)
    from handlers import common_compat
    dp.include_router(common_compat.router)

    # Подключаем noop ПОСЛЕДНИМ
    dp.include_router(noop_router)

```

## Файл: handlers\common_compat.py

```python
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

```

## Файл: handlers\manager.py

```python
# handlers/manager.py
from __future__ import annotations

from typing import List, Tuple

from aiogram import Router, F, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select, func, desc

from database.db import get_session, available_packed
from database.models import (
    User, UserRole,
    Supply, SupplyItem, Warehouse, Product,
    StockMovement, MovementType, ProductStage,
)
from handlers.common import send_content

router = Router()
PAGE = 10

# ---------------------------
# UI helpers
# ---------------------------

def _kb_manager_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 К сборке",     callback_data="mgr:list:queued")],
        [InlineKeyboardButton(text="🛠 В работе",     callback_data="mgr:list:assembling")],
        [InlineKeyboardButton(text="✅ Собранные",    callback_data="mgr:list:assembled")],
        [InlineKeyboardButton(text="🚚 В пути",       callback_data="mgr:list:in_transit")],
        [InlineKeyboardButton(text="⬅️ Назад",        callback_data="back_to_menu")],
    ])

_TITLES = {
    "queued": "📥 К сборке",
    "assembling": "🛠 В работе",
    "assembled": "✅ Собранные",
    "in_transit": "🚚 Доставляется",
    "archived_delivered": "🗄 Доставлена (архив)",
    "archived_returned": "🗄 Возврат (архив)",
    "cancelled": "❌ Отменена",
}

def _kb_list(items: List[Tuple[int, str, int]], page: int, status: str) -> InlineKeyboardMarkup:
    start = page * PAGE
    chunk = items[start:start+PAGE]
    rows: List[List[InlineKeyboardButton]] = []

    for sid, wh_name, cnt in chunk:
        rows.append([InlineKeyboardButton(
            text=f"SUP-{sid} • {wh_name} • позиций {cnt}",
            callback_data=f"mgr:open:{sid}"
        )])

    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"mgr:list:{status}:{page-1}"))
    if start + PAGE < len(items):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"mgr:list:{status}:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="🏠 Меню менеджера", callback_data="manager")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_card(s: Supply) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    # Доступные действия зависят от статуса (см. ТЗ §6.5)
    if s.status == "in_transit":
        rows.append([InlineKeyboardButton(text="✅ Доставлено",          callback_data=f"mgr:delivered:{s.id}")])
        rows.append([InlineKeyboardButton(text="↩️ Возврат",             callback_data=f"mgr:return:{s.id}")])
        rows.append([InlineKeyboardButton(text="♻️ Расформировать",      callback_data=f"mgr:unpost:{s.id}")])

    # Общая навигация
    rows.append([InlineKeyboardButton(text="⬅️ К спискам", callback_data="manager")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------------------------
# Root
# ---------------------------

@router.callback_query(F.data == "manager")
async def manager_root(cb: types.CallbackQuery, user: User):
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return
    await send_content(cb, "Управление поставками:", reply_markup=_kb_manager_root())


# ---------------------------
# Списки по статусам (с пагинацией)
# ---------------------------

@router.callback_query(F.data.startswith("mgr:list:"))
async def mgr_list(cb: types.CallbackQuery, user: User):
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return

    parts = cb.data.split(":")
    # варианты: "mgr:list:queued" или "mgr:list:queued:2"
    status = parts[2]
    page = int(parts[3]) if len(parts) > 3 else 0

    async with get_session() as s:
        rows = (await s.execute(
            select(
                Supply.id,
                Warehouse.name,
                func.count(SupplyItem.id)
            )
            .join(Warehouse, Warehouse.id == Supply.warehouse_id)
            .outerjoin(SupplyItem, SupplyItem.supply_id == Supply.id)
            .where(Supply.status == status)           # ВАЖНО: VARCHAR сравниваем со строкой
            .group_by(Supply.id, Warehouse.name)
            .order_by(Supply.id.desc())
        )).all()

    items: List[Tuple[int, str, int]] = [(r[0], r[1], int(r[2])) for r in rows]
    if not items:
        await send_content(cb, f"{_TITLES.get(status, status)}\n\nСписок пуст.", reply_markup=_kb_manager_root())
        return

    await send_content(
        cb,
        f"{_TITLES.get(status, status)} — выберите поставку:",
        reply_markup=_kb_list(items, page, status)
    )


# ---------------------------
# Карточка поставки (просмотр)
# ---------------------------

@router.callback_query(F.data.startswith("mgr:open:"))
async def mgr_open(cb: types.CallbackQuery, user: User):
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return

    sid = int(cb.data.split(":")[-1])
    async with get_session() as s:
        sup = (await s.execute(select(Supply).where(Supply.id == sid))).scalar_one_or_none()
        if not sup:
            await cb.answer("Поставка не найдена", show_alert=True)
            return

        wh_name = (await s.execute(select(Warehouse.name).where(Warehouse.id == sup.warehouse_id))).scalar_one()
        items = (await s.execute(
            select(SupplyItem.product_id, SupplyItem.qty)
            .where(SupplyItem.supply_id == sid)
            .order_by(SupplyItem.id)
        )).all()

        # Тело карточки + контроль доступности
        lines: List[str] = []
        total_qty = 0
        total_def = 0
        for pid, need in items:
            prod = (await s.execute(select(Product.name, Product.article).where(Product.id == pid))).first()
            name, art = prod if prod else (f"#{pid}", None)
            avail = await available_packed(s, sup.warehouse_id, pid)
            deficit = max(0, need - max(avail, 0))
            total_qty += int(need)
            total_def += int(deficit)
            lines.append(
                f"• `{art or pid}` — *{name}*: план {need}, доступно PACKED {avail}, дефицит {deficit}"
            )

    head = (
        f"📦 Поставка *SUP-{sid}*\n"
        f"🏬 Склад-источник: *{wh_name}*\n"
        f"🧭 Статус: *{sup.status}*\n"
        f"—\n"
    )
    body = "\n".join(lines) if lines else "_Позиции отсутствуют._"
    tail = f"\n\n📈 Итого: {len(items)} позиций, план {total_qty}, суммарный дефицит {total_def}"
    await send_content(cb, head + body + tail, parse_mode="Markdown", reply_markup=_kb_card(sup))


# ---------------------------
# Действия по in_transit (менеджер)
# ---------------------------

async def _next_doc_id() -> int:
    async with get_session() as s:
        max_doc = (await s.execute(select(func.max(StockMovement.doc_id)))).scalar()
        return int((max_doc or 0) + 1)

@router.callback_query(F.data.startswith("mgr:delivered:"))
async def mgr_delivered(cb: types.CallbackQuery, user: User):
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return

    sid = int(cb.data.split(":")[-1])
    async with get_session() as s:
        sup = (await s.execute(select(Supply).where(Supply.id == sid))).scalar_one_or_none()
        if not sup:
            return await cb.answer("Поставка не найдена", show_alert=True)
        if sup.status != "in_transit":
            return await cb.answer("Действие доступно только из статуса in_transit", show_alert=True)

        sup.status = "archived_delivered"
        await s.commit()

    await cb.answer("Отмечено как доставлено.")
    await mgr_open(cb, user)  # перерисовать карточку


@router.callback_query(F.data.startswith("mgr:return:"))
async def mgr_return(cb: types.CallbackQuery, user: User):
    """
    Возврат: приход PACKED по всем позициям поставки и статус -> archived_returned.
    """
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return

    sid = int(cb.data.split(":")[-1])
    async with get_session() as s:
        sup = (await s.execute(select(Supply).where(Supply.id == sid))).scalar_one_or_none()
        if not sup:
            return await cb.answer("Поставка не найдена", show_alert=True)
        if sup.status != "in_transit":
            return await cb.answer("Действие доступно только из статуса in_transit", show_alert=True)

        rows = (await s.execute(
            select(SupplyItem.product_id, SupplyItem.qty)
            .where(SupplyItem.supply_id == sid)
        )).all()

        doc_id = await _next_doc_id()
        for pid, qty in rows:
            s.add(StockMovement(
                warehouse_id=sup.warehouse_id,
                product_id=pid,
                qty=qty,
                type=MovementType.postavka,            # тип «поставка» используем для связанных движений
                stage=ProductStage.packed,
                user_id=user.id,
                doc_id=doc_id,
                comment=f"[SUP-RET {sid}] Возврат из МП",
            ))

        sup.status = "archived_returned"
        await s.commit()

    await cb.answer("Возврат оформлен.")
    await mgr_open(cb, user)


@router.callback_query(F.data.startswith("mgr:unpost:"))
async def mgr_unpost(cb: types.CallbackQuery, user: User):
    """
    Расформировать: приход PACKED (возврат на склад) и статус -> assembled.
    """
    if user.role not in (UserRole.manager, UserRole.admin):
        await cb.answer("Нет прав", show_alert=True)
        return

    sid = int(cb.data.split(":")[-1])
    async with get_session() as s:
        sup = (await s.execute(select(Supply).where(Supply.id == sid))).scalar_one_or_none()
        if not sup:
            return await cb.answer("Поставка не найдена", show_alert=True)
        if sup.status != "in_transit":
            return await cb.answer("Действие доступно только из статуса in_transit", show_alert=True)

        rows = (await s.execute(
            select(SupplyItem.product_id, SupplyItem.qty)
            .where(SupplyItem.supply_id == sid)
        )).all()

        doc_id = await _next_doc_id()
        for pid, qty in rows:
            s.add(StockMovement(
                warehouse_id=sup.warehouse_id,
                product_id=pid,
                qty=qty,
                type=MovementType.postavka,
                stage=ProductStage.packed,
                user_id=user.id,
                doc_id=doc_id,
                comment=f"[SUP-UNPOST {sid}] Расформирование поставки",
            ))

        sup.status = "assembled"   # вернули в собранные; короба открываются — реализуется в карточке/коробах
        await s.commit()

    await cb.answer("Поставка расформирована.")
    await mgr_open(cb, user)


# ---------------------------
# Регистрация
# ---------------------------

def register_manager_handlers(dp):
    dp.include_router(router)

```

## Файл: handlers\menu_info.py

```python
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

```

## Файл: handlers\msk_inbound.py

```python
# handlers/msk_inbound.py
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional, Tuple, List, Dict

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select

from database.db import get_session
from database.models import (
    MovementType, ProductStage,
    CnPurchase,  # нужен для кода и таймлайна
    CnPurchaseStatus,
    MskInboundDoc, MskInboundItem, MskInboundStatus,
    Warehouse, Product, StockMovement, User,
)

router = Router()

# ========= safe edit =========
async def safe_edit_text(msg: Message, text: str):
    try:
        await msg.edit_text(text)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            await msg.answer(text)

async def safe_edit_reply_markup(msg: Message, markup: InlineKeyboardMarkup | None):
    try:
        await msg.edit_reply_markup(reply_markup=markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            if markup:
                await msg.answer("⬇️", reply_markup=markup)

# ========= helpers =========
_re_int = re.compile(r"(\d+)")
_DOCNAME_RE = re.compile(r"\[(?:DOCNAME|NAME)\s*:\s*([^\]]+)\]", re.IGNORECASE)

def last_int(data: str) -> Optional[int]:
    if not data:
        return None
    m = _re_int.findall(data)
    return int(m[-1]) if m else None

def last_two_ints(data: str) -> Tuple[Optional[int], Optional[int]]:
    if not data:
        return None, None
    m = _re_int.findall(data)
    if not m:
        return None, None
    if len(m) == 1:
        return None, int(m[0])
    return int(m[-2]), int(m[-1])

def fmt_dt(dt: datetime | None) -> str:
    return dt.strftime("%d.%m.%Y %H:%M") if dt else "—"

def docname_from_text(text: Optional[str]) -> Optional[str]:
    """Достаёт [DOCNAME: ...] из комментария, если есть."""
    if not text:
        return None
    m = _DOCNAME_RE.search(text)
    return m.group(1).strip() if m else None

# ========= keyboards =========
def msk_root_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚚 Доставка в РФ",          callback_data="msk:list:in_ru")],
        [InlineKeyboardButton(text="🏢 Доставка на наш склад",  callback_data="msk:list:to_our")],
        [InlineKeyboardButton(text="🗄️ Архив",                  callback_data="msk:list:archive")],
        [InlineKeyboardButton(text="⬅️ Назад",                  callback_data="back_to_menu")],
    ])

def msk_doc_kb(msk_id: int, status: MskInboundStatus, warehouse_id: Optional[int], cn_id: Optional[int]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    if cn_id:
        rows.append([InlineKeyboardButton(text="👀 Фото CN", callback_data=f"cn:photos:{cn_id}:1")])

    if status == MskInboundStatus.PENDING and not warehouse_id:
        rows.append([InlineKeyboardButton(
            text="➡️ Перевести: Доставка на наш склад",
            callback_data=f"msk:to_our:{msk_id}"
        )])
    if status == MskInboundStatus.PENDING and warehouse_id:
        rows.append([InlineKeyboardButton(
            text="✅ Принято (оприходовать)",
            callback_data=f"msk:deliver:{msk_id}"
        )])

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="msk:root")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def msk_wh_kb(msk_id: int, warehouses: list[Warehouse]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    for w in warehouses:
        buttons.append([InlineKeyboardButton(text=w.name, callback_data=f"msk:whchoose:{msk_id}:{w.id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"msk:open:{msk_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ========= entry =========
@router.message(F.text == "Склад МСК")
async def msk_entry(msg: Message):
    await msg.answer("Раздел «Склад МСК».", reply_markup=None)
    await msg.answer("Выберите:", reply_markup=msk_root_kb())

@router.callback_query(F.data == "msk:root")
async def msk_root(cb: CallbackQuery):
    await safe_edit_text(cb.message, "Раздел «Склад МСК».")
    await safe_edit_reply_markup(cb.message, msk_root_kb())
    await cb.answer()

# ========= lists =========
@router.callback_query(F.data.startswith("msk:list:"))
async def msk_list(cb: CallbackQuery):
    mode = cb.data.split(":")[-1]  # in_ru | to_our | archive
    async with get_session() as s:
        all_rows = (await s.execute(select(MskInboundDoc).order_by(MskInboundDoc.created_at.desc()))).scalars().all()

        # подгружаем коды CN одним запросом
        cn_ids = [r.cn_purchase_id for r in all_rows if r and r.cn_purchase_id]
        cn_map: Dict[int, str] = {}
        if cn_ids:
            cn_rows = (await s.execute(select(CnPurchase.id, CnPurchase.code).where(CnPurchase.id.in_(cn_ids)))).all()
            cn_map = {i: code for i, code in cn_rows}

    if mode == "in_ru":
        rows = [r for r in all_rows if r.status == MskInboundStatus.PENDING and not r.warehouse_id]
        title = "🚚 Доставка в РФ"
    elif mode == "to_our":
        rows = [r for r in all_rows if r.status == MskInboundStatus.PENDING and r.warehouse_id]
        title = "🏢 Доставка на наш склад"
    else:
        rows = [r for r in all_rows if r.status == MskInboundStatus.RECEIVED]
        title = "🗄️ Архив"

    if not rows:
        await safe_edit_text(cb.message, f"{title}\n\nСписок пуст.")
        await safe_edit_reply_markup(cb.message, InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="msk:root")]
        ]))
        await cb.answer()
        return

    kb_rows: list[list[InlineKeyboardButton]] = []
    for r in rows:
        # имя документа: DOCNAME из комментария MSK или код CN
        human = docname_from_text(r.comment) or cn_map.get(r.cn_purchase_id, f"CN#{r.cn_purchase_id}")
        kb_rows.append([InlineKeyboardButton(
            text=f"📦 {human} · MSK #{r.id}",
            callback_data=f"msk:open:{r.id}"
        )])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="msk:root")])

    await safe_edit_text(cb.message, title)
    await safe_edit_reply_markup(cb.message, InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await cb.answer()

# ========= open doc =========
async def _fetch_msk_view(msk_id: int):
    async with get_session() as s:
        msk = await s.get(MskInboundDoc, msk_id)
        items = (await s.execute(select(MskInboundItem).where(MskInboundItem.msk_inbound_id == msk_id))).scalars().all()

        pmap = {}
        if items:
            pids = [it.product_id for it in items]
            prows = (await s.execute(select(Product).where(Product.id.in_(pids)))).scalars().all()
            pmap = {p.id: p for p in prows}

        wh_name = msk.warehouse.name if msk and msk.warehouse else None

        # связанный CN
        cn = await s.get(CnPurchase, msk.cn_purchase_id) if msk else None

    return msk, items, pmap, wh_name, cn

async def render_msk_doc(msg: Message, msk_id: int):
    msk, items, pmap, wh_name, cn = await _fetch_msk_view(msk_id)
    if not msk:
        await safe_edit_text(msg, "Документ не найден или удалён.")
        return

    if msk.status == MskInboundStatus.PENDING and not msk.warehouse_id:
        status_text = "🚚 Доставка в РФ"
    elif msk.status == MskInboundStatus.PENDING and msk.warehouse_id:
        status_text = "🏢 Доставка на наш склад"
    else:
        status_text = "🗄️ Принято (архив)"

    # читаем «человеческое имя»: DOCNAME или код CN
    docname = docname_from_text(msk.comment) or (getattr(cn, "code", None) or f"CN#{msk.cn_purchase_id}")

    lines = [
        f"📦 {docname} · MSK-док #{msk.id}",
        f"Статус: {status_text}",
        f"Склад назначения: {wh_name or '—'}",
        f"💬 Комментарий: {getattr(msk, 'comment', None) or '—'}",
        "",
        "🧱 Позиции:",
    ]
    if not items:
        lines.append("— нет позиций —")
    else:
        for it in items:
            p = pmap.get(it.product_id)
            title = f"{p.name} · {p.article}" if p else f"id={it.product_id}"
            price = f"{(it.unit_cost_rub or 0):.2f}"
            lines.append(f"• {title} — {it.qty} шт. × {price} ₽")

    # Полная хронология (как в CN: 1–6)
    lines += [
        "",
        "🕓 Хронология:",
        f"1) Создан: {fmt_dt(getattr(cn, 'created_at', None))}",
        f"2) Отправлен в карго: {fmt_dt(getattr(cn, 'sent_to_cargo_at', None))}",
        f"3) Переведён в отправку на склад МСК: {fmt_dt(getattr(cn, 'sent_to_msk_at', None))}",
        f"4) Отправлен на региональный склад: {fmt_dt(getattr(msk, 'to_our_at', None))}",
        f"5) Приходован на склад: {fmt_dt(getattr(msk, 'received_at', None))}",
        f"6) Архивирован: {fmt_dt(getattr(cn, 'archived_at', None))}",
    ]

    await safe_edit_text(msg, "\n".join(lines))
    await safe_edit_reply_markup(msg, msk_doc_kb(msk.id, msk.status, msk.warehouse_id, msk.cn_purchase_id))

@router.callback_query(F.data.startswith("msk:open:"))
async def msk_open(cb: CallbackQuery):
    parts = cb.data.split(":")
    # msk:open:by_cn:{cn_id}
    if len(parts) >= 3 and parts[2] == "by_cn":
        cn_id = last_int(cb.data)
        async with get_session() as s:
            msk = (await s.execute(select(MskInboundDoc).where(MskInboundDoc.cn_purchase_id == cn_id))).scalar_one_or_none()
        if not msk:
            await cb.answer("MSK-документ не найден.", show_alert=True)
            return
        msk_id = msk.id
    else:
        msk_id = last_int(cb.data)

    if not msk_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return

    await render_msk_doc(cb.message, msk_id)
    await cb.answer()

# ========= choose target warehouse =========
@router.callback_query(F.data.startswith("msk:to_our:"))
async def msk_to_our(cb: CallbackQuery):
    msk_id = last_int(cb.data)
    if not msk_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return

    async with get_session() as s:
        warehouses = (await s.execute(select(Warehouse).order_by(Warehouse.name.asc()))).scalars().all()
    if not warehouses:
        await cb.answer("Нет доступных складов.", show_alert=True)
        return

    await safe_edit_text(cb.message, "Выберите склад назначения:")
    await safe_edit_reply_markup(cb.message, msk_wh_kb(msk_id, warehouses))
    await cb.answer()

@router.callback_query(F.data.startswith("msk:whchoose:"))
async def msk_whchoose(cb: CallbackQuery):
    msk_id, wh_id = last_two_ints(cb.data)
    if not msk_id or not wh_id:
        await cb.answer("Не удалось определить склад.", show_alert=True)
        return

    async with get_session() as s:
        w = await s.get(Warehouse, wh_id)
        if not w:
            await cb.answer("Склад не найден. Выберите другой.", show_alert=True)
            return

        msk = await s.get(MskInboundDoc, msk_id)
        msk.warehouse_id = wh_id
        if not getattr(msk, "to_our_at", None):
            msk.to_our_at = datetime.utcnow()

        # Архивируем CN при выборе склада (как и раньше)
        cn = await s.get(CnPurchase, msk.cn_purchase_id)
        cn.status = CnPurchaseStatus.DELIVERED_TO_MSK
        if hasattr(cn, "archived_at"):
            cn.archived_at = datetime.utcnow()

        await s.commit()

    await render_msk_doc(cb.message, msk_id)
    await cb.answer("Склад выбран. Теперь нажмите «✅ Принято (оприходовать)».", show_alert=True)

# ========= deliver (create stock movements) =========
@router.callback_query(F.data.startswith("msk:deliver:"))
async def msk_deliver(cb: CallbackQuery):
    msk_id = last_int(cb.data)
    if not msk_id:
        await cb.answer("Не удалось определить документ.", show_alert=True)
        return

    async with get_session() as s:
        msk = await s.get(MskInboundDoc, msk_id)
        if not msk:
            await cb.answer("Документ не найден.", show_alert=True)
            return
        if not msk.warehouse_id:
            await cb.answer("Не выбран склад назначения.", show_alert=True)
            return

        cn = await s.get(CnPurchase, msk.cn_purchase_id) if msk.cn_purchase_id else None
        cn_code = getattr(cn, "code", None)

        db_user = (await s.execute(
            select(User).where(User.telegram_id == cb.from_user.id)
        )).scalar_one_or_none()
        user_id = db_user.id if db_user else None

        items = (await s.execute(
            select(MskInboundItem).where(MskInboundItem.msk_inbound_id == msk_id)
        )).scalars().all()

        if not items:
            await cb.answer("В документе нет позиций.", show_alert=True)
            return

        # имя документа: сначала DOCNAME из комментария MSK, иначе CN-код, иначе MSK #
        docname = docname_from_text(msk.comment) or cn_code or f"MSK#{msk.id}"

        # единый комментарий с маркером DOCNAME
        base_comment = "Оприходовано со склада МСК"
        comment_full = f"[DOCNAME: {docname}] {base_comment}: MSK #{msk.id}" + (f" (из {cn_code})" if cn_code else "")

        now = datetime.utcnow()
        # под одним doc_id — групповое поступление
        # определение doc_id перенесено в СУБД (автоинкремент StockMovement.doc_id отсутствует),
        # поэтому используем «следующий»: max(doc_id)+1 среди прихода.
        max_doc = (await s.execute(
            select(StockMovement.doc_id).where(StockMovement.type == MovementType.prihod).order_by(StockMovement.doc_id.desc())
        )).scalars().first()
        next_doc = (max_doc or 0) + 1

        for it in items:
            s.add(StockMovement(
                type=MovementType.prihod,
                stage=ProductStage.raw,
                qty=it.qty,
                product_id=it.product_id,
                warehouse_id=msk.warehouse_id,
                date=now,
                user_id=user_id,
                doc_id=next_doc,
                comment=comment_full,
            ))

        msk.status = MskInboundStatus.RECEIVED
        msk.received_at = now
        msk.received_by_user_id = user_id

        await s.commit()

    await cb.answer("Принято. Поступление создано, документ перенесён в Архив.", show_alert=True)
    await render_msk_doc(cb.message, msk_id)

```

## Файл: handlers\packing.py

```python
# handlers/packing.py
from __future__ import annotations

import datetime
from typing import Dict, List, Tuple, Union

from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func, and_, desc
from sqlalchemy.orm import aliased

from database.db import get_session
from database.models import (
    User, UserRole,
    Warehouse, Product, StockMovement,
    ProductStage, MovementType,
    PackDoc, PackDocItem,
)
from handlers.common import send_content
from keyboards.inline import warehouses_kb

router = Router()

# сколько товаров показываем на странице при подборе
PAGE_SIZE = 12


class PackFSM(StatesGroup):
    choose_wh = State()
    picking = State()
    input_qty = State()


# ===== ВСПОМОГАТЕЛЬНЫЕ =====

async def _raw_map(session, wh_id: int) -> Dict[int, int]:
    """
    Карта RAW остатков по складу: product_id -> qty (>0)
    """
    SM = aliased(StockMovement)
    rows = await session.execute(
        select(SM.product_id, func.sum(SM.qty).label("qty"))
        .where(and_(SM.warehouse_id == wh_id, SM.stage == ProductStage.raw))
        .group_by(SM.product_id)
        .having(func.sum(SM.qty) > 0)
    )
    return {pid: qty for pid, qty in rows.all()}


async def _next_pack_number(session, wh_id: int) -> str:
    """
    Генерация номера документа: YYYYMMDD-XXX в разрезе склада и дня
    """
    today = datetime.date.today()
    start = datetime.datetime.combine(today, datetime.time.min)
    end = datetime.datetime.combine(today, datetime.time.max)
    last = await session.scalar(
        select(PackDoc.number)
        .where(and_(PackDoc.warehouse_id == wh_id, PackDoc.created_at.between(start, end)))
        .order_by(desc(PackDoc.id))
        .limit(1)
    )
    seq = 1
    if last and "-" in last:
        try:
            seq = int(last.split("-")[-1]) + 1
        except Exception:
            seq = 1
    return f"{today.strftime('%Y%m%d')}-{seq:03d}"


def _cart_summary(cart: Dict[int, int]) -> Tuple[int, int]:
    """
    Возвращает (кол-во позиций, суммарное qty) для корзины
    """
    if not cart:
        return 0, 0
    return len(cart), sum(cart.values())


def _pack_docname(number: str) -> str:
    """
    Единое «человеческое» имя упаковочного документа, попадает в комментарии движений.
    """
    return f"PACK {number}"


# Универсальная inline-кнопка «Назад»
def back_inline_kb(target: str = "back_to_packing") -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data=target)]]
    )


def _kb_picking(
        products_rows: List[Tuple[int, str, str | None, int]],
        page: int,
        pages: int,
        cart_cnt: int,
        cart_sum: int,
) -> types.InlineKeyboardMarkup:
    """
    Клавиатура для страницы подбора: список товаров (RAW>0), пагинация, корзина/назад
    """
    rows: List[List[types.InlineKeyboardButton]] = []

    for pid, name, art, raw_qty in products_rows:
        caption = f"{name} (арт. {art or '—'}) • RAW: {raw_qty}"
        rows.append([types.InlineKeyboardButton(text=caption, callback_data=f"pack_add:{pid}")])

    # пагинация
    if pages > 1:
        prev_cb = f"pack_page:{page-1}" if page > 1 else "noop"
        next_cb = f"pack_page:{page+1}" if page < pages else "noop"
        rows.append([
            types.InlineKeyboardButton(text="◀", callback_data=prev_cb),
            types.InlineKeyboardButton(text=f"{page}/{pages}", callback_data="noop"),
            types.InlineKeyboardButton(text="▶", callback_data=next_cb),
        ])

    # корзина/навигация
    rows.append([
        types.InlineKeyboardButton(text=f"🧾 Корзина ({cart_cnt}/{cart_sum})", callback_data="pack_cart"),
    ])
    rows.append([
        types.InlineKeyboardButton(text="⬅️ Назад к складам", callback_data="pack_back_wh"),
        types.InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_packing"),
    ])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_cart(can_post: bool) -> types.InlineKeyboardMarkup:
    """
    Клавиатура для корзины (редактирование позиций, создание документа)
    """
    rows: List[List[types.InlineKeyboardButton]] = []

    rows.append([types.InlineKeyboardButton(text="➕ Добавить ещё", callback_data="pack_continue")])
    if can_post:
        rows.append([types.InlineKeyboardButton(text="✅ Создать документ", callback_data="pack_post")])
    else:
        rows.append([types.InlineKeyboardButton(text="⛔ Нет позиций", callback_data="noop")])

    rows.append([
        types.InlineKeyboardButton(text="🗑 Очистить", callback_data="pack_clear"),
        types.InlineKeyboardButton(text="⬅️ Назад к подбору", callback_data="pack_continue"),
    ])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_docs(docs_rows: List[Tuple[int, str, datetime.datetime, str, int]]) -> types.InlineKeyboardMarkup:
    """
    Список документов упаковки
    """
    rows: List[List[types.InlineKeyboardButton]] = []
    for did, number, created_at, wh_name, total in docs_rows:
        label = f"№{number} • {created_at:%d.%m %H:%M} • {wh_name} • {total} шт."
        rows.append([types.InlineKeyboardButton(text=label, callback_data=f"pack_doc:{did}")])
    rows.append([types.InlineKeyboardButton(text="⬅️ Назад", callback_data="pack_root")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_picking(target: Union[types.CallbackQuery, types.Message], state: FSMContext):
    """
    Рендер страницы подбора (универсально для CallbackQuery/Message)
    """
    data = await state.get_data()
    wh_name: str = data["wh_name"]
    page: int = int(data.get("page", 1))
    cart: Dict[int, int] = data.get("cart", {})
    raw_map: Dict[int, int] = data["raw_map"]
    products: List[Tuple[int, str, str | None]] = data["products"]

    pages = max(1, (len(products) + PAGE_SIZE - 1) // PAGE_SIZE)
    start, end = (page - 1) * PAGE_SIZE, (page - 1) * PAGE_SIZE + PAGE_SIZE
    slice_rows = [(pid, name, art, raw_map.get(pid, 0)) for (pid, name, art) in products[start:end]]

    cnt, summ = _cart_summary(cart)
    text = f"🏬 *{wh_name}*\nВыберите товар для упаковки (RAW > 0).\n\n🧾 Корзина: {cnt} поз., {summ} шт."

    await send_content(
        target,
        text,
        parse_mode="Markdown",
        reply_markup=_kb_picking(slice_rows, page, pages, cnt, summ),
    )


# ===== ROOT / МЕНЮ =====

@router.callback_query(F.data == "packing")
async def pack_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await state.clear()
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🆕 Новая упаковка", callback_data="pack_new")],
        [types.InlineKeyboardButton(text="🏷 Документы упаковки", callback_data="pack_docs")],
        [types.InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")],
    ])
    await send_content(cb, "Упаковка — выберите действие:", reply_markup=kb)


# ===== СОЗДАНИЕ НОВОЙ УПАКОВКИ =====

@router.callback_query(F.data == "pack_new")
async def pack_new(cb: types.CallbackQuery, user: User, state: FSMContext):
    await state.clear()
    async with get_session() as session:
        wh = (await session.execute(
            select(Warehouse).where(Warehouse.is_active == True).order_by(Warehouse.name)
        )).scalars().all()
    if not wh:
        return await send_content(cb, "🚫 Нет активных складов.")
    await state.set_state(PackFSM.choose_wh)
    await send_content(cb, "Выберите склад для новой упаковки:", reply_markup=warehouses_kb(wh, prefix="pack_wh"))


@router.callback_query(F.data.startswith("pack_wh:"))
async def pack_choose_wh(cb: types.CallbackQuery, user: User, state: FSMContext):
    # фикс состояния
    if await state.get_state() != PackFSM.choose_wh:
        await state.set_state(PackFSM.choose_wh)

    wh_id = int(cb.data.split(":")[1])
    async with get_session() as session:
        wh = await session.get(Warehouse, wh_id)
        if not wh or not wh.is_active:
            return await send_content(cb, "🚫 Склад не найден или неактивен.")
        raw = await _raw_map(session, wh_id)
        if not raw:
            return await send_content(cb, f"На складе *{wh.name}* нет RAW остатков.", parse_mode="Markdown")
        prod_rows = (await session.execute(
            select(Product.id, Product.name, Product.article)
            .where(and_(Product.is_active == True, Product.id.in_(raw.keys())))
            .order_by(Product.article)
        )).all()

    await state.update_data(
        wh_id=wh_id,
        wh_name=wh.name,
        page=1,
        cart={},
        raw_map=raw,
        products=prod_rows,
    )
    await state.set_state(PackFSM.picking)
    await _render_picking(cb, state)


@router.callback_query(F.data.startswith("pack_page:"))
async def pack_page(cb: types.CallbackQuery, state: FSMContext):
    page = int(cb.data.split(":")[1])
    await state.update_data(page=page)
    await _render_picking(cb, state)


@router.callback_query(F.data.startswith("pack_add:"))
async def pack_add(cb: types.CallbackQuery, state: FSMContext):
    """
    Клик по товару — запрос количества
    """
    pid = int(cb.data.split(":")[1])
    data = await state.get_data()
    raw_map: Dict[int, int] = data["raw_map"]
    can = int(raw_map.get(pid, 0))
    if can <= 0:
        return await cb.answer("Нет RAW остатка", show_alert=True)
    await state.update_data(current_pid=pid, current_can=can)
    await cb.message.answer(f"Введите количество для упаковки (доступно RAW: {can})")
    await state.set_state(PackFSM.input_qty)


@router.message(PackFSM.input_qty)
async def pack_input_qty(msg: types.Message, state: FSMContext):
    """
    Обработка ручного ввода qty и возврат в подбор со свежей корзиной
    """
    try:
        qty = int(msg.text.strip())
        if qty <= 0:
            raise ValueError
    except Exception:
        return await msg.answer("Введите целое положительное число.")

    data = await state.get_data()
    pid = data["current_pid"]
    can = data["current_can"]
    if qty > can:
        return await msg.answer(f"Недостаточно RAW. Доступно: {can}")

    cart: Dict[int, int] = data.get("cart", {})
    cart[pid] = cart.get(pid, 0) + qty

    raw_map: Dict[int, int] = data["raw_map"]
    raw_map[pid] = can - qty

    await state.update_data(cart=cart, raw_map=raw_map)
    await state.set_state(PackFSM.picking)

    await msg.answer("Добавлено ✅")
    await _render_picking(msg, state)


# ===== КОРЗИНА И РЕДАКТИРОВАНИЕ =====

@router.callback_query(F.data == "pack_cart")
async def pack_cart(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    if not cart:
        return await cb.answer("Корзина пуста", show_alert=True)

    async with get_session() as session:
        rows = (await session.execute(
            select(Product.id, Product.name, Product.article).where(Product.id.in_(cart.keys()))
        )).all()
    info = {pid: (name, art) for pid, name, art in rows}

    lines = ["🧾 *Подбор упаковки*:", ""]
    total = 0
    kb_rows: List[List[types.InlineKeyboardButton]] = []
    idx = 1
    for pid, q in cart.items():
        name, art = info.get(pid, ("?", None))
        lines.append(f"{idx}) `{art or pid}` — *{name}*: **{q}** шт.")
        kb_rows.append([
            types.InlineKeyboardButton(text="➖1", callback_data=f"pack_dec:{pid}"),
            types.InlineKeyboardButton(text="➕1", callback_data=f"pack_inc:{pid}"),
            types.InlineKeyboardButton(text="❌", callback_data=f"pack_del:{pid}"),
        ])
        total += q
        idx += 1

    lines += ["", f"📈 Итого: {len(cart)} позиций, {total} шт."]

    # общие кнопки
    kb_rows.append([types.InlineKeyboardButton(text="➕ Добавить ещё", callback_data="pack_continue")])
    if total > 0:
        kb_rows.append([types.InlineKeyboardButton(text="✅ Создать документ", callback_data="pack_post")])
    kb_rows.append([
        types.InlineKeyboardButton(text="🗑 Очистить", callback_data="pack_clear"),
        types.InlineKeyboardButton(text="⬅️ Назад к подбору", callback_data="pack_continue"),
    ])
    kb = types.InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await send_content(cb, "\n".join(lines), parse_mode="Markdown", reply_markup=kb)


@router.callback_query(F.data.startswith("pack_inc:"))
async def pack_inc(cb: types.CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    raw_map: Dict[int, int] = data.get("raw_map", {})
    can_left = int(raw_map.get(pid, 0))
    if can_left <= 0:
        return await cb.answer("Нет RAW для увеличения", show_alert=True)
    cart[pid] = cart.get(pid, 0) + 1
    raw_map[pid] = can_left - 1
    await state.update_data(cart=cart, raw_map=raw_map)
    await pack_cart(cb, state)


@router.callback_query(F.data.startswith("pack_dec:"))
async def pack_dec(cb: types.CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    q = cart.get(pid, 0)
    if q <= 0:
        return await cb.answer("Эта позиция уже 0", show_alert=True)
    cart[pid] = q - 1
    # возвращаем RAW доступность
    raw_map: Dict[int, int] = data.get("raw_map", {})
    raw_map[pid] = raw_map.get(pid, 0) + 1
    if cart[pid] == 0:
        del cart[pid]
    await state.update_data(cart=cart, raw_map=raw_map)
    await pack_cart(cb, state)


@router.callback_query(F.data.startswith("pack_del:"))
async def pack_del(cb: types.CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    q = cart.pop(pid, 0)
    raw_map: Dict[int, int] = data.get("raw_map", {})
    raw_map[pid] = raw_map.get(pid, 0) + q
    await state.update_data(cart=cart, raw_map=raw_map)
    await cb.answer("Удалено")
    await pack_cart(cb, state)


@router.callback_query(F.data == "pack_clear")
async def pack_clear(cb: types.CallbackQuery, state: FSMContext):
    """
    Очистить корзину и пересчитать доступный RAW из базы
    """
    data = await state.get_data()
    async with get_session() as session:
        raw = await _raw_map(session, data["wh_id"])
    await state.update_data(cart={}, raw_map=raw)
    await cb.answer("Корзина очищена")
    await _render_picking(cb, state)


@router.callback_query(F.data == "pack_continue")
async def pack_continue(cb: types.CallbackQuery, state: FSMContext):
    await _render_picking(cb, state)


# ===== СОЗДАНИЕ ДОКУМЕНТА (ПРОВЕДЕНИЕ) =====

@router.callback_query(F.data == "pack_post")
async def pack_post(cb: types.CallbackQuery, user: User, state: FSMContext):
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    if not cart:
        return await cb.answer("Корзина пуста", show_alert=True)
    wh_id = data["wh_id"]

    async with get_session() as session:
        number = await _next_pack_number(session, wh_id)
        doc = PackDoc(number=number, warehouse_id=wh_id, user_id=user.id)
        session.add(doc)
        await session.flush()  # получаем doc.id

        docname = _pack_docname(number)

        # позиции документа + движения
        for pid, qty in cart.items():
            session.add(PackDocItem(doc_id=doc.id, product_id=pid, qty=qty))
            # raw -
            session.add(StockMovement(
                type=MovementType.upakovka, stage=ProductStage.raw, qty=-qty,
                product_id=pid, warehouse_id=wh_id, doc_id=doc.id,
                comment=f"[DOCNAME: {docname}] Упаковка: списание RAW по PACK №{number}",
            ))
            # packed +
            session.add(StockMovement(
                type=MovementType.upakovka, stage=ProductStage.packed, qty=qty,
                product_id=pid, warehouse_id=wh_id, doc_id=doc.id,
                comment=f"[DOCNAME: {docname}] Упаковка: оприходование PACKED по PACK №{number}",
            ))

        # помечаем документ как проведённый
        doc.status = "posted"
        await session.commit()

    await state.clear()
    await send_content(cb, f"✅ Документ упаковки создан: *№{number}*.", parse_mode="Markdown")
    await _show_doc(cb, doc_id=None, number=number)


async def _show_doc(cb: types.CallbackQuery, doc_id: int | None = None, number: str | None = None):
    """
    Карточка документа упаковки
    """
    async with get_session() as session:
        if doc_id:
            doc = await session.get(PackDoc, doc_id)
        else:
            doc = (await session.execute(select(PackDoc).where(PackDoc.number == number))).scalar_one()
        wh = await session.get(Warehouse, doc.warehouse_id)
        items = (await session.execute(
            select(PackDocItem, Product.name, Product.article)
            .join(Product, Product.id == PackDocItem.product_id)
            .where(PackDocItem.doc_id == doc.id)
            .order_by(Product.article)
        )).all()

    total = sum(i.PackDocItem.qty for i in items)
    lines = [
        f"🏷 Документ упаковки *№{doc.number}* от {doc.created_at:%d.%m.%Y %H:%M}",
        f"Склад: *{wh.name}*",
        f"Статус: *{doc.status}*",
        "",
        "Состав:"
    ]
    for idx, row in enumerate(items, start=1):
        it = row.PackDocItem
        name, art = row.name, row.article
        lines.append(f"{idx}) `{art or it.product_id}` — *{name}*: **{it.qty}** шт.")
    lines += ["", f"📈 Итого: {len(items)} позиций, {total} шт."]

    kb_rows = [[types.InlineKeyboardButton(text="⬅️ К списку документов", callback_data="pack_docs")]]
    kb = types.InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await send_content(cb, "\n".join(lines), parse_mode="Markdown", reply_markup=kb)


# ===== СПИСОК ДОКУМЕНТОВ =====

@router.callback_query(F.data == "pack_docs")
async def pack_docs(cb: types.CallbackQuery, state: FSMContext):
    async with get_session() as session:
        rows = (await session.execute(
            select(
                PackDoc.id, PackDoc.number, PackDoc.created_at, Warehouse.name,
                func.coalesce(func.sum(PackDocItem.qty), 0).label("total")
            )
            .join(Warehouse, Warehouse.id == PackDoc.warehouse_id)
            .join(PackDocItem, PackDocItem.doc_id == PackDoc.id)
            .group_by(PackDoc.id, Warehouse.name)
            .order_by(desc(PackDoc.created_at))
            .limit(20)
        )).all()

    if not rows:
        kb = types.InlineKeyboardMarkup(
            inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data="pack_root")]]
        )
        return await send_content(cb, "Документов упаковки пока нет.", reply_markup=kb)

    await send_content(cb, "Последние документы упаковки:", reply_markup=_kb_docs(rows))


@router.callback_query(F.data.startswith("pack_doc:"))
async def pack_doc_open(cb: types.CallbackQuery, state: FSMContext):
    did = int(cb.data.split(":")[1])
    await _show_doc(cb, doc_id=did)


# ===== НАВИГАЦИЯ =====

@router.callback_query(F.data == "pack_back_wh")
async def pack_back_wh(cb: types.CallbackQuery, state: FSMContext):
    # заново начало флоу выбора склада
    await pack_new(cb, user=None, state=state)  # user в pack_new не используется


# Локальный обработчик «Назад» для упаковки
@router.callback_query(F.data == "back_to_packing")
async def back_to_packing(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await cb.message.edit_reply_markup()
    except Exception:
        pass
    await cb.message.answer("Раздел «Упаковка». Выберите действие:", reply_markup=back_inline_kb("back_to_menu"))
    await cb.answer()

```

## Файл: handlers\receiving.py

```python
# handlers/receiving.py
from __future__ import annotations

import re
from typing import Optional

from aiogram import Dispatcher, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func, desc
from database.models import ProductStage
from html import escape as h  # для безопасной разметки HTML

from database.db import get_session
from database.models import User, Warehouse, Product, StockMovement, MovementType
from keyboards.inline import (
    warehouses_kb, products_page_kb, qty_kb, comment_kb, receiving_confirm_kb
)
from handlers.common import send_content
from utils.validators import validate_positive_int
from utils.pagination import build_pagination_keyboard


class IncomingState(StatesGroup):
    choosing_warehouse = State()
    choosing_product = State()
    entering_qty = State()
    entering_comment = State()
    confirming = State()


class ReceivingViewState(StatesGroup):
    viewing_docs = State()


PAGE_SIZE_PRODUCTS = 10
PAGE_SIZE_DOCS = 10

# ===== Имя документа из комментария =====
_DOCNAME_RE = re.compile(r"\[(?:DOCNAME|NAME)\s*:\s*([^\]]+)\]", re.IGNORECASE)
_CN_CODE_RE = re.compile(r"(CN-\d{8}-\d{6})", re.IGNORECASE)  # fallback: распознать CN-код

def _doc_label(doc_id: int, comment: Optional[str]) -> str:
    """
    Возвращает «человеческое» имя документа для заголовка/списка.
    1) [DOCNAME: ...] из комментария;
    2) если нет — пытаемся найти CN-код в комментарии;
    3) иначе — «№<doc_id>».
    """
    if comment:
        m = _DOCNAME_RE.search(comment)
        if m:
            return m.group(1).strip()
        m2 = _CN_CODE_RE.search(comment)
        if m2:
            return m2.group(1).upper()
    return f"№{doc_id}"


def kb_receiving_root():
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📄 Просмотреть документы", callback_data="view_docs")],
        [types.InlineKeyboardButton(text="➕ Добавить документ", callback_data="add_doc")],
        [types.InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")],
    ])


# ===== Корневое меню для "Поступление" =====
async def receiving_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()
    await send_content(cb, "Поступление товара: выберите действие", reply_markup=kb_receiving_root())


# ===== Просмотреть документы =====
async def view_docs(cb: types.CallbackQuery, user: User, state: FSMContext, page: int = 1):
    await cb.answer()
    await state.set_state(ReceivingViewState.viewing_docs)

    async with get_session() as session:
        total_stmt = select(func.count(func.distinct(StockMovement.doc_id))).where(
            StockMovement.type == MovementType.prihod
        )
        total = await session.scalar(total_stmt)

        res = await session.execute(
            select(
                StockMovement.doc_id,
                func.min(StockMovement.date).label("date"),
                func.max(StockMovement.comment).label("comment"),  # берём любой комментарий из группы
            )
            .where(StockMovement.type == MovementType.prihod)
            .group_by(StockMovement.doc_id)
            .order_by(desc("date"))
            .offset((page - 1) * PAGE_SIZE_DOCS)
            .limit(PAGE_SIZE_DOCS)
        )
        docs = res.all()

    if not docs:
        await send_content(
            cb,
            "📭 Документов по поступлению пока нет.",
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data="receiving")]]
            ),
        )
        return

    rows = []
    for row in docs:
        doc_id = row.doc_id
        date_str = row.date.strftime("%Y-%m-%d %H:%M")
        human = _doc_label(doc_id, row.comment)
        rows.append([types.InlineKeyboardButton(
            text=f"Документ {human} от {date_str}",
            callback_data=f"view_doc:{doc_id}"
        )])

    pag_row = build_pagination_keyboard(
        page=page,
        page_size=PAGE_SIZE_DOCS,
        total=total,
        prev_cb_prefix="view_docs_page",
        next_cb_prefix="view_docs_page",
        prev_text="◀ Предыдущая",
        next_text="Следующая ▶"
    )
    if pag_row:
        rows.append(pag_row)

    rows.append([types.InlineKeyboardButton(text="⬅️ Назад", callback_data="receiving")])

    kb = types.InlineKeyboardMarkup(inline_keyboard=rows)
    await send_content(cb, "Документы по поступлению:", reply_markup=kb)


async def view_docs_page(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    try:
        _, page_str = cb.data.split(":")
        page = int(page_str)
    except Exception:
        page = 1
    await view_docs(cb, user, state, page=page)


# ===== Просмотр конкретного документа =====
async def view_doc(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    try:
        _, doc_id_str = cb.data.split(":")
        doc_id = int(doc_id_str)
    except Exception:
        await cb.answer("Неверные данные.", show_alert=True)
        return

    async with get_session() as session:
        res = await session.execute(
            select(StockMovement, Warehouse, Product, User)
            .join(Warehouse, Warehouse.id == StockMovement.warehouse_id)
            .join(Product, Product.id == StockMovement.product_id)
            .join(User, User.id == StockMovement.user_id)
            .where(StockMovement.doc_id == doc_id, StockMovement.type == MovementType.prihod)
            .order_by(StockMovement.id)
        )
        movements = res.all()

    if not movements:
        await send_content(cb, "Документ не найден.")
        return

    first_mv: StockMovement = movements[0][0]
    human = _doc_label(doc_id, first_mv.comment)
    header = f"📑 <b>Документ {h(human)} от {h(first_mv.date.strftime('%Y-%m-%d %H:%M:%S'))}</b>\n\n"

    parts = [header]
    for mv, wh, prod, usr in movements:
        parts.append(
            "🏬 Склад: <b>{wh}</b>\n"
            "📦 Товар: <b>{prod}</b> (арт. <code>{art}</code>)\n"
            "➡️ Количество: <b>{qty}</b> шт.\n"
            "💬 Комментарий: {comment}\n"
            "👤 Создал: <b>{user}</b>\n"
            .format(
                wh=h(wh.name),
                prod=h(prod.name),
                art=h(prod.article),
                qty=h(str(mv.qty)),
                comment=h(mv.comment or "—"),
                user=h(usr.name or str(usr.id)),
            )
        )
        parts.append("")  # пустая строка между позициями

    text = "\n".join(parts).strip()

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к документам", callback_data="view_docs")],
    ])
    await send_content(cb, text, reply_markup=kb, parse_mode="HTML")


# ===== Добавить документ (текущий флоу) =====
async def add_doc(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    async with get_session() as session:
        res = await session.execute(
            select(Warehouse).where(Warehouse.is_active == True).order_by(Warehouse.name)
        )
        warehouses = res.scalars().all()
    if not warehouses:
        await send_content(cb, "🚫 Нет активных складов. Обратитесь к администратору.")
        await state.clear()
        return

    await state.clear()
    await state.set_state(IncomingState.choosing_warehouse)
    await send_content(cb, "🏬 Выберите склад для поступления товара:",
                       reply_markup=warehouses_kb(warehouses))


# ===== Выбор склада -> сразу список активных товаров =====
async def pick_warehouse(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if not cb.data.startswith("rcv_wh:"):
        return

    wh_id = int(cb.data.split(":")[1])
    async with get_session() as session:
        warehouse = (await session.execute(
            select(Warehouse).where(Warehouse.id == wh_id, Warehouse.is_active == True)
        )).scalar()
    if not warehouse:
        await cb.message.answer("🚫 Склад не найден или неактивен.")
        return

    await state.update_data(warehouse_id=warehouse.id, warehouse_name=warehouse.name)
    await list_products(cb, user, state, page=1)  # Переход к выбору товара


# ===== Назад к выбору склада =====
async def back_to_warehouses(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    async with get_session() as session:
        res = await session.execute(
            select(Warehouse).where(Warehouse.is_active == True).order_by(Warehouse.name)
        )
        warehouses = res.scalars().all()
    await state.set_state(IncomingState.choosing_warehouse)
    await send_content(cb, "🏬 Выберите склад:", reply_markup=warehouses_kb(warehouses))


# ===== Список товаров с пагинацией =====
async def list_products(cb: types.CallbackQuery, user: User, state: FSMContext, page: int = 1):
    async with get_session() as session:
        total = (await session.execute(
            select(func.count()).select_from(Product).where(Product.is_active == True)
        )).scalar_one()
        res = await session.execute(
            select(Product)
            .where(Product.is_active == True)
            .order_by(Product.name)
            .offset((page - 1) * PAGE_SIZE_PRODUCTS)
            .limit(PAGE_SIZE_PRODUCTS)
        )
        products = res.scalars().all()

    if not products:
        await send_content(cb, "🚫 Активных товаров нет. Попросите администратора добавить товар.",
                           reply_markup=warehouses_kb([]))
        return

    await state.set_state(IncomingState.choosing_product)
    await send_content(
        cb,
        "📦 Выберите товар:",
        reply_markup=products_page_kb(products, page, PAGE_SIZE_PRODUCTS, total, back_to="rcv_back_wh")
    )


async def products_page(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if not cb.data.startswith("rcv_prod_page:"):
        return
    try:
        _, page_str = cb.data.split(":")
        page = int(page_str)
    except Exception:
        page = 1
    await list_products(cb, user, state, page=page)


# ===== Выбор товара из списка =====
async def pick_product(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if not cb.data.startswith("rcv_prod:"):
        return
    try:
        _, pid_str = cb.data.split(":")
        pid = int(pid_str)
    except Exception:
        await cb.answer("🚫 Неверные данные.", show_alert=True)
        return

    async with get_session() as session:
        product = (await session.execute(
            select(Product).where(Product.id == pid, Product.is_active == True)
        )).scalar()

    if not product:
        await cb.answer("🚫 Товар не найден/неактивен.", show_alert=True)
        return

    await state.update_data(product_id=product.id, product_article=product.article, product_name=product.name)
    await state.set_state(IncomingState.entering_qty)
    await send_content(
        cb,
        f"📦 Товар: <b>{h(product.name)}</b> (арт. <code>{h(product.article)}</code>)\n\n➡️ Введите количество (&gt;0):",
        reply_markup=qty_kb(back_to="rcv_back_products"),
        parse_mode="HTML",
    )


# ===== Назад к списку товаров =====
async def back_to_products(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await list_products(cb, user, state, page=1)


# ===== Количество =====
async def enter_qty(message: types.Message, user: User, state: FSMContext):
    txt = (message.text or "").strip()
    try:
        qty = int(txt)
    except Exception:
        await message.answer("🚫 Нужно целое число. Введите ещё раз:",
                             reply_markup=qty_kb(back_to="rcv_back_products"))
        return
    if not validate_positive_int(qty):
        await message.answer("🚫 Количество должно быть > 0. Введите ещё раз:",
                             reply_markup=qty_kb(back_to="rcv_back_products"))
        return

    await state.update_data(qty=qty)
    await state.set_state(IncomingState.entering_comment)
    await message.answer(
        "💬 Комментарий (или нажмите «Пропустить»):",
        reply_markup=comment_kb(back_to="rcv_back_qty")
    )


# ===== Комментарий =====
async def skip_comment(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    await state.update_data(comment="")
    await state.set_state(IncomingState.confirming)
    await send_content(
        cb,
        confirm_text(data),
        reply_markup=receiving_confirm_kb(confirm_prefix="rcv", back_to="rcv_back_comment"),
        parse_mode="HTML",
    )


async def back_to_qty(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    await state.set_state(IncomingState.entering_qty)
    await send_content(
        cb,
        f"📦 Товар: <b>{h(str(data['product_name']))}</b> (арт. <code>{h(str(data['product_article']))}</code>)\n\n➡️ Введите количество (&gt;0):",
        reply_markup=qty_kb(back_to="rcv_back_products"),
        parse_mode="HTML",
    )


async def set_comment(message: types.Message, user: User, state: FSMContext):
    comment = (message.text or "").strip()
    if comment == "-":
        comment = ""
    data = await state.get_data()
    await state.update_data(comment=comment)
    await state.set_state(IncomingState.confirming)
    await message.answer(
        confirm_text({**data, "comment": comment}),
        reply_markup=receiving_confirm_kb(confirm_prefix="rcv", back_to="rcv_back_comment"),
        parse_mode="HTML",
    )


def confirm_text(data: dict) -> str:
    return (
        "📑 <b>Подтвердите поступление:</b>\n\n"
        f"🏬 Склад: <b>{h(str(data['warehouse_name']))}</b>\n"
        f"📦 Товар: <b>{h(str(data['product_name']))}</b> (арт. <code>{h(str(data['product_article']))}</code>)\n"
        f"➡️ Количество: <b>{h(str(data['qty']))}</b>\n"
        f"💬 Комментарий: {h(data.get('comment') or '—')}\n"
    )


async def back_to_comment(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.set_state(IncomingState.entering_comment)
    await send_content(cb, "💬 Комментарий (или нажмите «Пропустить»):",
                       reply_markup=comment_kb(back_to="rcv_back_qty"))


# ===== Отмена в любом месте =====
async def cancel_flow(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer("🚫 Отмена")
    await state.clear()
    await send_content(cb, "Операция отменена.", reply_markup=kb_receiving_root())


# ===== Подтверждение и запись =====
async def confirm(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    if cb.data not in ("rcv_confirm", "rcv_cancel"):
        return
    if cb.data == "rcv_cancel":
        await state.clear()
        await send_content(cb, "🚫 Операция отменена.", reply_markup=kb_receiving_root())
        return

    data = await state.get_data()
    async with get_session() as session:
        max_doc = (await session.execute(
            select(func.max(StockMovement.doc_id)).where(StockMovement.type == MovementType.prihod)
        )).scalar()
        next_doc = (max_doc or 0) + 1

        sm = StockMovement(
            warehouse_id=data["warehouse_id"],
            product_id=data["product_id"],
            qty=data["qty"],
            type=MovementType.prihod,
            stage=ProductStage.raw,
            user_id=user.id,
            doc_id=next_doc,
            comment=data.get("comment", ""),
        )
        session.add(sm)
        await session.commit()
        await session.refresh(sm)

    await state.clear()
    done = (
        f"✅ <b>Поступление записано.</b>\n\n"
        f"📑 Документ <b>{h(_doc_label(sm.doc_id, sm.comment))}</b>\n"
        f"📅 Дата: <b>{h(sm.date.strftime('%Y-%m-%d %H:%M:%S'))}</b>\n"
        f"🏬 Склад: <b>{h(str(data['warehouse_name']))}</b>\n"
        f"📦 Товар: <b>{h(str(data['product_name']))}</b> (арт. <code>{h(str(data['product_article']))}</code>)\n"
        f"➡️ Количество: <b>{h(str(data['qty']))}</b>\n"
        f"💬 Комментарий: {h(data.get('comment') or '—')}"
    )
    await send_content(cb, done, reply_markup=kb_receiving_root(), parse_mode="HTML")


def register_receiving_handlers(dp: Dispatcher):
    # Корень
    dp.callback_query.register(receiving_root, lambda c: c.data == "receiving")

    # Просмотр документов
    dp.callback_query.register(view_docs, lambda c: c.data == "view_docs")
    dp.callback_query.register(view_docs_page, lambda c: c.data.startswith("view_docs_page:"))
    dp.callback_query.register(view_doc, lambda c: c.data.startswith("view_doc:"))

    # Добавить документ
    dp.callback_query.register(add_doc, lambda c: c.data == "add_doc")

    # Склад -> сразу товары
    dp.callback_query.register(pick_warehouse, lambda c: c.data.startswith("rcv_wh:"))
    dp.callback_query.register(back_to_warehouses, lambda c: c.data == "rcv_back_wh")

    # Товары и пагинация
    dp.callback_query.register(products_page, lambda c: c.data.startswith("rcv_prod_page:"))
    dp.callback_query.register(pick_product, lambda c: c.data.startswith("rcv_prod:"))
    dp.callback_query.register(back_to_products, lambda c: c.data == "rcv_back_products")

    # Комментарий/Qty/Отмена/Назад
    dp.callback_query.register(skip_comment, lambda c: c.data == "rcv_skip_comment")
    dp.callback_query.register(back_to_qty, lambda c: c.data == "rcv_back_qty")
    dp.callback_query.register(back_to_comment, lambda c: c.data == "rcv_back_comment")
    dp.callback_query.register(cancel_flow, lambda c: c.data == "rcv_cancel")

    # Вводы
    dp.message.register(enter_qty, IncomingState.entering_qty)
    dp.message.register(set_comment, IncomingState.entering_comment)

    # Подтверждение
    dp.callback_query.register(confirm, IncomingState.confirming)

```

## Файл: handlers\reports.py

```python
# handlers/reports.py
from aiogram import Dispatcher, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func, and_
from sqlalchemy.orm import aliased

from database.db import get_session
from database.models import User, Warehouse, Product, StockMovement, ProductStage
from handlers.common import send_content
from keyboards.inline import warehouses_kb, products_page_kb

PAGE_SIZE_REPORTS = 15


# ===== FSM =====
class ReportState(StatesGroup):
    warehouse_selected = State()  # держим wh_id и wh_name
    choosing_article = State()


# ===== Общие помощники =====
def split_message(text: str, max_len: int = 4000) -> list[str]:
    """Разбивает длинный текст по строкам, чтобы не упереться в лимит Телеграма."""
    parts = []
    while len(text) > max_len:
        split_at = text.rfind('\n', 0, max_len)
        if split_at == -1:
            split_at = max_len
        parts.append(text[:split_at].strip())
        text = text[split_at:].strip()
    if text:
        parts.append(text)
    return parts


def kb_reports_root():
    """Корень раздела «Отчёты»."""
    kb = [
        [types.InlineKeyboardButton(text="📦 Остатки по складу", callback_data="rep_view")],
        # здесь в будущем можно добавить другие виды отчётов
    ]
    kb.append([types.InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")])
    return types.InlineKeyboardMarkup(inline_keyboard=kb)


def kb_report_type():
    """Клавиатура выбора типа отчёта внутри выбранного склада."""
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📊 Отчёт по всем товарам", callback_data="rep_all")],
        [types.InlineKeyboardButton(text="🎁 Упакованные остатки", callback_data="rep_packed")],
        [types.InlineKeyboardButton(text="🔍 Отчёт по артикулу", callback_data="rep_article")],
        [types.InlineKeyboardButton(text="⬅️ Назад к складам", callback_data="rep_back_to_wh")],
        [types.InlineKeyboardButton(text="⬅️ В раздел «Отчёты»", callback_data="reports")],
    ])


# ===== Корень «Отчёты» =====
async def reports_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()
    await send_content(
        cb,
        "Раздел «Отчёты». Что нужно показать?",
        reply_markup=kb_reports_root(),
    )


# ===== Просмотр остатков: выбор склада =====
async def rep_view(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()

    async with get_session() as session:
        res = await session.execute(
            select(Warehouse).where(Warehouse.is_active == True).order_by(Warehouse.name)
        )
        warehouses = res.scalars().all()
    if not warehouses:
        await send_content(cb, "🚫 Нет активных складов.")
        return

    await send_content(
        cb,
        "🏬 Выберите склад для отчёта по остаткам:",
        reply_markup=warehouses_kb(warehouses, prefix="rep_wh"),
    )


# ===== Выбор склада -> меню типа отчёта =====
async def rep_pick_warehouse(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("rep_wh:"):
        return
    await cb.answer()

    wh_id = int(cb.data.split(":")[1])
    async with get_session() as session:
        warehouse = await session.get(Warehouse, wh_id)
        if not warehouse or not warehouse.is_active:
            await send_content(cb, "🚫 Склад не найден или неактивен.")
            return

    await state.set_state(ReportState.warehouse_selected)
    await state.update_data(wh_id=wh_id, wh_name=warehouse.name)
    await send_content(
        cb,
        f"🏬 Склад: *{warehouse.name}*. Выберите тип отчёта:",
        reply_markup=kb_report_type(),
        parse_mode="Markdown",
    )


# ===== Отчёт по всем товарам =====
async def rep_all(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    wh_id = data.get('wh_id')
    wh_name = data.get('wh_name')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    SM = aliased(StockMovement)
    async with get_session() as session:
        res = await session.execute(
            select(
                Product.article,
                Product.name,
                func.sum(SM.qty).label("balance")
            )
            .join(SM, and_(SM.product_id == Product.id, SM.warehouse_id == wh_id))
            .where(Product.is_active == True)
            .group_by(Product.id)
            .having(func.sum(SM.qty) > 0)
            .order_by(Product.article)
        )
        rows = res.all()

    if not rows:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="rep_back_to_types")],
            [types.InlineKeyboardButton(text="🏬 Выбор склада", callback_data="rep_back_to_wh")],
        ])
        await send_content(
            cb,
            f"📉 На складе *{wh_name}* сейчас нет товаров с остатком.\n\n"
            f"Выберите другой тип отчёта или склад.",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    total_items = len(rows)
    total_balance = sum(row.balance for row in rows)
    lines = [f"🔹 `{row.article}` — *{row.name}*: **{row.balance}** шт." for row in rows]
    text = (
            f"📊 **Остатки на складе {wh_name}** — товары с остатком:\n\n"
            + "\n\n".join(lines)
            + f"\n\n📈 **Итого:** {total_items} товаров, суммарный остаток: **{total_balance}** шт."
    )
    for i, part in enumerate(split_message(text), 1):
        if len(split_message(text)) > 1:
            part = f"Часть {i}/{len(split_message(text))}:\n\n{part}"
        await cb.message.answer(part, parse_mode="Markdown")

    kb_back = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="rep_back_to_types")],
    ])
    await cb.message.answer("Выберите дальнейшее действие:", reply_markup=kb_back)


# ===== Отчёт об упакованных остатках =====
async def rep_packed(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    wh_id = data.get('wh_id')
    wh_name = data.get('wh_name')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    SM = aliased(StockMovement)
    async with get_session() as session:
        res = await session.execute(
            select(
                Product.article,
                Product.name,
                func.sum(SM.qty).label("balance")
            )
            .join(SM, and_(
                SM.product_id == Product.id,
                SM.warehouse_id == wh_id,
                SM.stage == ProductStage.packed
            ))
            .where(Product.is_active == True)
            .group_by(Product.id)
            .having(func.sum(SM.qty) > 0)
            .order_by(Product.article)
        )
        rows = res.all()

    if not rows:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="rep_back_to_types")],
            [types.InlineKeyboardButton(text="🏬 Выбор склада", callback_data="rep_back_to_wh")],
        ])
        await send_content(
            cb,
            f"📭 На складе *{wh_name}* нет упакованных остатков.",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    total_items = len(rows)
    total_balance = sum(row.balance for row in rows)
    lines = [f"🎁 `{row.article}` — *{row.name}*: **{row.balance}** шт." for row in rows]
    text = (
            f"🎁 **Упакованные остатки на складе {wh_name}**\n\n"
            + "\n\n".join(lines)
            + f"\n\n📈 **Итого:** {total_items} товаров, упаковано: **{total_balance}** шт."
    )
    for i, part in enumerate(split_message(text), 1):
        if len(split_message(text)) > 1:
            part = f"Часть {i}/{len(split_message(text))}:\n\n{part}"
        await cb.message.answer(part, parse_mode="Markdown")

    kb_back = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="rep_back_to_types")],
    ])
    await cb.message.answer("Выберите дальнейшее действие:", reply_markup=kb_back)


# ===== Отчёт по артикулу (список) =====
async def rep_article(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.set_state(ReportState.choosing_article)
    await rep_articles_page(cb, user, state, page=1)


async def rep_articles_page(cb: types.CallbackQuery, user: User, state: FSMContext, page: int = 1):
    data = await state.get_data()
    wh_id = data.get('wh_id')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    async with get_session() as session:
        # total продуктов с положительным балансом
        subq = (
            select(Product.id)
            .join(StockMovement, StockMovement.product_id == Product.id)
            .where(Product.is_active == True, StockMovement.warehouse_id == wh_id)
            .group_by(Product.id)
            .having(func.sum(StockMovement.qty) > 0)
            .subquery()
        )
        total = await session.scalar(select(func.count()).select_from(subq))

        # текущая страница
        res = await session.execute(
            select(Product)
            .join(StockMovement, StockMovement.product_id == Product.id)
            .where(Product.is_active == True, StockMovement.warehouse_id == wh_id)
            .group_by(Product.id)
            .having(func.sum(StockMovement.qty) > 0)
            .order_by(Product.article)
            .offset((page - 1) * PAGE_SIZE_REPORTS)
            .limit(PAGE_SIZE_REPORTS)
        )
        products = res.scalars().all()

    if not products:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="rep_back_to_types")],
            [types.InlineKeyboardButton(text="🏬 Выбор склада", callback_data="rep_back_to_wh")],
        ])
        await send_content(
            cb,
            "📉 На этом складе сейчас нет товаров с остатком.\n\n"
            "Вернитесь назад и выберите другой тип отчёта или склад.",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    kb = products_page_kb(
        products=products,
        page=page,
        page_size=PAGE_SIZE_REPORTS,
        total=total,
        back_to="rep_back_to_types",
        item_prefix="rep_art",
        page_prefix="rep_art_page",
    )

    await send_content(cb, "🔍 Выберите артикул для отчёта:", reply_markup=kb)


# ===== Выбор артикула -> остаток по нему =====
async def rep_pick_article(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("rep_art:"):
        return
    await cb.answer()

    product_id = int(cb.data.split(":")[1])
    data = await state.get_data()
    wh_id = data.get('wh_id')
    wh_name = data.get('wh_name')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    SM = aliased(StockMovement)
    async with get_session() as session:
        product = await session.get(Product, product_id)
        if not product or not product.is_active:
            await send_content(cb, "🚫 Товар не найден или неактивен.")
            return

        balance = await session.scalar(
            select(func.coalesce(func.sum(SM.qty), 0))
            .where(SM.product_id == product_id, SM.warehouse_id == wh_id)
        )

    text = (
        f"📊 **Остаток на складе {wh_name}**\n\n"
        f"🔹 Артикул: `{product.article}`\n"
        f"📦 Товар: *{product.name}*\n"
        f"➡️ Остаток: **{balance}** шт."
    )
    await send_content(cb, text, parse_mode="Markdown")

    kb_back = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к артикулам", callback_data="rep_article")],
    ])
    await cb.message.answer("Выберите дальнейшее действие:", reply_markup=kb_back)


# ===== Навигация назад =====
async def rep_back_to_types(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    wh_name = data.get('wh_name', 'неизвестен')
    await send_content(
        cb,
        f"🏬 Склад: *{wh_name}*. Выберите тип отчёта:",
        reply_markup=kb_report_type(),
        parse_mode="Markdown",
    )


async def rep_articles_page_handler(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("rep_art_page:"):
        return
    page = int(cb.data.split(":")[1])
    await rep_articles_page(cb, user, state, page=page)


async def rep_back_to_warehouses(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()

    async with get_session() as session:
        res = await session.execute(
            select(Warehouse).where(Warehouse.is_active == True).order_by(Warehouse.name)
        )
        warehouses = res.scalars().all()

    if not warehouses:
        await send_content(cb, "🚫 Нет активных складов.")
        return

    await send_content(
        cb,
        "🏬 Выберите склад:",
        reply_markup=warehouses_kb(warehouses, prefix="rep_wh"),
    )


# ===== Регистрация =====
def register_reports_handlers(dp: Dispatcher):
    # Корень раздела
    dp.callback_query.register(reports_root, lambda c: c.data == "reports")

    # Просмотр остатков (через отчёты)
    dp.callback_query.register(rep_view,           lambda c: c.data == "rep_view")
    dp.callback_query.register(rep_pick_warehouse, lambda c: c.data.startswith("rep_wh:"))

    # Типы отчётов
    dp.callback_query.register(rep_all,    lambda c: c.data == "rep_all")
    dp.callback_query.register(rep_packed, lambda c: c.data == "rep_packed")
    dp.callback_query.register(rep_article, lambda c: c.data == "rep_article")

    # Пагинация и выбор артикула
    dp.callback_query.register(rep_articles_page_handler, lambda c: c.data.startswith("rep_art_page:"))
    dp.callback_query.register(rep_pick_article,          lambda c: c.data.startswith("rep_art:"))

    # Навигация назад
    dp.callback_query.register(rep_back_to_types,      lambda c: c.data == "rep_back_to_types")
    dp.callback_query.register(rep_back_to_warehouses, lambda c: c.data == "rep_back_to_wh")

```

## Файл: handlers\stocks.py

```python
# handlers/stocks.py
from aiogram import Dispatcher, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func, and_
from sqlalchemy.orm import aliased

from database.db import get_session
from database.models import User, Warehouse, Product, StockMovement, ProductStage
from handlers.common import send_content
from keyboards.inline import warehouses_kb, products_page_kb


PAGE_SIZE_STOCKS = 15


class StockReportState(StatesGroup):
    warehouse_selected = State()  # Храним wh_id и wh_name
    choosing_article = State()


def kb_stocks_root():
    kb = [[types.InlineKeyboardButton(text="📦 Просмотр остатков", callback_data="stocks_view")]]
    kb.append([types.InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_menu")])
    return types.InlineKeyboardMarkup(inline_keyboard=kb)


async def stocks_root(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()
    await send_content(
        cb,
        "Остатки товара на складах — выберите действие:",
        reply_markup=kb_stocks_root(),
    )


def kb_report_type():
    """Клавиатура выбора типа отчёта с кнопкой назад."""
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📊 Отчёт по всем товарам", callback_data="report_all")],
        [types.InlineKeyboardButton(text="🎁 Упакованные остатки", callback_data="report_packed")],
        [types.InlineKeyboardButton(text="🔍 Отчёт по артикулу", callback_data="report_article")],
        [types.InlineKeyboardButton(text="⬅️ Назад к складам", callback_data="stocks_back_to_wh")],
    ])


def split_message(text: str, max_len: int = 4000) -> list[str]:
    """Разбивает длинное сообщение на части по строкам, с запасом."""
    parts = []
    while len(text) > max_len:
        split_at = text.rfind('\n', 0, max_len)
        if split_at == -1:
            split_at = max_len
        parts.append(text[:split_at].strip())
        text = text[split_at:].strip()
    if text:
        parts.append(text)
    return parts


# ===== Просмотр остатков: выбор склада =====
async def stocks_view(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()

    async with get_session() as session:
        res = await session.execute(
            select(Warehouse).where(Warehouse.is_active == True).order_by(Warehouse.name)
        )
        warehouses = res.scalars().all()
    if not warehouses:
        await send_content(cb, "🚫 Нет активных складов.")
        return

    await send_content(
        cb,
        "🏬 Выберите склад для просмотра остатков:",
        reply_markup=warehouses_kb(warehouses, prefix="pr_wh"),
    )


# ===== Выбор склада для просмотра -> меню типа отчёта =====
async def pick_warehouse_for_view(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("pr_wh:"):
        return
    await cb.answer()

    wh_id = int(cb.data.split(":")[1])
    async with get_session() as session:
        warehouse = await session.get(Warehouse, wh_id)
        if not warehouse or not warehouse.is_active:
            await send_content(cb, "🚫 Склад не найден или неактивен.")
            return

    await state.set_state(StockReportState.warehouse_selected)
    await state.update_data(wh_id=wh_id, wh_name=warehouse.name)
    await send_content(
        cb,
        f"🏬 Склад: *{warehouse.name}*. Выберите тип отчёта:",
        reply_markup=kb_report_type(),
        parse_mode="Markdown",
    )


# ===== Отчёт по всем товарам =====
async def report_all(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    wh_id = data.get('wh_id')
    wh_name = data.get('wh_name')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    SM = aliased(StockMovement)
    async with get_session() as session:
        res = await session.execute(
            select(
                Product.article,
                Product.name,
                func.sum(SM.qty).label("balance")
            )
            .join(SM, and_(SM.product_id == Product.id, SM.warehouse_id == wh_id))
            .where(Product.is_active == True)
            .group_by(Product.id)
            .having(func.sum(SM.qty) > 0)
            .order_by(Product.article)
        )
        rows = res.all()

    if not rows:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="back_to_report_type")],
            [types.InlineKeyboardButton(text="🏬 Выбор склада", callback_data="stocks_back_to_wh")],
        ])
        await send_content(
            cb,
            f"📉 На складе *{wh_name}* сейчас нет товаров с остатком\n\n"
            f"Выберите другой тип отчёта или склад.",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    total_items = len(rows)
    total_balance = sum(row.balance for row in rows)
    lines = [f"🔹 `{row.article}` — *{row.name}*: **{row.balance}** шт." for row in rows]
    text = (
            f"📊 **Остатки на складе {wh_name}**  Товары с остатком:\n\n"
            + "\n\n".join(lines)
            + f"\n\n📈 **Итого:** {total_items} товаров, суммарный остаток: **{total_balance}** шт."
    )
    parts = split_message(text)

    for i, part in enumerate(parts, 1):
        if len(parts) > 1:
            part = f"Часть {i}/{len(parts)}:\n\n{part}"
        await cb.message.answer(part, parse_mode="Markdown")

    # Кнопка назад
    kb_back = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="back_to_report_type")],
    ])
    await cb.message.answer("Выберите дальнейшее действие:", reply_markup=kb_back)


# ===== НОВОЕ: Упакованные остатки (stage=packed) =====
async def report_packed(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    wh_id = data.get('wh_id')
    wh_name = data.get('wh_name')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    SM = aliased(StockMovement)
    async with get_session() as session:
        res = await session.execute(
            select(
                Product.article,
                Product.name,
                func.sum(SM.qty).label("balance")
            )
            .join(SM, and_(
                SM.product_id == Product.id,
                SM.warehouse_id == wh_id,
                SM.stage == ProductStage.packed
            ))
            .where(Product.is_active == True)
            .group_by(Product.id)
            .having(func.sum(SM.qty) > 0)
            .order_by(Product.article)
        )
        rows = res.all()

    if not rows:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="back_to_report_type")],
            [types.InlineKeyboardButton(text="🏬 Выбор склада", callback_data="stocks_back_to_wh")],
        ])
        await send_content(
            cb,
            f"📭 На складе *{wh_name}* нет упакованных остатков.",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    total_items = len(rows)
    total_balance = sum(row.balance for row in rows)
    lines = [f"🎁 `{row.article}` — *{row.name}*: **{row.balance}** шт." for row in rows]
    text = (
            f"🎁 **Упакованные остатки на складе {wh_name}**\n\n"
            + "\n\n".join(lines)
            + f"\n\n📈 **Итого:** {total_items} товаров, упаковано: **{total_balance}** шт."
    )
    parts = split_message(text)

    for i, part in enumerate(parts, 1):
        if len(parts) > 1:
            part = f"Часть {i}/{len(parts)}:\n\n{part}"
        await cb.message.answer(part, parse_mode="Markdown")

    kb_back = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="back_to_report_type")],
    ])
    await cb.message.answer("Выберите дальнейшее действие:", reply_markup=kb_back)


# ===== Отчёт по артикулу: показ пагинированного списка =====
async def report_article(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.set_state(StockReportState.choosing_article)
    await report_articles_page(cb, user, state, page=1)


async def report_articles_page(cb: types.CallbackQuery, user: User, state: FSMContext, page: int = 1):
    data = await state.get_data()
    wh_id = data.get('wh_id')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    async with get_session() as session:
        # Total: только товары с balance > 0
        subq = (
            select(Product.id)
            .join(StockMovement, StockMovement.product_id == Product.id)
            .where(Product.is_active == True, StockMovement.warehouse_id == wh_id)
            .group_by(Product.id)
            .having(func.sum(StockMovement.qty) > 0)
            .subquery()
        )
        total_stmt = select(func.count()).select_from(subq)
        total = await session.scalar(total_stmt)

        # Список: товары с balance > 0
        res = await session.execute(
            select(Product)
            .join(StockMovement, StockMovement.product_id == Product.id)
            .where(Product.is_active == True, StockMovement.warehouse_id == wh_id)
            .group_by(Product.id)
            .having(func.sum(StockMovement.qty) > 0)
            .order_by(Product.article)
            .offset((page - 1) * PAGE_SIZE_STOCKS)
            .limit(PAGE_SIZE_STOCKS)
        )
        products = res.scalars().all()

    if not products:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⬅️ Назад к типам отчёта", callback_data="back_to_report_type")],
            [types.InlineKeyboardButton(text="🏬 Выбор склада", callback_data="stocks_back_to_wh")],
        ])
        await send_content(
            cb,
            "📉 На этом складе сейчас нет товаров с остатком\n\n"
            "Вернитесь назад и выберите другой тип отчёта или склад.",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    # Универсальная клавиатура с «отчётными» префиксами (не конфликтует с Receiving)
    kb = products_page_kb(
        products=products,
        page=page,
        page_size=PAGE_SIZE_STOCKS,
        total=total,
        back_to="back_to_report_type",
        item_prefix="report_art",
        page_prefix="report_art_page",
    )

    await send_content(cb, "🔍 Выберите артикул для отчёта:", reply_markup=kb)


# ===== Выбор артикула -> показ остатка =====
async def pick_article(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("report_art:"):
        return
    await cb.answer()

    product_id = int(cb.data.split(":")[1])
    data = await state.get_data()
    wh_id = data.get('wh_id')
    wh_name = data.get('wh_name')
    if not wh_id:
        await send_content(cb, "❗ Ошибка: склад не выбран.")
        return

    SM = aliased(StockMovement)
    async with get_session() as session:
        product = await session.get(Product, product_id)
        if not product or not product.is_active:
            await send_content(cb, "🚫 Товар не найден или неактивен.")
            return

        balance = await session.scalar(
            select(func.coalesce(func.sum(SM.qty), 0))
            .where(SM.product_id == product_id, SM.warehouse_id == wh_id)
        )

    text = (
        f"📊 **Остаток на складе {wh_name}**\n\n"
        f"🔹 Артикул: `{product.article}`\n"
        f"📦 Товар: *{product.name}*\n"
        f"➡️ Остаток: **{balance}** шт."
    )

    await send_content(cb, text, parse_mode="Markdown")

    # Кнопка назад
    kb_back = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад к артикулам", callback_data="report_article")],
    ])
    await cb.message.answer("Выберите дальнейшее действие:", reply_markup=kb_back)


# ===== Назад к типу отчёта =====
async def back_to_report_type(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    wh_name = data.get('wh_name', 'неизвестен')
    await send_content(
        cb,
        f"🏬 Склад: *{wh_name}*. Выберите тип отчёта:",
        reply_markup=kb_report_type(),
        parse_mode="Markdown",
    )


# ===== Пагинация для артикулов =====
async def report_articles_page_handler(cb: types.CallbackQuery, user: User, state: FSMContext):
    if not cb.data.startswith("report_art_page:"):
        return
    page = int(cb.data.split(":")[1])
    await report_articles_page(cb, user, state, page=page)


# ===== Назад к складам =====
async def back_to_warehouses(cb: types.CallbackQuery, user: User, state: FSMContext):
    await cb.answer()
    await state.clear()
    await stocks_view(cb, user, state)  # Возвращаемся к выбору склада


def register_stocks_handlers(dp: Dispatcher):
    dp.callback_query.register(stocks_root, lambda c: c.data == "stocks")
    dp.callback_query.register(stocks_view, lambda c: c.data == "stocks_view")

    # Флоу для просмотра/отчёта
    dp.callback_query.register(pick_warehouse_for_view, lambda c: c.data.startswith("pr_wh:"))
    dp.callback_query.register(report_all, lambda c: c.data == "report_all")
    dp.callback_query.register(report_packed, lambda c: c.data == "report_packed")  # <— НОВОЕ
    dp.callback_query.register(report_article, lambda c: c.data == "report_article")
    dp.callback_query.register(report_articles_page_handler, lambda c: c.data.startswith("report_art_page:"))
    dp.callback_query.register(pick_article, lambda c: c.data.startswith("report_art:"))
    dp.callback_query.register(back_to_report_type, lambda c: c.data == "back_to_report_type")
    dp.callback_query.register(back_to_warehouses, lambda c: c.data == "stocks_back_to_wh")

```

## Файл: handlers\supplies.py

```python
from __future__ import annotations

from datetime import datetime
from typing import List, Dict, Tuple

from aiogram import Router, F, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from database.db import get_session, available_packed
from database.models import (
    Warehouse, Product, StockMovement,
    Supply, SupplyItem, SupplyBox, SupplyFile, User,
    MovementType, ProductStage, UserRole, SupplyStatus
)

router = Router()

PAGE = 10

# ---------- FSM (MVP создания поставки из PACKED) ----------
class SupFSM(StatesGroup):
    MP = State()
    WH = State()
    ITEMS = State()
    QTY = State()
    CONFIRM = State()


# ---------- Keyboards ----------
def kb_sup_tabs(role: UserRole) -> InlineKeyboardMarkup:
    rows = []
    if role in (UserRole.admin, UserRole.manager):
        rows += [
            [InlineKeyboardButton(text="🆕 Черновики", callback_data="sup:list:draft:0")],
            [InlineKeyboardButton(text="📥 К сборке",  callback_data="sup:list:queued:0")],
            [InlineKeyboardButton(text="🛠 В работе",  callback_data="sup:list:assembling:0")],
            [InlineKeyboardButton(text="✅ Собранные", callback_data="sup:list:assembled:0")],
            [InlineKeyboardButton(text="🚚 Доставляется", callback_data="sup:list:in_transit:0")],
            [InlineKeyboardButton(text="🗄 Архив", callback_data="sup:list:arch:0")],
            [InlineKeyboardButton(text="🆕 Создать поставку", callback_data="sup:new")],
        ]
    else:
        rows += [
            [InlineKeyboardButton(text="📥 К сборке",  callback_data="sup:list:queued:0")],
            [InlineKeyboardButton(text="🛠 Мои в работе", callback_data="sup:list:myassembling:0")],
            [InlineKeyboardButton(text="✅ Мои собранные", callback_data="sup:list:myassembled:0")],
            [InlineKeyboardButton(text="🗄 Архив", callback_data="sup:list:myarch:0")],
        ]
    rows.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_mp() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Wildberries", callback_data="sup:mp:wb")],
        [InlineKeyboardButton(text="Ozon",        callback_data="sup:mp:ozon")],
        [InlineKeyboardButton(text="❌ Отмена",    callback_data="sup:cancel")],
    ])


def kb_wh_list(warehouses, page=0) -> InlineKeyboardMarkup:
    start = page * PAGE
    chunk = warehouses[start:start + PAGE]
    rows = [[InlineKeyboardButton(text=name, callback_data=f"sup:wh:{wid}")]
            for wid, name in chunk]
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"sup:wh:page:{page - 1}"))
    if start + PAGE < len(warehouses):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"sup:wh:page:{page + 1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="sup:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_products_packed(products, page: int, wh_id: int) -> InlineKeyboardMarkup:
    start = page * PAGE
    chunk = products[start:start + PAGE]
    rows = [[InlineKeyboardButton(
        text=f"{name} (art. {article}) — PACKED {packed}",
        callback_data=f"sup:add:{wh_id}:{pid}"
    )] for pid, name, article, packed in chunk]
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"sup:prod:page:{page - 1}"))
    if start + PAGE < len(products):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"sup:prod:page:{page + 1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton(text="📩 Сохранить черновик", callback_data="sup:submit")])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="sup:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить ещё", callback_data="sup:more")],
        [InlineKeyboardButton(text="📩 Сохранить черновик", callback_data="sup:submit")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="sup:cancel")],
    ])


def kb_supply_card(s: Supply, role: UserRole, all_sealed: bool) -> InlineKeyboardMarkup:
    rows = []
    st = s.status
    if st == SupplyStatus.draft and role in (UserRole.admin, UserRole.manager):
        rows += [
            [InlineKeyboardButton(text="✏️ Реквизиты", callback_data=f"sup:edit:{s.id}")],
            [InlineKeyboardButton(text="➕ Короб", callback_data=f"sup:box:add:{s.id}")],
            [InlineKeyboardButton(text="📎 PDF", callback_data=f"sup:file:add:{s.id}")],
            [InlineKeyboardButton(text="📤 Отправить на сборку", callback_data=f"sup:queue:{s.id}")],
            [InlineKeyboardButton(text="🗑 Отменить", callback_data=f"sup:cancel:{s.id}")],
        ]
    elif st == SupplyStatus.queued:
        rows += [[InlineKeyboardButton(text="🛠 Взять в работу", callback_data=f"sup:assign:{s.id}")]]
        if role in (UserRole.admin, UserRole.manager):
            rows += [
                [InlineKeyboardButton(text="↩️ В черновик", callback_data=f"sup:to_draft:{s.id}")],
                [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"sup:edit:{s.id}")],
                [InlineKeyboardButton(text="📎 PDF", callback_data=f"sup:file:add:{s.id}")],
            ]
    elif st == SupplyStatus.assembling:
        rows += [
            [InlineKeyboardButton(text="📦 Запечатать все", callback_data=f"sup:box:seal_all:{s.id}")],
            [InlineKeyboardButton(text="🔓 Снять пломбы", callback_data=f"sup:box:unseal_all:{s.id}")],
            [InlineKeyboardButton(text="✅ Отметить как собранную", callback_data=f"sup:assembled:{s.id}")],
        ]
        if role in (UserRole.admin, UserRole.manager):
            rows += [[InlineKeyboardButton(text="↩️ Снять с работы", callback_data=f"sup:to_queue:{s.id}")]]
    elif st == SupplyStatus.assembled:
        rows += [
            [InlineKeyboardButton(text="🚚 Провести (отправить)", callback_data=f"sup:post:{s.id}")],
            [InlineKeyboardButton(text="🔓 Снять пломбу", callback_data=f"sup:box:unseal_all:{s.id}")],
        ]
    elif st == SupplyStatus.in_transit and role in (UserRole.admin, UserRole.manager):
        rows += [
            [InlineKeyboardButton(text="✅ Доставлено", callback_data=f"sup:delivered:{s.id}")],
            [InlineKeyboardButton(text="↩️ Возврат", callback_data=f"sup:return:{s.id}")],
            [InlineKeyboardButton(text="♻️ Расформировать", callback_data=f"sup:unpost:{s.id}")],
        ]
    rows.append([InlineKeyboardButton(text="⬅️ К списку", callback_data=f"sup:list:auto:0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------- Helpers ----------
async def _warehouses_list() -> List[Tuple[int, str]]:
    async with get_session() as s:
        rows = (await s.execute(
            select(Warehouse.id, Warehouse.name)
            .where((Warehouse.is_active.is_(True)) | (Warehouse.is_active.is_(None)))
            .order_by(Warehouse.name.asc())
        )).all()
    items = [(r[0], r[1]) for r in rows]
    counts = {}
    for _, n in items:
        counts[n] = counts.get(n, 0) + 1
    return [(wid, name if counts[name] == 1 else f"{name} (#{wid})") for wid, name in items]


async def _products_with_packed(session: AsyncSession, warehouse_id: int) -> List[Tuple[int, str, str, int]]:
    sm, p = StockMovement, Product
    packed_sum = select(
        sm.product_id.label("pid"),
        func.coalesce(func.sum(sm.qty), 0).label("packed_balance")
    ).where(
        sm.warehouse_id == warehouse_id,
        sm.stage == ProductStage.packed
    ).group_by(sm.product_id).subquery()

    q = select(p.id, p.name, p.article, packed_sum.c.packed_balance) \
        .join(packed_sum, packed_sum.c.pid == p.id) \
        .where(packed_sum.c.packed_balance > 0) \
        .order_by(p.name.asc())
    rows = (await session.execute(q)).all()
    return [(r[0], r[1], r[2], int(r[3])) for r in rows]


async def _get_balance(session: AsyncSession, wh: int, pid: int, stage: ProductStage) -> int:
    val = (await session.execute(
        select(func.coalesce(func.sum(StockMovement.qty), 0))
        .where(StockMovement.warehouse_id == wh)
        .where(StockMovement.product_id == pid)
        .where(StockMovement.stage == stage)
    )).scalar()
    return int(val or 0)


def _now():
    return datetime.utcnow()


# ---------- Lists / Cards ----------
async def _load_supplies(tab: str, user: User, page: int = 0):
    q = select(Supply).order_by(Supply.created_at.desc())
    if tab == "draft":
        q = q.where(Supply.status == SupplyStatus.draft)
    elif tab == "queued":
        q = q.where(Supply.status == SupplyStatus.queued)
    elif tab == "assembling":
        q = q.where(Supply.status == SupplyStatus.assembling)
    elif tab == "assembled":
        q = q.where(Supply.status == SupplyStatus.assembled)
    elif tab == "in_transit":
        q = q.where(Supply.status == SupplyStatus.in_transit)
    elif tab == "arch":
        q = q.where(Supply.status.in_([SupplyStatus.archived_delivered,
                                       SupplyStatus.archived_returned,
                                       SupplyStatus.cancelled]))
    elif tab.startswith("my"):
        if tab == "myassembling":
            q = q.where(Supply.status == SupplyStatus.assembling, Supply.assigned_picker_id == user.id)
        elif tab == "myassembled":
            q = q.where(Supply.status == SupplyStatus.assembled, Supply.assigned_picker_id == user.id)
        else:  # myarch
            q = q.where(Supply.assigned_picker_id == user.id,
                        Supply.status.in_([SupplyStatus.archived_delivered, SupplyStatus.archived_returned]))
    async with get_session() as s:
        rows = (await s.execute(q.offset(page * PAGE).limit(PAGE + 1))).scalars().all()
    has_next = len(rows) > PAGE
    rows = rows[:PAGE]
    return rows, has_next


def _kb_sup_list(tab: str, s_list: List[Supply], page: int, has_next: bool) -> InlineKeyboardMarkup:
    rows = []
    for s in s_list:
        title = f"SUP-{s.created_at:%Y%m%d}-{s.id:03d} • {s.status.value}"
        rows.append([InlineKeyboardButton(text=title, callback_data=f"sup:open:{s.id}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"sup:list:{tab}:{page - 1}"))
    if has_next:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"sup:list:{tab}:{page + 1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="supplies")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_supply_card(call_or_msg, s_id: int, user: User):
    async with get_session() as s:
        sup = await s.get(Supply, s_id)
        if not sup:
            return await (call_or_msg.message.answer if isinstance(call_or_msg, types.CallbackQuery) else call_or_msg.answer)(
                "Поставка не найдена."
            )
        boxes = (await s.execute(select(SupplyBox).where(SupplyBox.supply_id == s_id).order_by(SupplyBox.box_number))).scalars().all()
        items = (await s.execute(select(SupplyItem, Product.article, Product.name)
                                 .join(Product, Product.id == SupplyItem.product_id)
                                 .where(SupplyItem.supply_id == s_id))).all()
        files = (await s.execute(select(SupplyFile).where(SupplyFile.supply_id == s_id)
                                 .order_by(SupplyFile.uploaded_at.desc()))).scalars().all()

    by_box: Dict[int, List[Tuple[str, str, int]]] = {}
    for it, art, name in items:
        by_box.setdefault(it.box_id or 0, []).append((art, name, it.qty))
    lines = [
        f"№SUP-{sup.created_at:%Y%m%d}-{sup.id:03d} • [{sup.status.value}]",
        f"МП: {sup.mp or '—'} • МП-склад: {sup.mp_warehouse or '—'} • Склад: #{sup.warehouse_id}",
        f"Сборщик: {sup.assigned_picker_id or '—'}",
        f"Комментарий: {sup.comment or '—'}",
        "",
        "Короба:"
    ]
    all_sealed = True
    if not boxes:
        lines.append("— нет коробов —")
    else:
        for b in boxes:
            b_items = by_box.get(b.id, [])
            qty = sum(q for _, _, q in b_items)
            lines.append(f"#{b.box_number} • {'sealed' if b.sealed else 'open'} • позиций {len(b_items)} • qty {qty}")
            all_sealed = all_sealed and b.sealed
    lines.append("")
    lines.append("Вложения:")
    lines += ([f"• {f.filename or f.file_id}" for f in files] or ["— нет файлов —"])

    kb = kb_supply_card(sup, user.role, all_sealed)
    txt = "\n".join(lines)
    if isinstance(call_or_msg, types.CallbackQuery):
        await call_or_msg.message.edit_text(txt, reply_markup=kb)
    else:
        await call_or_msg.answer(txt, reply_markup=kb)


# ---------- Entry ----------
@router.callback_query(F.data == "supplies")
async def supplies_root(call: types.CallbackQuery, user: User):
    await call.answer()
    await call.message.edit_text("Раздел «Поставки»", reply_markup=kb_sup_tabs(user.role))


@router.callback_query(F.data.startswith("sup:list:"))
async def sup_list(call: types.CallbackQuery, user: User):
    _, _, tab, page_s = call.data.split(":")
    if tab == "auto":
        tab = "draft" if user.role in (UserRole.admin, UserRole.manager) else "queued"
    page = int(page_s)
    s_list, has_next = await _load_supplies(tab, user, page)
    await call.message.edit_text(f"Поставки — {tab}", reply_markup=_kb_sup_list(tab, s_list, page, has_next))


@router.callback_query(F.data.startswith("sup:open:"))
async def sup_open(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    await _render_supply_card(call, sid, user)


# ---------- Create (FSM) ----------
@router.callback_query(F.data == "sup:new")
async def sup_new(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(SupFSM.MP)
    await call.message.edit_text("Выберите маркетплейс:", reply_markup=kb_mp())


@router.callback_query(SupFSM.MP, F.data.startswith("sup:mp:"))
async def sup_pick_mp(call: types.CallbackQuery, state: FSMContext):
    mp = call.data.split(":")[-1]  # wb|ozon
    await state.update_data(mp=mp)
    ws = await _warehouses_list()
    await state.set_state(SupFSM.WH)
    await call.message.edit_text("Выберите склад-источник:", reply_markup=kb_wh_list(ws, 0))


@router.callback_query(SupFSM.WH, F.data.startswith("sup:wh:page:"))
async def sup_wh_page(call: types.CallbackQuery, state: FSMContext):
    page = int(call.data.split(":")[-1])
    ws = await _warehouses_list()
    await call.message.edit_reply_markup(reply_markup=kb_wh_list(ws, page))


@router.callback_query(SupFSM.WH, F.data.startswith("sup:wh:"))
async def sup_wh_pick(call: types.CallbackQuery, state: FSMContext):
    wh_id = int(call.data.split(":")[-1])
    async with get_session() as s:
        products = await _products_with_packed(s, wh_id)
    await state.update_data(wh_id=wh_id, products=products, page=0, cart={})
    await state.set_state(SupFSM.ITEMS)
    await call.message.edit_text("Добавьте позиции (из упакованного PACKED):",
                                 reply_markup=kb_products_packed(products, 0, wh_id))


@router.callback_query(SupFSM.ITEMS, F.data.startswith("sup:prod:page:"))
async def sup_products_page(call: types.CallbackQuery, state: FSMContext):
    page = int(call.data.split(":")[-1])
    data = await state.get_data()
    await state.update_data(page=page)
    await call.message.edit_reply_markup(reply_markup=kb_products_packed(data["products"], page, data["wh_id"]))


@router.callback_query(SupFSM.ITEMS, F.data.startswith("sup:add:"))
async def sup_add_product(call: types.CallbackQuery, state: FSMContext):
    _, _, wh, pid = call.data.split(":")
    await state.update_data(cur_pid=int(pid))
    await state.set_state(SupFSM.QTY)
    await call.message.edit_text("Введите количество для добавления в поставку:")


@router.message(SupFSM.QTY)
async def sup_qty_input(msg: types.Message, state: FSMContext):
    txt = (msg.text or "").strip()
    if not txt.isdigit() or int(txt) <= 0:
        return await msg.answer("Нужно целое число > 0. Введите ещё раз:")
    qty = int(txt)
    data = await state.get_data()
    wh_id, pid = data["wh_id"], data["cur_pid"]

    async with get_session() as s:
        packed = await _get_balance(s, wh_id, pid, ProductStage.packed)

    warn = ""
    if qty > packed:
        warn = f"⚠️ Упаковано PACKED={packed}, вы добавляете {qty}. Дефицит {qty - packed}. Будет проверено при «Провести»."
    cart = data.get("cart", {})
    cart[pid] = cart.get(pid, 0) + qty
    await state.update_data(cart=cart)
    await state.set_state(SupFSM.CONFIRM)

    async with get_session() as s:
        prod = (await s.execute(select(Product.name, Product.article).where(Product.id == pid))).first()
    name, article = prod
    await msg.answer(
        f"Добавлено: {name} (art. {article}) — {qty}\n{warn}\n"
        f"Что дальше?",
        reply_markup=kb_confirm()
    )


@router.callback_query(SupFSM.CONFIRM, F.data == "sup:more")
async def sup_more(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    async with get_session() as s:
        products = await _products_with_packed(s, data["wh_id"])
    await state.update_data(products=products, page=0)
    await state.set_state(SupFSM.ITEMS)
    await call.message.edit_text("Добавьте позиции (из упакованного PACKED):",
                                 reply_markup=kb_products_packed(products, 0, data["wh_id"]))


@router.callback_query(SupFSM.CONFIRM, F.data == "sup:submit")
async def sup_submit(call: types.CallbackQuery, state: FSMContext, user: User):
    data = await state.get_data()
    cart: Dict[int, int] = data.get("cart", {})
    if not cart:
        return await call.answer("Пусто", show_alert=True)

    mp = data["mp"]
    wh_id = data["wh_id"]

    async with get_session() as s:
        # создаём черновик
        sup = Supply(
            warehouse_id=wh_id,
            created_by=user.id,
            status=SupplyStatus.draft,
            mp=mp,
        )
        s.add(sup)
        await s.flush()
        # авто-создаём короб #1 (MVP)
        box = SupplyBox(supply_id=sup.id, box_number=1, sealed=False)
        s.add(box)
        await s.flush()
        for pid, qty in cart.items():
            s.add(SupplyItem(supply_id=sup.id, product_id=pid, qty=qty, box_id=box.id))
        await s.commit()
        sup_id = sup.id

    await state.clear()
    await call.message.edit_text(f"Черновик поставки создан: SUP-{datetime.utcnow():%Y%m%d}-{sup_id:03d}")
    await _render_supply_card(call, sup_id, user)


# ---------- Status transitions ----------
@router.callback_query(F.data.startswith("sup:queue:"))
async def sup_to_queue(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid)
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if user.role not in (UserRole.admin, UserRole.manager): return await call.answer("Недостаточно прав", show_alert=True)
        if sup.status != SupplyStatus.draft: return await call.answer("Только из 'draft'", show_alert=True)
        sup.status = SupplyStatus.queued
        sup.queued_at = _now()
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:assign:"))
async def sup_assign(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid)
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if sup.status != SupplyStatus.queued: return await call.answer("Только из 'queued'", show_alert=True)
        sup.assigned_picker_id = user.id
        sup.status = SupplyStatus.assembling
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:assembled:"))
async def sup_mark_assembled(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid)
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if sup.status != SupplyStatus.assembling: return await call.answer("Неверный статус", show_alert=True)
        sup.status = SupplyStatus.assembled
        sup.assembled_at = _now()
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:post:"))
async def sup_post(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid, options=[joinedload(Supply.items)])
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if sup.status != SupplyStatus.assembled: return await call.answer("Только из 'assembled'", show_alert=True)

        # Валидация доступности PACKED c учетом резервов
        for it in sup.items:
            can = await available_packed(s, sup.warehouse_id, it.product_id)
            if it.qty > can:
                return await call.answer(f"Недостаточно PACKED по товару {it.product_id}: доступно {can}, нужно {it.qty}", show_alert=True)

        max_doc = await s.scalar(select(func.max(StockMovement.doc_id)).where(StockMovement.type == MovementType.postavka))
        next_doc = int(max_doc or 0) + 1
        docname = f"SUP-{sup.created_at:%Y%m%d}-{sup.id:03d}"

        for it in sup.items:
            s.add(StockMovement(
                warehouse_id=sup.warehouse_id,
                product_id=it.product_id,
                qty=-it.qty,
                type=MovementType.postavka,
                stage=ProductStage.packed,
                user_id=user.id,
                doc_id=next_doc,
                comment=f"[SUP {docname}] Отправка в {sup.mp or 'MP'}/{sup.mp_warehouse or '-'}"
            ))

        sup.status = SupplyStatus.in_transit
        sup.posted_at = _now()
        await s.commit()

    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:delivered:"))
async def sup_delivered(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid)
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if user.role not in (UserRole.admin, UserRole.manager): return await call.answer("Недостаточно прав", show_alert=True)
        if sup.status != SupplyStatus.in_transit: return await call.answer("Только из 'in_transit'", show_alert=True)
        sup.status = SupplyStatus.archived_delivered
        sup.delivered_at = _now()
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:return:"))
async def sup_return(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid, options=[joinedload(Supply.items)])
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if user.role not in (UserRole.admin, UserRole.manager): return await call.answer("Недостаточно прав", show_alert=True)
        if sup.status != SupplyStatus.in_transit: return await call.answer("Только из 'in_transit'", show_alert=True)

        max_doc = await s.scalar(select(func.max(StockMovement.doc_id)).where(StockMovement.type == MovementType.postavka))
        next_doc = int(max_doc or 0) + 1
        docname = f"SUP-RET-{sup.created_at:%Y%m%d}-{sup.id:03d}"

        for it in sup.items:
            s.add(StockMovement(
                warehouse_id=sup.warehouse_id,
                product_id=it.product_id,
                qty=it.qty,
                type=MovementType.postavka,
                stage=ProductStage.packed,
                user_id=user.id,
                doc_id=next_doc,
                comment=f"[{docname}] Возврат из МП"
            ))

        sup.status = SupplyStatus.archived_returned
        sup.returned_at = _now()
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:unpost:"))
async def sup_unpost(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid, options=[joinedload(Supply.items)])
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if user.role not in (UserRole.admin, UserRole.manager): return await call.answer("Недостаточно прав", show_alert=True)
        if sup.status != SupplyStatus.in_transit: return await call.answer("Только из 'in_transit'", show_alert=True)

        max_doc = await s.scalar(select(func.max(StockMovement.doc_id)).where(StockMovement.type == MovementType.postavka))
        next_doc = int(max_doc or 0) + 1
        docname = f"SUP-UNPOST-{sup.created_at:%Y%m%d}-{sup.id:03d}"

        for it in sup.items:
            s.add(StockMovement(
                warehouse_id=sup.warehouse_id,
                product_id=it.product_id,
                qty=it.qty,
                type=MovementType.postavka,
                stage=ProductStage.packed,
                user_id=user.id,
                doc_id=next_doc,
                comment=f"[{docname}] Расформирование (возврат на склад)"
            ))

        sup.status = SupplyStatus.assembled
        sup.unposted_at = _now()
        await s.commit()
    await _render_supply_card(call, sid, user)


# ---------- Boxes (MVP действия) ----------
@router.callback_query(F.data.startswith("sup:box:add:"))
async def sup_box_add(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        sup = await s.get(Supply, sid)
        if not sup: return await call.answer("Не найдена", show_alert=True)
        if sup.status not in (SupplyStatus.draft, SupplyStatus.queued, SupplyStatus.assembling):
            return await call.answer("На этом статусе нельзя добавлять короб.", show_alert=True)
        last = (await s.execute(select(func.max(SupplyBox.box_number)).where(SupplyBox.supply_id == sid))).scalar()
        num = int(last or 0) + 1
        s.add(SupplyBox(supply_id=sid, box_number=num, sealed=False))
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:box:seal_all:"))
async def sup_box_seal_all(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        rows = (await s.execute(select(SupplyBox).where(SupplyBox.supply_id == sid))).scalars().all()
        for b in rows:
            b.sealed = True
        await s.commit()
    await _render_supply_card(call, sid, user)


@router.callback_query(F.data.startswith("sup:box:unseal_all:"))
async def sup_box_unseal_all(call: types.CallbackQuery, user: User):
    sid = int(call.data.split(":")[-1])
    async with get_session() as s:
        rows = (await s.execute(select(SupplyBox).where(SupplyBox.supply_id == sid))).scalars().all()
        for b in rows:
            b.sealed = False
        await s.commit()
    await _render_supply_card(call, sid, user)


# ---------- Files (PDF) ----------
@router.callback_query(F.data.startswith("sup:file:add:"))
async def sup_file_add_hint(call: types.CallbackQuery, state: FSMContext):
    sid = int(call.data.split(":")[-1])
    await state.update_data(upload_sup_id=sid)
    await call.message.answer("Пришлите PDF-файл для прикрепления к поставке (application/pdf).")


@router.message(F.document)
async def sup_file_upload(msg: types.Message, user: User, state: FSMContext):
    data = await state.get_data()
    sid = data.get("upload_sup_id")
    if not sid:
        return
    doc = msg.document
    if not doc or (doc.mime_type != "application/pdf"):
        return await msg.answer("Нужен PDF.")
    async with get_session() as s:
        s.add(SupplyFile(supply_id=int(sid), file_id=doc.file_id, filename=doc.file_name or "file.pdf", uploaded_by=user.id))
        await s.commit()
    await msg.answer("PDF прикреплён.")


# ---------- Cancel (FSM) ----------
@router.callback_query(F.data == "sup:cancel")
async def sup_cancel(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("Операция отменена.", reply_markup=kb_sup_tabs(UserRole.manager))  # роль не знаем тут ⇒ вернёмся из меню


# ---------- Registrar ----------
from aiogram import Dispatcher
def register_supplies_handlers(dp: Dispatcher):
    dp.include_router(router)

```

## Файл: handlers\__init__.py

```python

```

## Файл: keyboards\callbacks.py

```python

```

## Файл: keyboards\inline.py

```python
# keyboards/inline.py
from typing import List, Optional, Dict
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database.models import Warehouse, Product
from utils.pagination import build_pagination_keyboard  # ожидаем: -> List[InlineKeyboardButton]


def confirm_kb(prefix: str = "rcv") -> InlineKeyboardMarkup:
    """
    Универсальная клавиатура подтверждения (Подтвердить / Назад).
    Клик: <prefix>_confirm / <prefix>_back
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"{prefix}_confirm")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{prefix}_back")],
    ])


def warehouses_kb(
        warehouses: List[Warehouse],
        prefix: str = "rcv_wh",
        priorities_by_id: Optional[Dict[int, int]] = None,
        priorities_by_name: Optional[Dict[str, int]] = None,
        show_menu_back: bool = True,
) -> InlineKeyboardMarkup:
    """
    Список складов. По умолчанию — без спец-сортировки.
    Можно задать:
      - priorities_by_id={warehouse_id: priority}
      - priorities_by_name={"Санкт-Петербург": 0, "Томск": 1}
    callback_data: <prefix>:<id>
    """
    def prio(w: Warehouse) -> int:
        if priorities_by_id and w.id in priorities_by_id:
            return priorities_by_id[w.id]
        if priorities_by_name and w.name in priorities_by_name:
            return priorities_by_name[w.name]
        return 9999

    warehouses_sorted = sorted(warehouses, key=prio)

    rows: List[List[InlineKeyboardButton]] = []
    for w in warehouses_sorted:
        label = w.name
        rows.append([InlineKeyboardButton(text=label, callback_data=f"{prefix}:{w.id}")])

    if show_menu_back:
        rows.append([InlineKeyboardButton(text="⬅️ Назад к меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def products_page_kb(
        products: List[Product],
        page: int,
        page_size: int,
        total: int,
        back_to: Optional[str] = None,
        item_prefix: str = "rcv_prod",
        page_prefix: str = "rcv_prod_page",
        show_cancel: bool = False,
        cancel_to: str = "cancel",
        trim_len: int = 48,
) -> InlineKeyboardMarkup:
    """
    Список товаров с пагинацией.
    callback_data:
      - <item_prefix>:<product_id>
      - <page_prefix>:<page>
      - back_to (например, rcv_back_wh / stocks_back_wh / reports_back)
    """
    rows: List[List[InlineKeyboardButton]] = []

    def short_text(name: str) -> str:
        return name if len(name) <= trim_len else (name[:trim_len - 1] + "…")

    for p in products:
        title = short_text(p.name or f"ID {p.id}")
        art = f" (арт. {p.article})" if getattr(p, "article", None) else ""
        rows.append([InlineKeyboardButton(text=f"{title}{art}", callback_data=f"{item_prefix}:{p.id}")])

    # Пагинация — ожидаем, что build_pagination_keyboard вернёт одну строку кнопок
    pag_row = build_pagination_keyboard(
        page=page,
        page_size=page_size,
        total=total,
        prev_cb_prefix=page_prefix,
        next_cb_prefix=page_prefix,
        prev_text="◀ Предыдущая",
        next_text="Следующая ▶",
        # Если библиотека поддерживает no-op, можно пробросить:
        # noop_cb="noop"
    )
    if pag_row:
        rows.append(pag_row)

    # Назад / Отмена
    if back_to:
        last_row: List[InlineKeyboardButton] = [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)]
        if show_cancel:
            last_row.append(InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_to))
        rows.append(last_row)
    else:
        rows.append([InlineKeyboardButton(text="⬅️ Назад к меню", callback_data="back_to_menu")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def qty_kb(back_to: str, cancel_to: Optional[str] = None) -> InlineKeyboardMarkup:
    """
    Клавиатура для шага ввода количества.
    back_to: callback_data для шага «назад»
    cancel_to: (опционально) callback_data для «Отмена»
    """
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)],
    ]
    if cancel_to:
        rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_to)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def comment_kb(
        back_to: str,
        cancel_to: Optional[str] = None,
        skip_cb: str = "rcv_skip_comment"
) -> InlineKeyboardMarkup:
    """
    Клавиатура для комментария (Пропустить / Назад / (опц.) Отмена).
    skip_cb: callback_data для «Пропустить комментарий»
    """
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="⏭ Пропустить комментарий", callback_data=skip_cb)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)],
    ]
    if cancel_to:
        rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_to)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def receiving_confirm_kb(
        confirm_prefix: str,
        back_to: str,
        cancel_to: Optional[str] = None,
        confirm_text: str = "✅ Добавить",
) -> InlineKeyboardMarkup:
    """
    Клавиатура подтверждения поступления/действия.
    confirm_prefix="rcv" → "rcv_confirm"
    back_to: callback «назад»
    cancel_to: (опц.) callback «отмена»
    """
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text=confirm_text, callback_data=f"{confirm_prefix}_confirm")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_to)],
    ]
    if cancel_to:
        rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=cancel_to)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

```

## Файл: keyboards\main_menu.py

```python
# keyboards/main_menu.py
from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database.db import get_session
from database.menu_visibility import get_visible_menu_items_for_role
from database.models import UserRole, MenuItem

# Человеко-читаемые тексты
TEXTS = {
    MenuItem.stocks:        "📦 Остатки",
    MenuItem.receiving:     "➕ Поступление",
    MenuItem.supplies:      "🚚 Поставки",
    MenuItem.packing:       "🎁 Упаковка",
    MenuItem.picking:       "🧰 Сборка",
    MenuItem.reports:       "📈 Отчёты",
    MenuItem.purchase_cn:   "🇨🇳 Закупка CN",
    MenuItem.msk_warehouse: "🏢 Склад MSK",
    MenuItem.admin:         "⚙️ Администрирование",
}

# callback_data для существующих пунктов (как было)
CB = {
    MenuItem.stocks:        "stocks",
    MenuItem.receiving:     "receiving",
    MenuItem.supplies:      "supplies",
    MenuItem.packing:       "packing",
    MenuItem.picking:       "picking",
    MenuItem.reports:       "reports",
    MenuItem.purchase_cn:   "cn:root",
    MenuItem.msk_warehouse: "msk:root",
    MenuItem.admin:         "admin",
}

# Группы подкаталогов
# 1) «Закупки-поступления»
PROCURE_GROUP = [
    MenuItem.purchase_cn,
    MenuItem.msk_warehouse,
    MenuItem.receiving,
]

# 2) «Упаковка-поставки»
PACK_GROUP = [
    MenuItem.packing,
    MenuItem.supplies,
    MenuItem.picking,
    MenuItem.stocks,
]

# Тексты верхних категорий
ROOT_PROCURE_TEXT = "🧾 Закупки-поступления"
ROOT_PACK_TEXT    = "📦 Упаковка-поставки"


# -------------------- helpers --------------------
async def _get_visible_set(role: UserRole) -> set[MenuItem]:
    async with get_session() as session:
        visible = await get_visible_menu_items_for_role(session, role)
    return set(visible)

def _any_visible(visible: set[MenuItem], items: list[MenuItem]) -> bool:
    return any(i in visible for i in items)

def _rows_from_items(
        visible: set[MenuItem],
        items: list[MenuItem],
        per_row: int = 2,
) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    buf: list[InlineKeyboardButton] = []
    for it in items:
        if it not in visible:
            continue
        buf.append(InlineKeyboardButton(text=TEXTS[it], callback_data=CB[it]))
        if len(buf) == per_row:
            rows.append(buf)
            buf = []
    if buf:
        rows.append(buf)
    return rows


# -------------------- публичные билдеры --------------------
async def get_main_menu(role: UserRole) -> InlineKeyboardMarkup:
    """
    Корневое меню:
      • 2 категории (показываем только если внутри есть видимые пункты);
      • Отчёты — отдельной кнопкой;
      • Администрирование — отдельной кнопкой.
    """
    visible = await _get_visible_set(role)

    rows: list[list[InlineKeyboardButton]] = []

    if _any_visible(visible, PROCURE_GROUP):
        rows.append([InlineKeyboardButton(text=ROOT_PROCURE_TEXT, callback_data="root:procure")])

    if _any_visible(visible, PACK_GROUP):
        rows.append([InlineKeyboardButton(text=ROOT_PACK_TEXT, callback_data="root:pack")])

    if MenuItem.reports in visible:
        rows.append([InlineKeyboardButton(text=TEXTS[MenuItem.reports], callback_data=CB[MenuItem.reports])])

    if MenuItem.admin in visible:
        rows.append([InlineKeyboardButton(text=TEXTS[MenuItem.admin], callback_data=CB[MenuItem.admin])])

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def get_procure_submenu(role: UserRole) -> InlineKeyboardMarkup:
    """Подменю «Закупки-поступления»: Закупка CN, Склад MSK, Поступление."""
    visible = await _get_visible_set(role)
    rows = _rows_from_items(visible, PROCURE_GROUP)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="root:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def get_pack_submenu(role: UserRole) -> InlineKeyboardMarkup:
    """Подменю «Упаковка-поставки»: Упаковка, Поставки, Сборка, Остатки."""
    visible = await _get_visible_set(role)
    rows = _rows_from_items(visible, PACK_GROUP)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="root:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

```

## Файл: keyboards\__init__.py

```python

```

## Файл: middleware\role.py

```python
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
```

## Файл: scheduler\backup_scheduler.py

```python
# scheduler/backup_scheduler.py
from __future__ import annotations

import logging

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select

from database.db import get_session
from database.models import BackupSettings, BackupFrequency
from utils.backup import run_backup

JOB_ID = "warehouse_backup_job"
logger = logging.getLogger(__name__)


def _calc_trigger(st: BackupSettings, tzname: str) -> CronTrigger:
    tz = pytz.timezone(tzname)
    h, m = st.time_hour, st.time_minute

    if st.frequency == BackupFrequency.daily:
        return CronTrigger(hour=h, minute=m, timezone=tz)
    if st.frequency == BackupFrequency.weekly:
        # по умолчанию — понедельник; можно хранить день недели в БД
        return CronTrigger(day_of_week="mon", hour=h, minute=m, timezone=tz)
    # monthly
    return CronTrigger(day="1", hour=h, minute=m, timezone=tz)


async def reschedule_backup(scheduler: AsyncIOScheduler, tzname: str, db_url: str) -> None:
    """
    Снимает старую задачу и вешает новую по настройкам из БД (id=1).
    """
    # 1) Читаем настройки
    async with get_session() as s:
        st: BackupSettings | None = (
            (await s.execute(select(BackupSettings).where(BackupSettings.id == 1)))
            .scalar_one_or_none()
        )

    # 2) Снимаем прошлую джобу
    try:
        scheduler.remove_job(JOB_ID)
    except Exception:
        pass

    # 3) Проверяем, надо ли планировать
    if not st or not st.enabled:
        logger.info("Backups are disabled or settings missing — job not scheduled")
        return

    # 4) Считаем триггер и навешиваем джобу
    trigger = _calc_trigger(st, tzname)

    async def _job():
        ok, msg = await run_backup(db_url)
        if ok:
            logger.info(f"[BACKUP] {msg}")
        else:
            logger.error(f"[BACKUP] {msg}")
        # здесь при желании можно уведомлять админа в TG

    scheduler.add_job(_job, trigger=trigger, id=JOB_ID, replace_existing=True)
    logger.info(
        f"Backup job scheduled: {st.frequency.name} at {st.time_hour:02d}:{st.time_minute:02d} ({tzname})"
    )

```

## Файл: scripts\safety_check.py

```python
# scripts/safety_check.py
from __future__ import annotations
import os, re, sys

REPO = os.path.abspath(os.path.dirname(__file__) + "/..")

# Папки/файлы, которые не сканируем
SKIP_DIRS = {
    ".git", ".idea", ".vscode", "__pycache__", "venv", ".venv",
    "node_modules", "dist", "build", ".mypy_cache"
}
SKIP_FILES = {
    os.path.normcase(os.path.relpath(__file__, REPO)),     # сам чекер
    "scripts/safety_check.ps1",                            # возможный PS-скрипт
}

def walk_files(base: str):
    for root, dirs, files in os.walk(base):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for f in files:
            rel = os.path.normcase(os.path.relpath(os.path.join(root, f), REPO))
            if rel in SKIP_FILES:
                continue
            if not f.endswith((".py", ".txt", ".md", ".cfg", ".ini", ".yml", ".yaml", ".sh")):
                continue
            yield os.path.join(root, f), rel

def read_text(path: str) -> str:
    try:
        return open(path, "r", encoding="utf-8", errors="ignore").read()
    except Exception:
        return ""

errors: list[str] = []

# 1) Запрещаем любые упоминания старого пути/скрипта wb_db_restore.sh
legacy_re = re.compile(r"wb_db_restore\.sh")
legacy_hits: list[str] = []
for abs_path, rel in walk_files(REPO):
    txt = read_text(abs_path)
    if legacy_re.search(txt):
        legacy_hits.append(rel)

if legacy_hits:
    errors.append("Legacy path found (wb_db_restore.sh) in:\n  - " + "\n  - ".join(sorted(set(legacy_hits))))

# 2) Ровно ОДНА реализация build_restore_cmd — в utils/backup.py
def_re = re.compile(r"^\s*def\s+build_restore_cmd\s*\(", re.MULTILINE)
defs: list[str] = []
for abs_path, rel in walk_files(REPO):
    if rel.endswith(".py") and def_re.search(read_text(abs_path)):
        defs.append(rel.replace("\\", "/"))

if len(defs) == 0:
    errors.append("No build_restore_cmd found.")
elif len(defs) > 1 or defs[0] != "utils/backup.py":
    errors.append("build_restore_cmd must exist ONLY in utils/backup.py. Found in:\n  - " + "\n  - ".join(defs))

# 3) Запрещаем прямые вызовы pg_restore из handlers/*
direct_hits: list[str] = []
handlers_dir = os.path.join(REPO, "handlers")
if os.path.isdir(handlers_dir):
    for abs_path, rel in walk_files(handlers_dir):
        if "pg_restore" in read_text(abs_path):
            direct_hits.append(rel.replace("\\", "/"))
if direct_hits:
    errors.append("Direct pg_restore calls in handlers are forbidden:\n  - " + "\n  - ".join(direct_hits))

# Результат
if errors:
    print("ERRORS:\n" + "\n\n".join(errors))
    sys.exit(1)
else:
    print("OK: no legacy restore refs; single build_restore_cmd in utils/backup.py; no direct pg_restore in handlers.")
    sys.exit(0)

```

## Файл: utils\audit.py

```python
# utils/audit.py
from contextvars import ContextVar
from typing import Optional

_current_user_id: ContextVar[Optional[int]] = ContextVar("_current_user_id", default=None)

def set_current_user(user_id: Optional[int]) -> None:
    _current_user_id.set(user_id)

def get_current_user() -> Optional[int]:
    return _current_user_id.get()

```

## Файл: utils\backup.py

```python
# utils/backup.py
from __future__ import annotations

import os
import sys
import time
import shlex
import shutil
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Tuple, List

import requests
from xml.etree import ElementTree as ET
from email.utils import parsedate_to_datetime

from sqlalchemy import select
from sqlalchemy.engine.url import make_url

from database.db import get_session
from database.models import BackupSettings

# --- Google Drive (оставляем для совместимости; не используется при BACKUP_DRIVER=webdav)
from utils.gdrive_oauth import build_drive_oauth, upload_file, cleanup_old  # type: ignore
try:
    from utils.gdrive import build_drive as build_drive_sa  # type: ignore
except Exception:
    build_drive_sa = None  # noqa: F401

from config import (
    PG_DUMP_PATH,
    GOOGLE_AUTH_MODE,             # 'oauth' | 'sa' (для совместимости)
    GOOGLE_OAUTH_CLIENT_PATH,     # client_secret.json
    GOOGLE_OAUTH_TOKEN_PATH,      # token.json

    # --- новый блок для WebDAV / выбора драйвера ---
    BACKUP_DRIVER,                # "webdav" | "oauth" | "sa"
    WEBDAV_BASE_URL,              # напр. https://webdav.yandex.ru
    WEBDAV_USERNAME,              # логин/почта Яндекс
    WEBDAV_PASSWORD,              # пароль или пароль приложения
    WEBDAV_ROOT,                  # удалённая папка, напр. /botwb
)


# ------------------------- PG utils -------------------------

def parse_db_url(db_url: str) -> dict:
    u = make_url(db_url)
    return {
        "host": u.host or "localhost",
        "port": u.port or 5432,
        "user": u.username or "postgres",
        "password": u.password or "",
        "database": u.database,
    }


def _human_mb(path: str) -> float:
    try:
        return os.path.getsize(path) / (1024 * 1024)
    except Exception:
        return 0.0


def _resolve_pg_dump() -> str | None:
    """
    Ищем pg_dump в PATH; если нет — берём PG_DUMP_PATH, если он указывает на реально существующий файл.
    Так мы игнорируем «виндовые» пути, случайно попавшие в окружение.
    """
    found = shutil.which("pg_dump")
    if found:
        return found
    if PG_DUMP_PATH:
        p = Path(PG_DUMP_PATH)
        if p.is_file():
            return str(p.resolve())
    return None


# ------------------------- WebDAV client -------------------------

class WebDAVClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = (username, password)

    def _url(self, path: str) -> str:
        return f"{self.base}/{path.lstrip('/')}"

    def mkcol_recursive(self, remote_dir: str) -> None:
        parts = [p for p in remote_dir.strip("/").split("/") if p]
        cur = ""
        for seg in parts:
            cur = f"{cur}/{seg}"
            url = self._url(cur)
            r = self.session.request("MKCOL", url)
            # 201 — создано, 405 — уже существует, 409 — родителя нет (создадим на следующей итерации)
            if r.status_code in (201, 405, 409):
                continue

    def put_file(self, local_path: str, remote_path: str) -> None:
        url = self._url(remote_path)
        with open(local_path, "rb") as f:
            r = self.session.put(url, data=f)
        if r.status_code not in (200, 201, 204):
            raise RuntimeError(f"WebDAV PUT failed ({r.status_code}): {r.text[:400]}")

    def list_dir(self, remote_dir: str) -> List[dict]:
        """
        Список элементов каталога (1 уровень).
        Возвращает словари: {"href","name","is_dir","modified"(datetime|None)}.
        """
        url = self._url(remote_dir)
        headers = {"Depth": "1", "Content-Type": "text/xml; charset=utf-8"}
        body = """<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:displayname />
    <d:getlastmodified />
    <d:resourcetype />
  </d:prop>
</d:propfind>"""
        r = self.session.request("PROPFIND", url, data=body.encode("utf-8"), headers=headers)
        if r.status_code != 207:
            raise RuntimeError(f"WebDAV PROPFIND failed ({r.status_code}): {r.text[:400]}")

        out: List[dict] = []
        ns = {"d": "DAV:"}
        root = ET.fromstring(r.text)
        for resp in root.findall("d:response", ns):
            href_el = resp.find("d:href", ns)
            if href_el is None:
                continue
            href = href_el.text or ""
            prop = resp.find("d:propstat/d:prop", ns)
            if prop is None:
                continue
            name_el = prop.find("d:displayname", ns)
            name = name_el.text if name_el is not None else ""
            rtype = prop.find("d:resourcetype", ns)
            is_dir = rtype is not None and rtype.find("d:collection", ns) is not None
            mod_el = prop.find("d:getlastmodified", ns)
            modified_dt = None
            if mod_el is not None and mod_el.text:
                try:
                    modified_dt = parsedate_to_datetime(mod_el.text)
                    if modified_dt.tzinfo is None:
                        modified_dt = modified_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    modified_dt = None

            # пропускаем сам каталог
            if href.rstrip("/").endswith(remote_dir.strip("/")):
                continue

            if not name:
                name = href.rstrip("/").split("/")[-1]

            out.append({"href": href, "name": name, "is_dir": is_dir, "modified": modified_dt})
        return out

    def delete(self, remote_path: str) -> None:
        url = self._url(remote_path)
        r = self.session.delete(url)
        if r.status_code not in (200, 204):
            raise RuntimeError(f"WebDAV DELETE failed ({r.status_code}): {r.text[:400]}")


def _webdav_upload_and_cleanup(
        local_path: str,
        filename: str,
        retention_days: int,
        name_prefix: str,
) -> Tuple[str, int]:
    """
    Заливает файл на WEBDAV_ROOT и удаляет старые с тем же префиксом имени.
    Возвращает (remote_path, deleted_count).
    """
    if not WEBDAV_BASE_URL or not WEBDAV_USERNAME or not WEBDAV_PASSWORD:
        raise RuntimeError("WebDAV not configured: WEBDAV_BASE_URL/WEBDAV_USERNAME/WEBDAV_PASSWORD are required")

    client = WebDAVClient(WEBDAV_BASE_URL, WEBDAV_USERNAME, WEBDAV_PASSWORD)

    remote_dir = WEBDAV_ROOT or "/"
    client.mkcol_recursive(remote_dir)

    remote_path = f"{remote_dir.rstrip('/')}/{filename}"
    client.put_file(local_path, remote_path)

    deleted = 0
    if retention_days > 0:
        try:
            items = client.list_dir(remote_dir)
            cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
            for it in items:
                if it.get("is_dir"):
                    continue
                nm = it.get("name") or ""
                mod = it.get("modified")
                if not nm.startswith(name_prefix):
                    continue
                if mod and mod < cutoff:
                    href = it.get("href") or ""
                    # href вида /botwb/filename.backup → берём относительный путь
                    rel = "/" + href.lstrip("/").split("/", 1)[-1] if href.startswith("/") else href
                    try:
                        client.delete(rel)
                        deleted += 1
                    except Exception:
                        continue
        except Exception:
            # Не считаем ошибку очистки фатальной для бэкапа
            pass

    return remote_path, deleted


# ------------------------- Основной бэкап -------------------------

async def run_backup(db_url: str) -> Tuple[bool, str]:
    """
    Делает pg_dump и отправляет в хранилище по BACKUP_DRIVER:
      - 'webdav' → Яндекс.Диск/любой WebDAV
      - 'oauth'  → Google Drive (личный)
      - 'sa'     → Google Drive (Service Account на Shared Drive)
    """
    # Охранный флаг: бэкап только на сервере
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        return False, "Backups are disabled on non-server host (HOST_ROLE != server)"

    # 1) Настройки
    async with get_session() as s:
        st = (await s.execute(select(BackupSettings).where(BackupSettings.id == 1))).scalar_one_or_none()
        if not st:
            return False, "Backup settings not found (id=1)"
        if not st.enabled:
            return False, "Backups disabled"

        params = parse_db_url(db_url)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fname = f"{params['database']}_{ts}.backup"

        # 2) Дамп во временный файл
        with tempfile.TemporaryDirectory() as tmpdir:
            fpath = os.path.join(tmpdir, fname)

            env = os.environ.copy()
            if params["password"]:
                env["PGPASSWORD"] = params["password"]

            pg_dump_bin = _resolve_pg_dump()
            if not pg_dump_bin:
                return False, "pg_dump not found on PATH and PG_DUMP_PATH is invalid"

            cmd = [
                pg_dump_bin,
                "-h", params["host"],
                "-p", str(params["port"]),
                "-U", params["user"],
                "-d", params["database"],
                "-F", "c",
                "-Z", "9",
                "-f", fpath,
            ]

            kw = dict(check=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if sys.platform.startswith("win"):
                kw["creationflags"] = 0x08000000  # CREATE_NO_WINDOW

            t0 = time.monotonic()
            try:
                subprocess.run(cmd, timeout=900, **kw)
            except subprocess.TimeoutExpired:
                return False, "pg_dump timeout (900s)"
            except subprocess.CalledProcessError as e:
                return False, f"pg_dump failed: {e.stderr.decode(errors='ignore')[:400]}"

            duration = round(time.monotonic() - t0, 2)
            size_mb = _human_mb(fpath)

            # 3) Загрузка по драйверу
            driver = (BACKUP_DRIVER or "oauth").lower()
            try:
                if driver == "webdav":
                    remote_path, deleted = _webdav_upload_and_cleanup(
                        fpath,
                        fname,
                        st.retention_days,
                        name_prefix=params["database"],
                    )
                    msg = (f"OK: {fname} uploaded to WebDAV ({remote_path}), "
                           f"size={size_mb:.2f} MB, duration={duration}s, deleted {deleted} old")

                elif driver == "sa":
                    if not build_drive_sa:
                        return False, "Service Account mode requested but utils.gdrive is missing"
                    if not st.gdrive_sa_json:
                        return False, "Service Account JSON is not configured in backup_settings"
                    drive = build_drive_sa(st.gdrive_sa_json)
                    if not st.gdrive_folder_id:
                        return False, "Google Drive not configured: Folder ID is empty"
                    file_id = upload_file(drive, fpath, fname, st.gdrive_folder_id)
                    try:
                        deleted = cleanup_old(drive, st.gdrive_folder_id, st.retention_days, name_prefix=params["database"])
                        msg = (f"OK: {fname} uploaded (id={file_id}), "
                               f"size={size_mb:.2f} MB, duration={duration}s, deleted {deleted} old")
                    except Exception as e:
                        msg = (f"OK: {fname} uploaded (id={file_id}), "
                               f"size={size_mb:.2f} MB, duration={duration}s; cleanup failed: {e}")

                else:
                    # 'oauth'
                    if not st.gdrive_folder_id:
                        return False, "Google Drive not configured: Folder ID is empty"
                    drive = build_drive_oauth(GOOGLE_OAUTH_CLIENT_PATH, GOOGLE_OAUTH_TOKEN_PATH)
                    file_id = upload_file(drive, fpath, fname, st.gdrive_folder_id)
                    try:
                        deleted = cleanup_old(drive, st.gdrive_folder_id, st.retention_days, name_prefix=params["database"])
                        msg = (f"OK: {fname} uploaded (id={file_id}), "
                               f"size={size_mb:.2f} MB, duration={duration}s, deleted {deleted} old")
                    except Exception as e:
                        msg = (f"OK: {fname} uploaded (id={file_id}), "
                               f"size={size_mb:.2f} MB, duration={duration}s; cleanup failed: {e}")
            except Exception as e:
                return False, f"Upload failed ({driver}): {e}"

        # 4) Сохраняем статус
        st.last_run_at = datetime.utcnow()
        st.last_status = msg[:500]
        await s.commit()

    return True, msg


# -------------------- Restore command builder (server-only) --------------------

def build_restore_cmd(filepath: str) -> str:
    """
    Собирает команду восстановления строго для сервера через системный скрипт.
    """
    # Разрешаем restore только на сервере
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        raise RuntimeError("Restore доступен только на сервере (HOST_ROLE != server)")

    if sys.platform.startswith("win"):
        raise RuntimeError("Restore недоступен на Windows")

    restore_path = os.environ.get("RESTORE_SCRIPT_PATH")
    if not restore_path or not (os.path.isfile(restore_path) and os.access(restore_path, os.X_OK)):
        raise RuntimeError("RESTORE_SCRIPT_PATH не задан или не исполняем")

    return f"sudo -n {shlex.quote(restore_path)} {shlex.quote(filepath)}"


# --- Backward compatibility aliases -----------------------------------------

async def make_backup_and_maybe_upload(db_url: str):
    return await run_backup(db_url)

async def backup_now(db_url: str):
    return await run_backup(db_url)

```

## Файл: utils\gdrive.py

```python
# utils/gdrive.py
from __future__ import annotations
import io
from datetime import datetime, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from typing import Optional

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

def build_drive(sa_json: dict):
    creds = service_account.Credentials.from_service_account_info(sa_json, scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def upload_file(drive, local_path: str, file_name: str, folder_id: str) -> str:
    file_metadata = {"name": file_name, "parents": [folder_id]}
    media = MediaIoBaseUpload(open(local_path, "rb"), mimetype="application/octet-stream", chunksize=1024*1024, resumable=True)
    file = drive.files().create(body=file_metadata, media_body=media, fields="id,name").execute()
    return file["id"]

def cleanup_old(drive, folder_id: str, older_than_days: int, name_prefix: Optional[str] = None) -> int:
    """Удалить файлы в папке старше N дней (по createdTime). Возвращает кол-во удалённых."""
    if older_than_days <= 0:
        return 0
    cutoff = (datetime.utcnow() - timedelta(days=older_than_days)).isoformat("T") + "Z"
    q = f"'{folder_id}' in parents and createdTime < '{cutoff}' and trashed = false"
    if name_prefix:
        q += f" and name contains '{name_prefix}'"
    page_token = None
    deleted = 0
    while True:
        resp = drive.files().list(q=q, spaces="drive", fields="nextPageToken, files(id,name)", pageToken=page_token).execute()
        for f in resp.get("files", []):
            drive.files().delete(fileId=f["id"]).execute()
            deleted += 1
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return deleted

```

## Файл: utils\gdrive_oauth.py

```python
# utils/gdrive_oauth.py
from __future__ import annotations
import os
from typing import Optional, List
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def build_drive_oauth(client_secret_path: str, token_path: str):
    """
    Авторизация по OAuth (личный Google Drive).
    Сохраняет/читает токен из token_path.
    """
    creds: Optional[Credentials] = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
            # Откроет браузер на локальной машине; на сервере используем уже готовый token.json
            creds = flow.run_local_server(port=0)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds, cache_discovery=False)

def upload_file(drive, local_path: str, filename: str, folder_id: str) -> str:
    from googleapiclient.http import MediaFileUpload
    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(local_path, resumable=True)
    file = drive.files().create(body=file_metadata, media_body=media, fields="id").execute()
    return file["id"]

def cleanup_old(drive, folder_id: str, keep_days: int, name_prefix: Optional[str] = None) -> int:
    """
    Удаляет файлы старше keep_days в указанной папке.
    Работает по дате модификации. Чтобы не усложнять — удаляем по имени-префиксу (если задан).
    """
    import datetime as dt

    # Запросим файлы из папки
    q_parts: List[str] = [f"'{folder_id}' in parents", "trashed = false"]
    if name_prefix:
        q_parts.append(f"name contains '{name_prefix}'")
    q = " and ".join(q_parts)

    files = []
    page_token = None
    while True:
        resp = drive.files().list(
            q=q,
            spaces="drive",
            fields="nextPageToken, files(id, name, modifiedTime)",
            pageToken=page_token,
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    if keep_days <= 0:
        # Ничего не удаляем
        return 0

    threshold = dt.datetime.utcnow() - dt.timedelta(days=keep_days)
    threshold_iso = threshold.replace(microsecond=0).isoformat() + "Z"

    removed = 0
    for f in files:
        # modifiedTime в ISO, сравним строкой — доп. запрос не нужен
        if f.get("modifiedTime", "") < threshold_iso:
            try:
                drive.files().delete(fileId=f["id"]).execute()
                removed += 1
            except Exception:
                pass
    return removed

```

## Файл: utils\google_sheets.py

```python
# utils/google_sheets.py
async def export_to_sheets(*args, **kwargs):
    # позже добавим реальную интеграцию
    return True

```

## Файл: utils\notifications.py

```python
# utils/notifications.py
async def notify_manager(*args, **kwargs):
    return True

```

## Файл: utils\pagination.py

```python
from typing import List
from aiogram.types import InlineKeyboardButton


def build_pagination_keyboard(
        page: int,
        page_size: int,
        total: int,
        prev_cb_prefix: str,
        next_cb_prefix: str,
        prev_text: str = "◀ Предыдущая",
        next_text: str = "Следующая ▶",
        noop_cb: str = "noop",
) -> List[InlineKeyboardButton]:
    """
    Возвращает ОДНУ строку кнопок пагинации (List[InlineKeyboardButton]).
    Если страниц нет — возвращает пустой список [].

    Правила:
      - aiogram v3 требует именованные аргументы у InlineKeyboardButton.
      - Центральная кнопка (N/M) и "заблокированные" стрелки используют callback 'noop'.
      - Ожидается, что вызывающая сторона добавляет полученную строку в inline_keyboard:
          row = build_pagination_keyboard(...);  if row: rows.append(row)
    """
    if page_size <= 0:
        raise ValueError("page_size должен быть > 0")

    # ceil(total / page_size) без math
    total_pages = max(1, -(-total // page_size))

    # Пагинация не нужна
    if total <= page_size or total_pages <= 1:
        return []

    # В какую сторону можем листать
    has_prev = page > 1
    has_next = page < total_pages

    prev_cb = f"{prev_cb_prefix}:{page-1}" if has_prev else noop_cb
    next_cb = f"{next_cb_prefix}:{page+1}" if has_next else noop_cb

    row: List[InlineKeyboardButton] = [
        InlineKeyboardButton(
            text=(prev_text if has_prev else "⛔"),
            callback_data=prev_cb,
        ),
        InlineKeyboardButton(
            text=f"{page}/{total_pages}",
            callback_data=noop_cb,
        ),
        InlineKeyboardButton(
            text=(next_text if has_next else "⛔"),
            callback_data=next_cb,
        ),
    ]
    return row

```

## Файл: utils\validators.py

```python
# utils/validators.py
def validate_positive_int(value: int) -> bool:
    return isinstance(value, int) and value > 0

```

## Файл: utils\__init__.py

```python

```

