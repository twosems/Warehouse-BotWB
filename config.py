# config.py
import os
from dotenv import load_dotenv

# Загружаем переменные из .env в окружение
load_dotenv()


def getenv_bool(name: str, default: bool = False) -> bool:
    """
    Получить переменную окружения как bool.
    Значения '1', 'true', 'yes', 'y', 'on' → True.
    """
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


# --- Telegram ---
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "0") or 0)

# --- Database ---
DB_URL = os.getenv(
    "DB_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/warehouse_db",
)

# --- Google (Sheets / Drive) ---
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")  # путь к credentials.json (если используете Drive/Sheets)
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")  # опционально: для экспорта в таблицы
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")  # ID папки в Drive (опц.)
GOOGLE_DRIVE_PUBLIC = getenv_bool("GOOGLE_DRIVE_PUBLIC", False)

# --- Backups ---
BACKUP_DIR = os.getenv("BACKUP_DIR", "backups")
PG_DUMP_PATH = os.getenv("PG_DUMP_PATH")  # Windows: полный путь к pg_dump.exe; Linux: обычно не нужен

# --- Timezone / Logging ---
TIMEZONE = os.getenv("timezone", "Europe/Berlin")  # часовой пояс по умолчанию
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")   # уровень логов
# --- Google Drive auth mode ---
# 'oauth' (личный Drive) или 'sa' (service account). По умолчанию oauth.
GOOGLE_AUTH_MODE = os.getenv("GOOGLE_AUTH_MODE", "oauth")

# Для OAuth:
GOOGLE_OAUTH_CLIENT_PATH = os.getenv("GOOGLE_OAUTH_CLIENT_PATH", "client_secret.json")
GOOGLE_OAUTH_TOKEN_PATH = os.getenv("GOOGLE_OAUTH_TOKEN_PATH", "token.json")
import os

RESTORE_DRIVER = os.getenv("RESTORE_DRIVER", "linux")  # linux|windows
RESTORE_SCRIPT_PATH = os.getenv("RESTORE_SCRIPT_PATH", "/usr/local/bin/wb_db_restore.sh")

DB_NAME  = os.getenv("DB_NAME", "warehouse_db")
DB_USER  = os.getenv("DB_USER", "postgres")
DB_HOST  = os.getenv("DB_HOST", "localhost")
DB_PORT  = int(os.getenv("DB_PORT", "5432"))

PG_BIN   = os.getenv("PG_BIN")  # только для Windows, можно None
