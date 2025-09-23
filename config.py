# config.py
import os
from dotenv import load_dotenv

# Загружаем переменные из .env (локально); на сервере их задаёт systemd
load_dotenv()


def getenv_bool(name: str, default: bool = False) -> bool:
    """Прочитать переменную окружения как bool."""
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
# На всякий случай оставим совместимость со старым именем
POSTGRES_DSN = os.getenv("POSTGRES_DSN", DB_URL)

# --- Backups / pg_dump ---
BACKUP_DIR = os.getenv("BACKUP_DIR", "backups")
PG_DUMP_PATH = os.getenv("PG_DUMP_PATH")  # явный путь к pg_dump, если не в PATH (чаще нужно на Windows)

# --- Резервные копии: драйвер ---
# сейчас у нас единственная цель — Яндекс.Диск через REST API
BACKUP_DRIVER = (os.getenv("BACKUP_DRIVER", "yadisk") or "yadisk").strip().lower()

# --- Yandex.Disk (REST API) ---
YADISK_TOKEN = (os.getenv("YADISK_TOKEN") or "").strip()                  # OAuth-токен
YADISK_DIR = (os.getenv("YADISK_DIR", "/backups/malinawb_v2") or "").strip().rstrip("/")
BACKUP_TMP = (os.getenv("BACKUP_TMP", "/tmp/malinawb_backups") or "").strip()
try:
    BACKUP_KEEP = int(os.getenv("BACKUP_KEEP", "14"))
except Exception:
    BACKUP_KEEP = 14

# --- Timezone / Logging ---
TIMEZONE = (
        os.getenv("TIMEZONE")
        or os.getenv("timezone")
        or os.getenv("TZ")
        or "Europe/Berlin"
)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# --- Доп. переменные (если где-то используются) ---
DB_NAME = os.getenv("DB_NAME", "warehouse_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
PG_BIN = os.getenv("PG_BIN")  # только для Windows, если нужно явно задать bin-папку
