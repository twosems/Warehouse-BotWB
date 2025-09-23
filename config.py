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
