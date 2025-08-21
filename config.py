# config.py
import os
from dotenv import load_dotenv

load_dotenv()  # грузим .env из корня проекта

def getenv_bool(name: str, default: bool = False) -> bool:
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

# --- Backups ---
BACKUP_DIR = os.getenv("BACKUP_DIR", "backups")
PG_DUMP_PATH = os.getenv("PG_DUMP_PATH")  # на Linux обычно не нужен; на Windows можно указать полный путь к pg_dump.exe
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")  # ID папки в Drive (опционально)
GOOGLE_DRIVE_PUBLIC = getenv_bool("GOOGLE_DRIVE_PUBLIC", False)
