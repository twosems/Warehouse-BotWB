# utils/backup.py
from __future__ import annotations

import os
import gzip
import shutil
import subprocess
from datetime import datetime
from typing import Optional, Tuple

from sqlalchemy.engine.url import make_url

from config import (
    DB_URL,
    BACKUP_DIR,
    PG_DUMP_PATH,
    GOOGLE_CREDENTIALS_PATH,
    GOOGLE_DRIVE_FOLDER_ID,
    GOOGLE_DRIVE_PUBLIC,
)

# ====== ЛОКАЛЬНЫЙ ДАМП (pg_dump) ======

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _build_pg_dump_cmd(db_url: str, out_sql_path: str, pg_dump_path: Optional[str] = None) -> Tuple[list, dict]:
    url = make_url(db_url)  # понимает postgresql+asyncpg://...
    host = url.host or "localhost"
    port = str(url.port or 5432)
    user = url.username or "postgres"
    password = url.password or ""
    dbname = url.database or "postgres"

    cmd = [
        pg_dump_path or "pg_dump",
        "-h", host,
        "-p", port,
        "-U", user,
        "-d", dbname,
        "-F", "p",          # plain SQL
        "-f", out_sql_path, # сохранить сюда
    ]
    env = os.environ.copy()
    if password:
        env["PGPASSWORD"] = password  # не светим пароль в командной строке
    return cmd, env

def create_local_backup() -> Tuple[str, int]:
    """
    Делает дамп БД в BACKUP_DIR: <db>_YYYYmmdd_HHMMSS.sql.gz
    Возвращает (путь_к_файлу, размер_байт).
    """
    _ensure_dir(BACKUP_DIR)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dbname = make_url(DB_URL).database or "database"
    base = f"{dbname}_{stamp}"
    sql_path = os.path.join(BACKUP_DIR, f"{base}.sql")
    gz_path  = os.path.join(BACKUP_DIR, f"{base}.sql.gz")

    cmd, env = _build_pg_dump_cmd(DB_URL, sql_path, PG_DUMP_PATH)
    res = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"pg_dump failed: {res.stderr.strip() or res.stdout.strip()}")

    # Сжимаем в .gz и удаляем .sql
    with open(sql_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(sql_path)

    size = os.path.getsize(gz_path)
    return gz_path, size


# ====== GOOGLE DRIVE (опционально) ======

def _get_drive_service():
    """
    Возвращает авторизованный сервис Drive (v3).
    Ленивая загрузка Google SDK, чтобы локальный бэкап работал без них.
    """
    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
        from google.auth.transport.requests import Request
    except Exception as e:
        raise RuntimeError("Пакеты Google API не установлены. Выполните: "
                           "pip install google-api-python-client google-auth-oauthlib google-auth-httplib2") from e

    SCOPES = ["https://www.googleapis.com/auth/drive.file"]

    creds = None
    if not GOOGLE_CREDENTIALS_PATH:
        raise ValueError("GOOGLE_CREDENTIALS_PATH не задан (нужен для загрузки в Drive).")

    token_path = os.path.join(os.path.dirname(GOOGLE_CREDENTIALS_PATH), "token_drive.json")
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
                raise FileNotFoundError(f"credentials.json не найден по пути: {GOOGLE_CREDENTIALS_PATH}")
            flow = InstalledAppFlow.from_client_secrets_file(GOOGLE_CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    return build("drive", "v3", credentials=creds)

def upload_to_drive(file_path: str, folder_id: str) -> Tuple[str, Optional[str]]:
    """
    Загружает файл в Google Drive в указанную папку.
    Возвращает (file_id, webViewLink|None).
    Если GOOGLE_DRIVE_PUBLIC=True — включает доступ «по ссылке».
    """
    if not folder_id:
        raise ValueError("Не задан GOOGLE_DRIVE_FOLDER_ID")

    try:
        from googleapiclient.http import MediaFileUpload
    except Exception as e:
        raise RuntimeError("Пакеты Google API не установлены. Выполните: "
                           "pip install google-api-python-client google-auth-oauthlib google-auth-httplib2") from e

    service = _get_drive_service()

    file_metadata = {"name": os.path.basename(file_path), "parents": [folder_id]}
    media = MediaFileUpload(file_path, mimetype="application/gzip", resumable=True)
    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, webViewLink, webContentLink"
    ).execute()
    file_id = created["id"]
    link = created.get("webViewLink") or created.get("webContentLink")

    if GOOGLE_DRIVE_PUBLIC:
        try:
            service.permissions().create(
                fileId=file_id,
                body={"type": "anyone", "role": "reader"},
            ).execute()
            created = service.files().get(fileId=file_id, fields="webViewLink, webContentLink").execute()
            link = created.get("webViewLink") or created.get("webContentLink")
        except Exception:
            pass

    return file_id, link


# ====== ВСПОМОГАТЕЛЬНАЯ ОБЪЕДИНЁННАЯ ФУНКЦИЯ ======
def make_backup_and_maybe_upload() -> dict:
    """
    Делает локальный бэкап и, если задан GOOGLE_DRIVE_FOLDER_ID, грузит в Drive.
    """
    gz_path, size = create_local_backup()
    info = {
        "local_path": gz_path,
        "size": size,
        "drive_file_id": None,
        "drive_link": None,
    }
    if GOOGLE_DRIVE_FOLDER_ID:
        file_id, link = upload_to_drive(gz_path, GOOGLE_DRIVE_FOLDER_ID)
        info["drive_file_id"] = file_id
        info["drive_link"] = link
    return info
