# utils/backup.py
from __future__ import annotations

import os
import sys
import time
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Tuple

from sqlalchemy import select
from sqlalchemy.engine.url import make_url

from database.db import get_session
from database.models import BackupSettings

# OAuth (личный Google Drive)
from utils.gdrive_oauth import build_drive_oauth, upload_file, cleanup_old

# Режим сервис-аккаунта (Shared Drive) — опционально
try:
    from utils.gdrive import build_drive as build_drive_sa  # type: ignore
except Exception:
    build_drive_sa = None  # noqa: F401

from config import (
    PG_DUMP_PATH,
    GOOGLE_AUTH_MODE,            # 'oauth' (по умолчанию) или 'sa'
    GOOGLE_OAUTH_CLIENT_PATH,    # путь к client_secret.json
    GOOGLE_OAUTH_TOKEN_PATH,     # путь к token.json
)


def parse_db_url(db_url: str) -> dict:
    """Разбирает DB_URL на компоненты для pg_dump."""
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
    Возвращает путь к pg_dump:
      1) сначала пробуем найти в PATH текущего хоста;
      2) затем используем PG_DUMP_PATH ТОЛЬКО если файл реально существует на ЭТОМ хосте.
    Так мы игнорируем "виндовые" пути, случайно попавшие из .env.
    """
    found = shutil.which("pg_dump")
    if found:
        return found

    if PG_DUMP_PATH:
        p = Path(PG_DUMP_PATH)
        if p.is_file():
            return str(p.resolve())

    return None


async def run_backup(db_url: str) -> Tuple[bool, str]:
    """
    Делает pg_dump и грузит в Google Drive согласно backup_settings (id=1).
    Возвращает (ok, message).
    """

    # Охранный флаг: не запускаем бэкап на не-серверных хостах.
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        return False, "Backups are disabled on non-server host (HOST_ROLE != server)"

    # 1) Читаем настройки
    async with get_session() as s:
        st = (
            await s.execute(select(BackupSettings).where(BackupSettings.id == 1))
        ).scalar_one_or_none()

        if not st:
            return False, "Backup settings not found (id=1)"
        if not st.enabled:
            return False, "Backups disabled"
        if not st.gdrive_folder_id:
            return False, "Google Drive not configured: Folder ID is empty"

        params = parse_db_url(db_url)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fname = f"{params['database']}_{ts}.backup"

        # 2) Делаем дамп во временный файл
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
                "-F", "c",           # custom формат .backup
                "-Z", "9",           # сжатие в pg_dump
                "-f", fpath,
            ]

            kw = dict(check=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if sys.platform.startswith("win"):
                kw["creationflags"] = 0x08000000  # CREATE_NO_WINDOW

            t0 = time.monotonic()
            try:
                subprocess.run(cmd, timeout=900, **kw)  # таймаут 15 минут
            except subprocess.TimeoutExpired:
                return False, "pg_dump timeout (900s)"
            except subprocess.CalledProcessError as e:
                return False, f"pg_dump failed: {e.stderr.decode(errors='ignore')[:400]}"

            duration = round(time.monotonic() - t0, 2)
            size_mb = _human_mb(fpath)

            # 3) Загрузка в Google Drive
            try:
                mode = (GOOGLE_AUTH_MODE or "oauth").lower()

                if mode == "sa":
                    if not build_drive_sa:
                        return False, "Service Account mode requested but utils.gdrive is missing"
                    if not st.gdrive_sa_json:
                        return False, "Service Account JSON is not configured in backup_settings"
                    # ВНИМАНИЕ: для SA нужен Shared Drive
                    drive = build_drive_sa(st.gdrive_sa_json)
                else:
                    # OAuth: личный Drive. Нужны client_secret.json и token.json
                    drive = build_drive_oauth(GOOGLE_OAUTH_CLIENT_PATH, GOOGLE_OAUTH_TOKEN_PATH)

                file_id = upload_file(drive, fpath, fname, st.gdrive_folder_id)
            except Exception as e:
                return False, f"Drive upload failed: {e}"

            # 4) Очистка старых файлов (ретеншн)
            try:
                deleted = cleanup_old(
                    drive,
                    st.gdrive_folder_id,
                    st.retention_days,
                    name_prefix=params["database"],
                )
                msg = (
                    f"OK: {fname} uploaded (id={file_id}), "
                    f"size={size_mb:.2f} MB, duration={duration}s, deleted {deleted} old"
                )
            except Exception as e:
                msg = (
                    f"OK: {fname} uploaded (id={file_id}), "
                    f"size={size_mb:.2f} MB, duration={duration}s; cleanup failed: {e}"
                )

        # 5) Сохраняем статус
        st.last_run_at = datetime.utcnow()
        st.last_status = msg[:500]
        await s.commit()

    return True, msg


# --- Backward compatibility aliases -----------------------------------------

async def make_backup_and_maybe_upload(db_url: str):
    """Deprecated alias. Use run_backup(db_url) instead."""
    return await run_backup(db_url)

async def backup_now(db_url: str):
    """Deprecated alias. Use run_backup(db_url) instead."""
    return await run_backup(db_url)
