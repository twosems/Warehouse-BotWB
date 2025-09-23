# utils/backup.py
from __future__ import annotations
import os
import sys
import time
import shlex
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple
from sqlalchemy import select
from sqlalchemy.engine.url import make_url
from database.db import get_session
from database.models import BackupSettings
from config import (
    PG_DUMP_PATH,
    BACKUP_DRIVER,    # ожидаем "yadisk" (по умолчанию)
    BACKUP_DIR,       # локальная папка (если используешь)
    YADISK_TOKEN,
    YADISK_DIR,
    BACKUP_KEEP,
)
from utils.yadisk_client import YaDisk, YaDiskError


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
    """ Ищем pg_dump в PATH; если нет — используем PG_DUMP_PATH, если он указывает на реальный файл. """
    found = shutil.which("pg_dump")
    if found:
        return found
    if PG_DUMP_PATH:
        p = Path(PG_DUMP_PATH)
        if p.is_file():
            return str(p.resolve())
    return None


# ------------------------- Основной бэкап -------------------------

async def run_backup(db_url: str) -> Tuple[bool, str]:
    """
    Делает pg_dump и отправляет в Яндекс.Диск (REST).
    BACKUP_DRIVER должен быть 'yadisk' (или пустой — мы приведём к 'yadisk').
    """
    # Охранный флаг: бэкап только на сервере
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        return False, "Backups are disabled on non-server host (HOST_ROLE != server)"

    driver = (BACKUP_DRIVER or "yadisk").lower()
    if driver != "yadisk":
        return False, f"Unsupported BACKUP_DRIVER='{driver}'. Only 'yadisk' is supported in this build."

    async with get_session() as s:
        st = (await s.execute(select(BackupSettings).where(BackupSettings.id == 1))).scalar_one_or_none()
        if not st:
            return False, "Backup settings not found (id=1)"
        if not st.enabled:
            return False, "Backups disabled"

        params = parse_db_url(db_url)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fname = f"{params['database']}_{ts}.backup"

        with tempfile.TemporaryDirectory() as tmpdir:
            fpath = os.path.join(tmpdir, fname)

            env = os.environ.copy()
            if params["password"]:
                env["PGPASSWORD"] = params["password"]

            pg_dump_bin = _resolve_pg_dump()
            if not pg_dump_bin:
                return False, "pg_dump not found (PATH/PG_DUMP_PATH)"

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

            # Загрузка на Я.Диск + ротация
            try:
                yd = YaDisk(YADISK_TOKEN)
                yd.ensure_tree(YADISK_DIR)
                remote_path = yd.upload_file(fpath, YADISK_DIR)

                deleted = []
                if BACKUP_KEEP and BACKUP_KEEP > 0:
                    items = yd.list(YADISK_DIR, limit=1000)
                    files = [x for x in items if x.get("type") == "file"]
                    # сортируем по имени (…_YYYYmmdd_HHMMSS.backup) — гарантированно детерминированно
                    files = sorted(files, key=lambda x: x.get("name", ""), reverse=True)
                    if len(files) > BACKUP_KEEP:
                        for old in files[BACKUP_KEEP:]:
                            yd.delete(old["path"], permanently=True)
                            deleted.append(old["name"])

                msg = (f"OK: {fname} uploaded to Yandex.Disk ({remote_path}), "
                       f"size={size_mb:.2f} MB, duration={duration}s, deleted {len(deleted)} old")
            except Exception as e:
                return False, f"Upload failed (yadisk): {e}"

        st.last_run_at = datetime.utcnow()
        st.last_status = msg[:500]
        await s.commit()

    return True, msg


# -------------------- Restore command builder --------------------

def build_restore_cmd(filepath: str) -> str:
    """ Собирает команду восстановления (только на сервере, через внешний скрипт). """
    if os.environ.get("HOST_ROLE") and os.environ["HOST_ROLE"] != "server":
        raise RuntimeError("Restore доступен только на сервере")

    if sys.platform.startswith("win"):
        raise RuntimeError("Restore недоступен на Windows")

    restore_path = os.environ.get("RESTORE_SCRIPT_PATH")
    if not restore_path or not (os.path.isfile(restore_path) and os.access(restore_path, os.X_OK)):
        raise RuntimeError("RESTORE_SCRIPT_PATH не задан или не исполняем")

    return f"sudo -n {shlex.quote(restore_path)} {shlex.quote(filepath)}"


# --- Алиасы для совместимости -----------------------------------

async def make_backup_and_maybe_upload(db_url: str):
    return await run_backup(db_url)

async def backup_now(db_url: str):
    return await run_backup(db_url)
