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
