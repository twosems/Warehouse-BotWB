# bot/utils/yadisk_client.py
from __future__ import annotations

import os
from typing import List, Dict, Optional

import requests


API_BASE = "https://cloud-api.yandex.net/v1/disk"


class YaDiskError(RuntimeError):
    pass


class YaDisk:
    """
    Минималистичный REST-клиент для Яндекс.Диска.
    Поддерживает: создание каталога (в т.ч. дерева), листинг, загрузку файла, удаление.
    """

    def __init__(self, token: str):
        if not token:
            raise ValueError("YADISK_TOKEN is empty")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"OAuth {token}"})

    # --- internal ---------------------------------------------------------

    @staticmethod
    def _join(*parts: str) -> str:
        """
        Безопасная склейка путей:
        _join('/a', 'b', 'c.txt') -> '/a/b/c.txt'
        """
        clean = []
        for p in parts:
            if not p:
                continue
            p = p.replace("\\", "/")
            clean.append(p.strip("/"))
        return "/" + "/".join(clean) if clean else "/"

    def _url(self, path: str) -> str:
        return f"{API_BASE}{path}"

    # --- public -----------------------------------------------------------

    def ensure_dir(self, path: str) -> None:
        """Создать папку (идемпотентно)."""
        r = self.session.put(self._url("/resources"), params={"path": path})
        if r.status_code in (201, 409):  # 201 — создано, 409 — уже существует
            return
        try:
            r.raise_for_status()
        except Exception as e:
            raise YaDiskError(f"ensure_dir failed: {r.text}") from e

    def ensure_tree(self, path: str) -> None:
        """Идемпотентно создаёт всю иерархию /a/b/c."""
        norm = (path or "").replace("\\", "/").strip("/")
        cur = ""
        for part in (norm.split("/") if norm else []):
            cur = self._join(cur, part)
            r = self.session.put(self._url("/resources"), params={"path": cur})
            if r.status_code in (201, 409):  # создано / уже существует
                continue
            try:
                r.raise_for_status()
            except Exception as e:
                raise YaDiskError(f"ensure_dir({cur}) failed: {r.text}") from e

    def list(self, path: str, limit: int = 1000) -> List[Dict]:
        """
        Список объектов внутри каталога.

        Основной запрос: с сортировкой по убыванию modified и полями (md5/size и т.п.).
        На некоторых аккаунтах такой запрос может вернуть 403 — в этом случае
        делаем фолбэк на минимальный запрос без fields/sort.
        """
        # «Богатый» запрос
        params = {
            "path": path,
            "limit": limit,
            "sort": "-modified",
            "fields": "_embedded.items.name,_embedded.items.type,"
                      "_embedded.items.path,_embedded.items.modified,"
                      "_embedded.items.size,_embedded.items.md5",
        }
        r = self.session.get(self._url("/resources"), params=params)

        # Фолбэк на минимальный вариант при 403
        if r.status_code == 403:
            r = self.session.get(self._url("/resources"), params={"path": path, "limit": limit})

        try:
            r.raise_for_status()
        except Exception as e:
            raise YaDiskError(f"list failed: {r.text}") from e

        data = r.json()
        return data.get("_embedded", {}).get("items", [])

    def delete(self, path: str, permanently: bool = True) -> None:
        """Удалить файл/папку."""
        r = self.session.delete(
            self._url("/resources"),
            params={"path": path, "permanently": "true" if permanently else "false"},
        )
        if r.status_code in (202, 204):
            return
        try:
            r.raise_for_status()
        except Exception as e:
            raise YaDiskError(f"delete failed: {r.text}") from e

    def get_upload_url(self, remote_path: str, overwrite: bool = True) -> str:
        r = self.session.get(
            self._url("/resources/upload"),
            params={"path": remote_path, "overwrite": "true" if overwrite else "false"},
        )
        r.raise_for_status()
        href = r.json().get("href")
        if not href:
            raise YaDiskError("No 'href' in upload response")
        return href

    def upload_file(
            self,
            local_path: str,
            remote_dir: str,
            remote_name: Optional[str] = None,
            overwrite: bool = True,
    ) -> str:
        """
        Загрузить локальный файл в папку `remote_dir`.
        Возвращает полный удалённый путь.
        """
        if not os.path.isfile(local_path):
            raise FileNotFoundError(local_path)

        if not remote_name:
            remote_name = os.path.basename(local_path)

        # Убедимся, что каталог существует (идемпотентно)
        self.ensure_tree(remote_dir)

        remote_path = self._join(remote_dir, remote_name)
        href = self.get_upload_url(remote_path, overwrite=overwrite)

        # На пред-подписанный href аутентификация не нужна.
        with open(local_path, "rb") as f:
            r = requests.put(href, data=f)

        if r.status_code not in (201, 202, 204):
            try:
                r.raise_for_status()
            except Exception as e:
                raise YaDiskError(f"upload_file failed: {r.text}") from e

        return remote_path



