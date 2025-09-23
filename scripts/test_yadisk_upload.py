# scripts/test_yadisk_upload.py
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from utils.yadisk_client import YaDisk  # класс REST-клиента

# Подтягиваем переменные из .env при локальном запуске
load_dotenv()


def require_vars():
    need = {
        "YADISK_TOKEN": os.getenv("YADISK_TOKEN"),
        "YADISK_DIR": os.getenv("YADISK_DIR"),
    }
    missing = [k for k, v in need.items() if not v]
    if missing:
        print(
            "Не заданы переменные окружения: " + ", ".join(missing)
            + "\nДобавьте их в .env и повторите запуск.",
            file=sys.stderr,
            )
        sys.exit(2)
    return need["YADISK_TOKEN"], need["YADISK_DIR"]


def normalize_remote(path: str) -> str:
    """Нормализует удалённый путь вида 'backups/x' -> '/backups/x'."""
    p = (path or "").replace("\\", "/").strip()
    if not p.startswith("/"):
        p = "/" + p
    # у корня оставляем одиночный слэш
    return "/" if p == "/" else p.rstrip("/")


def main():
    token, root_dir = require_vars()
    y = YaDisk(token)

    # Убедимся, что корневая папка и подпапка /tests существуют
    root = normalize_remote(root_dir)
    test_dir = f"{root}/tests"
    # Требуется метод ensure_tree в utils.yadisk_client.YaDisk
    y.ensure_tree(root)
    y.ensure_tree(test_dir)

    # Создаём локальный файл для загрузки
    local = Path("backups") / "yadisk_test.txt"
    local.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    local.write_text(f"ok {ts}\n", encoding="utf-8")

    # Загружаем файл в Ya.Disk
    remote = y.upload_file(str(local), test_dir, f"test_{ts}.txt", overwrite=True)
    print("Uploaded to:", remote)

    # Покажем несколько последних объектов в папке /tests
    items = y.list(test_dir, limit=5)
    for it in items:
        print(
            f"- {it.get('type')} {it.get('name')} "
            f"{it.get('size')} bytes modified {it.get('modified')}"
        )


if __name__ == "__main__":
    main()
