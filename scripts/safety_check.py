# scripts/safety_check.py
from __future__ import annotations
import os, re, sys

REPO = os.path.abspath(os.path.dirname(__file__) + "/..")

# Папки/файлы, которые не сканируем
SKIP_DIRS = {
    ".git", ".idea", ".vscode", "__pycache__", "venv", ".venv",
    "node_modules", "dist", "build", ".mypy_cache"
}
SKIP_FILES = {
    os.path.normcase(os.path.relpath(__file__, REPO)),     # сам чекер
    "scripts/safety_check.ps1",                            # возможный PS-скрипт
}

def walk_files(base: str):
    for root, dirs, files in os.walk(base):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for f in files:
            rel = os.path.normcase(os.path.relpath(os.path.join(root, f), REPO))
            if rel in SKIP_FILES:
                continue
            if not f.endswith((".py", ".txt", ".md", ".cfg", ".ini", ".yml", ".yaml", ".sh")):
                continue
            yield os.path.join(root, f), rel

def read_text(path: str) -> str:
    try:
        return open(path, "r", encoding="utf-8", errors="ignore").read()
    except Exception:
        return ""

errors: list[str] = []

# 1) Запрещаем любые упоминания старого пути/скрипта wb_db_restore.sh
legacy_re = re.compile(r"wb_db_restore\.sh")
legacy_hits: list[str] = []
for abs_path, rel in walk_files(REPO):
    txt = read_text(abs_path)
    if legacy_re.search(txt):
        legacy_hits.append(rel)

if legacy_hits:
    errors.append("Legacy path found (wb_db_restore.sh) in:\n  - " + "\n  - ".join(sorted(set(legacy_hits))))

# 2) Ровно ОДНА реализация build_restore_cmd — в utils/backup.py
def_re = re.compile(r"^\s*def\s+build_restore_cmd\s*\(", re.MULTILINE)
defs: list[str] = []
for abs_path, rel in walk_files(REPO):
    if rel.endswith(".py") and def_re.search(read_text(abs_path)):
        defs.append(rel.replace("\\", "/"))

if len(defs) == 0:
    errors.append("No build_restore_cmd found.")
elif len(defs) > 1 or defs[0] != "utils/backup.py":
    errors.append("build_restore_cmd must exist ONLY in utils/backup.py. Found in:\n  - " + "\n  - ".join(defs))

# 3) Запрещаем прямые вызовы pg_restore из handlers/*
direct_hits: list[str] = []
handlers_dir = os.path.join(REPO, "handlers")
if os.path.isdir(handlers_dir):
    for abs_path, rel in walk_files(handlers_dir):
        if "pg_restore" in read_text(abs_path):
            direct_hits.append(rel.replace("\\", "/"))
if direct_hits:
    errors.append("Direct pg_restore calls in handlers are forbidden:\n  - " + "\n  - ".join(direct_hits))

# Результат
if errors:
    print("ERRORS:\n" + "\n\n".join(errors))
    sys.exit(1)
else:
    print("OK: no legacy restore refs; single build_restore_cmd in utils/backup.py; no direct pg_restore in handlers.")
    sys.exit(0)
