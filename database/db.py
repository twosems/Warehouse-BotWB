# database/db.py
# Async engine + session, авто-создание БД, устойчивый пул, JSON-safe аудит,
# дефолты видимости меню и хелперы для restore.

from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncGenerator, Optional
import enum
import pathlib
from datetime import datetime, date, time
from decimal import Decimal

from sqlalchemy import select, event, text, func
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import Session

from config import DB_URL
from database.models import Base, Warehouse, AuditLog, AuditAction, SupplyStatus
# для хелпера available_packed
from database.models import (
    StockMovement, ProductStage,
    Supply, SupplyItem,
)

# ---------------------------------------------------------------------
# Engine & Session
# ---------------------------------------------------------------------
engine = create_async_engine(
    DB_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,   # прозрачно чинит "connection is closed"
    pool_recycle=1800,    # раз в 30 минут обновляет соединения
)
SessionFactory = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


async def reset_db_engine() -> None:
    """
    Полностью пересобрать engine/SessionFactory (например, сразу после restore).
    """
    global engine, SessionFactory
    try:
        await engine.dispose(close=True)
    except Exception:
        pass
    engine = create_async_engine(
        DB_URL,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_recycle=1800,
    )
    SessionFactory = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


async def ping_db() -> None:
    """
    Лёгкая проверка доступности БД и «прогрев» пула.
    """
    async with SessionFactory() as s:
        await s.execute(text("SELECT 1"))


async def ensure_database_exists() -> None:
    """
    Если целевой базы (из DB_URL) нет — создаёт её.
    Если template1 не в UTF8 (как на Windows с WIN1251), создаём из template0
    c LC_COLLATE/LC_CTYPE = 'C', чтобы не было конфликта кодировок.
    """
    url = make_url(DB_URL)
    target_db = url.database
    owner = url.username or "postgres"

    admin_url = url.set(database="postgres")
    admin_engine = create_async_engine(
        admin_url,
        echo=False,
        future=True,
        pool_pre_ping=True,
    )
    try:
        async with admin_engine.connect() as conn:
            # Уже есть?
            exists = await conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :n"),
                {"n": target_db},
            )
            if exists:
                return

            # Смотрим кодировку template1
            row = await conn.execute(
                text("""
                    SELECT pg_encoding_to_char(encoding) AS enc,
                           datcollate, datctype
                    FROM pg_database
                    WHERE datname = 'template1'
                """)
            )
            enc, collate, ctype = row.first()

            def _create_db(sync_conn):
                # отдельное sync-подключение в AUTOCOMMIT
                with sync_conn.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as ac:
                    if (enc or "").upper() == "UTF8":
                        # можно спокойно от template1
                        ac.exec_driver_sql(
                            f'CREATE DATABASE "{target_db}" '
                            f'OWNER "{owner}" ENCODING \'UTF8\' TEMPLATE template1'
                        )
                    else:
                        # универсально: от template0 с нейтральной локалью
                        ac.exec_driver_sql(
                            f'CREATE DATABASE "{target_db}" '
                            f'OWNER "{owner}" ENCODING \'UTF8\' '
                            f'LC_COLLATE \'C\' LC_CTYPE \'C\' '
                            f'TEMPLATE template0'
                        )

            await conn.run_sync(_create_db)
    finally:
        await admin_engine.dispose(close=True)


# ---------------------------------------------------------------------
# Раннер SQL-патчей (минимальная замена Alembic)
# ---------------------------------------------------------------------
PATCHES_DIR = pathlib.Path(__file__).resolve().parents[1] / "sql" / "patches"

# --- замени вашу apply_sql_patches на эту ---
async def apply_sql_patches():
    """
    Простой раннер SQL-патчей.
    - Папка: sql/patches
    - Имена файлов: 001_*.sql, 002_*.sql, ...
    - Хранит версию в таблице schema_version (max(version))
    - Игнорирует пустые/комментные строки; исполняет скрипт целиком (поддержка DO $$ ... $$)
    """
    PATCHES_DIR.mkdir(parents=True, exist_ok=True)
    async with engine.begin() as conn:
        # таблица версий
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS schema_version(
                version INTEGER PRIMARY KEY
            )
        """))
        current = await conn.execute(text("SELECT COALESCE(MAX(version), 0) FROM schema_version"))
        (current_version,) = current.fetchone()

        patches = sorted(PATCHES_DIR.glob("*.sql"))
        for p in patches:
            # ожидаем имена вида 001_*.sql
            try:
                v = int(p.stem.split("_", 1)[0])
            except Exception:
                continue
            if v <= current_version:
                continue

            raw = p.read_text(encoding="utf-8")

            # убираем пустые строки и строки-комментарии ('-- ...') — остальное оставляем как есть,
            # чтобы не ломать DO $$ ... $$ и т.п.
            cleaned_lines = []
            for ln in raw.splitlines():
                s = ln.strip()
                if not s or s.startswith("--"):
                    continue
                cleaned_lines.append(ln)
            cleaned = "\n".join(cleaned_lines).strip()

            if cleaned:
                # ВАЖНО: исполняем скрипт одной строкой, чтобы работали многооператорные блоки
                await conn.exec_driver_sql(cleaned)

            # фиксируем применённую версию (даже если скрипт был пустым после чистки)
            await conn.execute(
                text("INSERT INTO schema_version(version) VALUES (:v)"),
                {"v": v}
            )


# ---------------------------------------------------------------------
# Текущий пользователь для аудита (ставим из middleware/handler)
# ---------------------------------------------------------------------
_current_audit_user_id: ContextVar[Optional[int]] = ContextVar("current_audit_user_id", default=None)


def set_audit_user(user_id: Optional[int]) -> None:
    """
    Установить текущего пользователя для записи в AuditLog.user_id.
    Вызывай, например, в middleware перед обработкой апдейта.
    """
    _current_audit_user_id.set(user_id)


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
async def init_db():
    """
    Инициализация схемы без Alembic:
    - Создаём БД (если нет)
    - Создаём таблицы/ENUM-типы из ORM
    - Применяем SQL-патчи (ALTER/ADD VALUE и т.п.)
    - Регистрируем аудит-слушатели
    """
    # 1) убедиться, что база существует
    await ensure_database_exists()

    # 2) создать таблицы/типы по моделям
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 3) применить SQL-патчи (если есть)
    await apply_sql_patches()

    # 4) подписать аудит
    register_audit_listeners()


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with SessionFactory() as session:
        try:
            yield session
        finally:
            await session.close()


async def ensure_core_data() -> None:
    """
    Создаём базовые склады, если их нет.
    """
    needed = ["Санкт-Петербург", "Томск"]
    async with get_session() as session:
        existing = (await session.execute(select(Warehouse))).scalars().all()
        existing_names = {w.name for w in existing}
        to_add = [Warehouse(name=name, is_active=True) for name in needed if name not in existing_names]
        if to_add:
            session.add_all(to_add)
            await session.commit()


# ---------------------------------------------------------------------
# Stock helpers (ENUM-безопасно)
# ---------------------------------------------------------------------
async def available_packed(session: AsyncSession, warehouse_id: int, product_id: int) -> int:
    """
    Доступный PACKED = фактический PACKED - сумма qty в активных поставках
    (status in assembling|assembled|in_transit) по этому складу/товару.
    """
    fact = await session.scalar(
        select(func.coalesce(func.sum(StockMovement.qty), 0))
        .where(
            StockMovement.warehouse_id == warehouse_id,
            StockMovement.product_id == product_id,
            StockMovement.stage == ProductStage.packed,
            )
    )

    active_status = (SupplyStatus.assembling, SupplyStatus.assembled, SupplyStatus.in_transit)
    reserved = await session.scalar(
        select(func.coalesce(func.sum(SupplyItem.qty), 0))
        .join(Supply, Supply.id == SupplyItem.supply_id)
        .where(
            Supply.warehouse_id == warehouse_id,
            Supply.status.in_(active_status),
            SupplyItem.product_id == product_id,
            )
    )
    return int((fact or 0) - (reserved or 0))


# ---------------------------------------------------------------------
# Audit helpers (JSON-safe)
# ---------------------------------------------------------------------
def _to_plain(value):
    """Рекурсивно приводит к JSON-совместимым типам (enum -> .value, даты -> ISO, Decimal -> float и т.п.)."""
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_plain(v) for v in value]
    # На всякий случай: Row -> dict
    try:
        from sqlalchemy.engine import Row
        if isinstance(value, Row):
            return {k: _to_plain(value[k]) for k in value.keys()}
    except Exception:
        pass
    return value


def _row_as_dict_plain(obj) -> dict:
    insp = sa_inspect(obj)
    data = {}
    for attr in insp.mapper.column_attrs:
        key = attr.key
        data[key] = _to_plain(getattr(obj, key))
    return data


def _diff_for_update_plain(obj) -> dict:
    insp = sa_inspect(obj)
    dif = {}
    for attr in insp.mapper.column_attrs:
        hist = insp.attrs[attr.key].history
        if hist.has_changes():
            old_val = hist.deleted[0] if hist.deleted else None
            new_val = hist.added[0] if hist.added else getattr(obj, attr.key)
            old_val = _to_plain(old_val)
            new_val = _to_plain(new_val)
            if old_val != new_val:
                dif[attr.key] = {"old": old_val, "new": new_val}
    return dif


def register_audit_listeners() -> None:
    """
    Подписка на ORM-события, чтобы писать AuditLog для INSERT/UPDATE/DELETE.
    Работает и с AsyncSession, т.к. слушатель висит на sync Session-классе.
    """
    @event.listens_for(Session, "after_flush")
    def _audit_after_flush(session: Session, flush_context) -> None:
        # чтобы не зациклиться
        def skip(obj) -> bool:
            return obj.__class__.__name__ == "AuditLog"

        conn = session.connection()
        uid = _current_audit_user_id.get()

        # INSERT
        for obj in session.new:
            if skip(obj):
                continue
            table = getattr(obj, "__tablename__", obj.__class__.__name__)
            pk = sa_inspect(obj).identity
            conn.execute(
                AuditLog.__table__.insert().values(
                    user_id=uid,
                    action=AuditAction.insert,
                    table_name=table,
                    record_pk=str(pk),
                    old_data=None,
                    new_data=_row_as_dict_plain(obj),  # JSON-safe
                    diff=None,
                )
            )

        # UPDATE
        for obj in session.dirty:
            if skip(obj) or not session.is_modified(obj, include_collections=False):
                continue
            table = getattr(obj, "__tablename__", obj.__class__.__name__)
            pk = sa_inspect(obj).identity
            dif = _diff_for_update_plain(obj)  # JSON-safe
            if not dif:
                continue
            conn.execute(
                AuditLog.__table__.insert().values(
                    user_id=uid,
                    action=AuditAction.update,
                    table_name=table,
                    record_pk=str(pk),
                    old_data={k: v["old"] for k, v in dif.items()},
                    new_data={k: v["new"] for k, v in dif.items()},
                    diff=dif,
                )
            )

        # DELETE
        for obj in session.deleted:
            if skip(obj):
                continue
            table = getattr(obj, "__tablename__", obj.__class__.__name__)
            pk = sa_inspect(obj).identity
            conn.execute(
                AuditLog.__table__.insert().values(
                    user_id=uid,
                    action=AuditAction.delete,
                    table_name=table,
                    record_pk=str(pk),
                    old_data=_row_as_dict_plain(obj),  # JSON-safe
                    new_data=None,
                    diff=None,
                )
            )
