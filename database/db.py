# database/db.py
# Async engine + session, авто-создание БД, устойчивый пул, JSON-safe аудит,
# дефолты видимости меню и хелперы для restore.

from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncGenerator, Optional

import enum
from datetime import datetime, date, time
from decimal import Decimal

from sqlalchemy import select, event, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import Session

from config import DB_URL
from database.models import Base, Warehouse, AuditLog, AuditAction
from database.menu_visibility import ensure_menu_visibility_defaults


# ---------------------------
# Engine & session factory (устойчивый пул)
# ---------------------------
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



# ---------------------------
# Текущий пользователь для аудита (ставим из middleware/handler)
# ---------------------------
_current_audit_user_id: ContextVar[Optional[int]] = ContextVar("current_audit_user_id", default=None)


def set_audit_user(user_id: Optional[int]) -> None:
    """
    Установить текущего пользователя для записи в AuditLog.user_id.
    Вызывай, например, в middleware перед обработкой апдейта.
    """
    _current_audit_user_id.set(user_id)


# ---------------------------
# Public API
# ---------------------------
async def init_db() -> None:
    """
    Создаёт БД при её отсутствии, затем таблицы, регистрирует аудит
    и гарантирует дефолтные настройки видимости меню.
    """
    # 1) Если базы нет после DROP DATABASE — создадим её
    await ensure_database_exists()

    # 2) Создадим таблицы
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 3) Аудит (после create_all, чтобы таблица audit_logs точно была)
    register_audit_listeners()

    # 4) Дефолтные настройки видимости меню (безопасно вызывать повторно)
    async with get_session() as session:
        await ensure_menu_visibility_defaults(session)


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


# ---------------------------
# Audit helpers (JSON-safe)
# ---------------------------
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
