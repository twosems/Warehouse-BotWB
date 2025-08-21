# database/db.py
# Async engine + session, аудит (JSON-safe), core-данные, и дефолты видимости меню.

from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncGenerator, Optional

from sqlalchemy import select, event
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy.inspection import inspect as sa_inspect

from config import DB_URL
from database.models import Base, Warehouse, AuditLog, AuditAction
from database.menu_visibility import ensure_menu_visibility_defaults

# ---- Engine & session factory ----
engine = create_async_engine(DB_URL, echo=False, future=True)
SessionFactory = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)

# ---- Текущий пользователь для аудита (ставим из middleware/handler) ----
_current_audit_user_id: ContextVar[Optional[int]] = ContextVar("current_audit_user_id", default=None)


def set_audit_user(user_id: Optional[int]) -> None:
    """
    Установить текущего пользователя для записи в AuditLog.user_id.
    Вызывай, например, в middleware перед обработкой апдейта.
    """
    _current_audit_user_id.set(user_id)


# ---- Public API ----
async def init_db() -> None:
    """
    Создаёт недостающие таблицы, регистрирует слушатели аудита
    и гарантирует дефолтные настройки видимости меню.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Аудит (после create_all, чтобы таблица audit_logs точно была)
    register_audit_listeners()

    # Дефолтные настройки видимости меню (безопасно вызывать повторно)
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


# ---- Audit helpers (JSON-safe) ----
import enum
from datetime import datetime, date, time
from decimal import Decimal


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
