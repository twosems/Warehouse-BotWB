# database/db.py
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import select
from database.models import Base, Warehouse

from config import DB_URL

engine = create_async_engine(DB_URL, echo=False, future=True)
SessionFactory = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@asynccontextmanager
async def get_session() -> AsyncSession:
    async with SessionFactory() as session:
        try:
            yield session
        finally:
            await session.close()

# --- НОВОЕ: гарантируем наличие складов СПб и Томск ---
async def ensure_core_data():
    needed = ["Санкт-Петербург", "Томск"]
    async with get_session() as session:
        existing = (await session.execute(select(Warehouse))).scalars().all()
        existing_names = {w.name for w in existing}
        to_add = [Warehouse(name=name, is_active=True) for name in needed if name not in existing_names]
        if to_add:
            session.add_all(to_add)
            await session.commit()
