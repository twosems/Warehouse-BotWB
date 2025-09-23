import asyncio
from sqlalchemy import text
from database.db import engine

async def main():
    async with engine.begin() as conn:
        sql = """
        SELECT t.typname, e.enumlabel
        FROM pg_type t
        JOIN pg_enum e ON e.enumtypid = t.oid
        WHERE t.typname IN ('supply_status','pack_doc_status_enum')
        ORDER BY t.typname, e.enumsortorder
        """
        rows = await conn.execute(text(sql))
        for typ, label in rows:
            print(f"{typ} => {label}")

if __name__ == "__main__":
    asyncio.run(main())
