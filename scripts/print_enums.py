import asyncio
from sqlalchemy import text
from database.db import engine

async def main():
    async with engine.begin() as conn:
        # Печать значений наших enum-типов
        sql_enums = """
        SELECT t.typname, e.enumlabel
        FROM pg_type t
        JOIN pg_enum e ON e.enumtypid = t.oid
        WHERE t.typname IN ('supply_status','pack_doc_status_enum','packdocstatus')
        ORDER BY t.typname, e.enumsortorder
        """
        rows = await conn.execute(text(sql_enums))
        print("=== ENUM labels ===")
        for typ, label in rows:
            print(f"{typ} => {label}")

        # Фактический тип колонки pack_docs.status
        sql_coltype = """
        SELECT t.typname
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_type t ON a.atttypid = t.oid
        WHERE c.relname = 'pack_docs' AND a.attname = 'status'
        """
        coltype = await conn.scalar(text(sql_coltype))
        print("\n=== Column type ===")
        print("pack_docs.status type:", coltype)

if __name__ == "__main__":
    asyncio.run(main())
