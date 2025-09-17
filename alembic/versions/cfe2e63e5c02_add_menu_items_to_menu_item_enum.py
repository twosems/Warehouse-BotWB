from alembic import op

# ревизии
revision = "add_menu_items_enum_2cats"
down_revision = "d07a5ed359a8"
 # возьми из вывода `alembic current`

def upgrade():
    for val in ("picking", "purchase_cn", "msk_warehouse"):
        op.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_type t
                JOIN pg_enum e ON t.oid = e.enumtypid
                WHERE t.typname = 'menu_item_enum' AND e.enumlabel = '{val}'
            ) THEN
                ALTER TYPE menu_item_enum ADD VALUE '{val}';
            END IF;
        END$$;
        """)

def downgrade():
    # удалять значения из ENUM нельзя — оставляем пустым
    pass
