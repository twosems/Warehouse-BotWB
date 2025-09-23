"""supplies v1: statuses, boxes, files, audit fields

Revision ID: 20250922_supplies_v1
Revises: 4b69a9e3e759
Create Date: 2025-09-22 10:00:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20250922_supplies_v1"
down_revision = "4b69a9e3e759"
branch_labels = None
depends_on = None


def upgrade():
    # 1) ENUM supply_status
    op.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'supply_status') THEN
                CREATE TYPE supply_status AS ENUM (
                    'draft','queued','assembling','assembled','in_transit',
                    'archived_delivered','archived_returned','cancelled'
                );
            END IF;
        END$$;
    """)

    # 2) supplies: статус + реквизиты + таймстемпы
    with op.batch_alter_table("supplies") as batch:
        batch.add_column(sa.Column("status", sa.Enum(name="supply_status", create_type=False),
                                   nullable=False, server_default="draft"))
        batch.add_column(sa.Column("mp", sa.String(16), nullable=True))               # 'wb' | 'ozon'
        batch.add_column(sa.Column("mp_warehouse", sa.String(128), nullable=True))    # строкой для MVP
        batch.add_column(sa.Column("assigned_picker_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("comment", sa.String(), nullable=True))
        batch.add_column(sa.Column("queued_at", sa.DateTime(timezone=False)))
        batch.add_column(sa.Column("assembled_at", sa.DateTime(timezone=False)))
        batch.add_column(sa.Column("posted_at", sa.DateTime(timezone=False)))
        batch.add_column(sa.Column("delivered_at", sa.DateTime(timezone=False)))
        batch.add_column(sa.Column("returned_at", sa.DateTime(timezone=False)))
        batch.add_column(sa.Column("unposted_at", sa.DateTime(timezone=False)))

    op.create_index("ix_supplies_status", "supplies", ["status"])
    op.create_index("ix_supplies_warehouse", "supplies", ["warehouse_id"])

    # 3) supply_boxes
    op.create_table(
        "supply_boxes",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("supply_id", sa.Integer, sa.ForeignKey("supplies.id", ondelete="CASCADE"),
                  index=True, nullable=False),
        sa.Column("box_number", sa.Integer, nullable=False),  # 1..N
        sa.Column("sealed", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.UniqueConstraint("supply_id", "box_number", name="uq_supply_box_number")
    )

    # 4) supply_items: привязка к коробу и индекс на supply_id
    with op.batch_alter_table("supply_items") as batch:
        batch.add_column(sa.Column("box_id", sa.Integer,
                                   sa.ForeignKey("supply_boxes.id", ondelete="CASCADE"),
                                   nullable=True))
    op.create_index("ix_supply_items_supply", "supply_items", ["supply_id"])

    # 5) supply_files (PDF)
    op.create_table(
        "supply_files",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("supply_id", sa.Integer, sa.ForeignKey("supplies.id", ondelete="CASCADE"),
                  index=True, nullable=False),
        sa.Column("file_id", sa.String(256), nullable=False),
        sa.Column("filename", sa.String(255)),
        sa.Column("uploaded_by", sa.Integer, sa.ForeignKey("users.id")),
        sa.Column("uploaded_at", sa.DateTime(timezone=False),
                  server_default=sa.text("CURRENT_TIMESTAMP"))
    )


def downgrade():
    op.drop_table("supply_files")
    op.drop_index("ix_supply_items_supply", table_name="supply_items")
    with op.batch_alter_table("supply_items") as batch:
        batch.drop_column("box_id")
    op.drop_table("supply_boxes")

    op.drop_index("ix_supplies_status", table_name="supplies")
    op.drop_index("ix_supplies_warehouse", table_name="supplies")
    with op.batch_alter_table("supplies") as batch:
        for col in ["status","mp","mp_warehouse","assigned_picker_id","comment",
                    "queued_at","assembled_at","posted_at","delivered_at","returned_at","unposted_at"]:
            batch.drop_column(col)

    op.execute("DROP TYPE IF EXISTS supply_status")
