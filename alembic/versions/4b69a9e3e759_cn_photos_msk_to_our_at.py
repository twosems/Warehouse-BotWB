from alembic import op
import sqlalchemy as sa

# --- identifiers ---
revision = "4b69a9e3e759"
down_revision = "d3783dd38de1"  # merge-head
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # 1) Таблица фото для CN (создадим, если её ещё нет)
    if "cn_purchase_photos" not in insp.get_table_names():
        op.create_table(
            "cn_purchase_photos",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column(
                "cn_purchase_id",
                sa.Integer(),
                sa.ForeignKey("cn_purchases.id", ondelete="CASCADE"),
                nullable=False,
                index=True,
            ),
            sa.Column("file_id", sa.String(length=256), nullable=False),
            sa.Column("caption", sa.String(length=512), nullable=True),
            sa.Column(
                "uploaded_at",
                sa.DateTime(),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            ),
            sa.Column("uploaded_by_user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=True),
        )
        op.create_index(
            "ix_cn_purchase_photos_purchase_id",
            "cn_purchase_photos",
            ["cn_purchase_id"],
        )

    # 2) Колонка to_our_at в msk_inbound_docs (если ещё нет)
    cols = [c["name"] for c in insp.get_columns("msk_inbound_docs")]
    if "to_our_at" not in cols:
        op.add_column("msk_inbound_docs", sa.Column("to_our_at", sa.DateTime(), nullable=True))


def downgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # Откатываем колонку
    cols = [c["name"] for c in insp.get_columns("msk_inbound_docs")]
    if "to_our_at" in cols:
        op.drop_column("msk_inbound_docs", "to_our_at")

    # Откатываем таблицу фото (и индекс), если есть
    if "cn_purchase_photos" in insp.get_table_names():
        # индекс мог и не существовать, поэтому аккуратно
        try:
            op.drop_index("ix_cn_purchase_photos_purchase_id", table_name="cn_purchase_photos")
        except Exception:
            pass
        op.drop_table("cn_purchase_photos")
