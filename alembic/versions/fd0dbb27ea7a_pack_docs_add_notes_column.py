"""pack_docs: add notes column

Revision ID: fd0dbb27ea7a
Revises: d07a5ed359a8
Create Date: 2025-09-11
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "fd0dbb27ea7a"
down_revision: str | None = "d07a5ed359a8"
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column("pack_docs", sa.Column("notes", sa.String(length=255), nullable=True))

def downgrade() -> None:
    op.drop_column("pack_docs", "notes")
