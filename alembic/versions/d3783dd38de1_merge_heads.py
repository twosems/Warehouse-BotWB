from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "d3783dd38de1"
down_revision = ("add_menu_items_enum_2cats", "fd0dbb27ea7a")
branch_labels = None
depends_on = None


def upgrade():
    # merge point; схема не меняется
    pass


def downgrade():
    # откат merge'а ничего не делает
    pass
