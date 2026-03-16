"""baseline schema bootstrap

Revision ID: d7bef0e0c44a
Revises:
Create Date: 2026-03-14 02:29:24.697773

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from database.models import Base


# revision identifiers, used by Alembic.
revision: str = "d7bef0e0c44a"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Bootstrap baseline schema from SQLAlchemy metadata."""
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)

    if bind.dialect.name == "postgresql":
        op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        try:
            op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"))
        except Exception:
            # Managed Postgres providers may not allow this extension.
            pass


def downgrade() -> None:
    """Downgrade is intentionally a no-op for baseline bootstrap."""
    pass
