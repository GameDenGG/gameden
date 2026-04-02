"""add featured_media to games

Revision ID: 5f8d5d9e7c21
Revises: a12f5d7c9e31
Create Date: 2026-04-02 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5f8d5d9e7c21"
down_revision: Union[str, Sequence[str], None] = "a12f5d7c9e31"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _existing_columns(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {str(column["name"]) for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    """Upgrade schema."""
    if "featured_media" not in _existing_columns("games"):
        op.add_column("games", sa.Column("featured_media", sa.JSON(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    if "featured_media" in _existing_columns("games"):
        op.drop_column("games", "featured_media")
