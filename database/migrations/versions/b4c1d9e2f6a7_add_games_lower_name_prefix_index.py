"""add lower(name) prefix index for games search

Revision ID: b4c1d9e2f6a7
Revises: 5f8d5d9e7c21
Create Date: 2026-04-03 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b4c1d9e2f6a7"
down_revision: Union[str, Sequence[str], None] = "5f8d5d9e7c21"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


INDEX_NAME = "idx_games_lower_name_prefix"
CREATE_INDEX_SQL = f"""
CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME}
ON games (lower(name) text_pattern_ops);
"""
DROP_INDEX_SQL = f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME};"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    with op.get_context().autocommit_block():
        op.execute(sa.text(CREATE_INDEX_SQL))


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    with op.get_context().autocommit_block():
        op.execute(sa.text(DROP_INDEX_SQL))
