from alembic import command
from alembic.config import Config

from config import DATABASE_URL
from database import direct_engine
from database.schema_guard import validate_scale_schema


def _alembic_config() -> Config:
    config = Config("alembic.ini")
    if DATABASE_URL:
        config.set_main_option("sqlalchemy.url", DATABASE_URL.replace("%", "%%"))
    return config


def setup_database() -> None:
    print("Applying schema via Alembic (upgrade head)...")
    command.upgrade(_alembic_config(), "head")

    report = validate_scale_schema(direct_engine)
    if not report.is_ready:
        raise RuntimeError(
            "Alembic migration completed but required schema is incomplete. "
            f"details={report.to_dict()}"
        )

    print("Schema is ready.")


if __name__ == "__main__":
    setup_database()
