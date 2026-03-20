from __future__ import annotations

from logging.config import fileConfig
from pathlib import Path
import sys

from alembic import context
from sqlalchemy import create_engine, pool


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

plugin_root = Path(config.attributes.get("plugin_root") or Path(__file__).resolve().parents[1]).resolve()
for candidate in (str(plugin_root.parent), str(plugin_root)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from official_weather.models import WeatherDeviceBinding  # noqa: E402


configured_url = str(config.attributes.get("sqlalchemy_url"))
version_table = str(config.attributes.get("version_table") or "alembic_version")
target_metadata = WeatherDeviceBinding.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=configured_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        version_table=version_table,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configured_connection = config.attributes.get("connection")
    if configured_connection is not None:
        context.configure(
            connection=configured_connection,
            target_metadata=target_metadata,
            compare_type=True,
            version_table=version_table,
        )

        with context.begin_transaction():
            context.run_migrations()
        return

    connectable = create_engine(
        configured_url,
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            version_table=version_table,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
