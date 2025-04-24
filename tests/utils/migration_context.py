from typing import Any, Dict, List, Union

from alembic.runtime.migration import MigrationContext
from sqlalchemy import MetaData
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy import Connection
from sqlalchemy.dialects import postgresql


def default_migration_options(
    target_schema: Union[MetaData, List[MetaData]],
) -> dict[str, Any]:
    return {
        "alembic_module_prefix": "op.",
        "sqlalchemy_module_prefix": "sa.",
        "compare_type": True,
        "compare_server_default": True,
        "target_metadata": target_schema,
        "upgrade_token": "upgrades",
        "downgrade_token": "downgrades",
        "include_schemas": True,
    }


def create_migration_context(
    connection: "Connection",
    target_schema: Union[MetaData, List[MetaData]],
    *,
    migration_options_overrides: Dict[str, Any] = {},
) -> MigrationContext:
    """Create a migration context using the provided schema and optional configuration overrides."""
    migration_options = default_migration_options(target_schema)
    migration_options.update(migration_options_overrides)
    return MigrationContext.configure(
        connection=connection,
        opts=migration_options,
        dialect=postgresql.dialect,
    )
