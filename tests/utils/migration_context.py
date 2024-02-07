from typing import Union, List

from alembic.runtime.migration import MigrationContext
from sqlalchemy import MetaData
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy import Connection
from sqlalchemy.dialects import postgresql


def default_migration_options(target_schema: Union[MetaData, List[MetaData]]) -> dict:
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


def create_migration_context(connection: "Connection", target_schema: Union[MetaData, List[MetaData]]):
    return MigrationContext.configure(
        connection=connection,
        opts=default_migration_options(target_schema),
        dialect=postgresql.dialect,
    )
