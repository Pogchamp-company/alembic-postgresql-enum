from typing import Iterable, Union

import alembic
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps

from alembic_postgresql_enum.enum_alteration import sync_changed_enums
from alembic_postgresql_enum.enum_creation import create_new_enums
from alembic_postgresql_enum.enum_deletion import drop_unused_enums
from alembic_postgresql_enum.get_enum_data import get_defined_enums, get_declared_enums


@alembic.autogenerate.comparators.dispatch_for("schema")
def compare_enums(autogen_context: AutogenContext, upgrade_ops: UpgradeOps, schema_names: Iterable[Union[str, None]]):
    """
    Walk the declared SQLAlchemy schema for every referenced Enum, walk the PG
    schema for every defined Enum, then generate SyncEnumValuesOp migrations
    for each defined enum that has grown new entries when compared to its
    declared version.
    Enums that don't exist in the database yet are ignored, since
    SQLAlchemy/Alembic will create them as part of the usual migration process.
    """
    for schema in schema_names:
        default_schema = autogen_context.dialect.default_schema_name
        if schema is None:
            schema = default_schema

        definitions = get_defined_enums(autogen_context.connection, schema)
        declarations = get_declared_enums(autogen_context.metadata, schema, default_schema, autogen_context.dialect)

        create_new_enums(definitions, declarations.enum_values, schema, upgrade_ops)

        drop_unused_enums(definitions, declarations.enum_values, schema, upgrade_ops)

        sync_changed_enums(definitions, declarations.enum_values,
                           declarations.enum_table_references, schema, upgrade_ops)
