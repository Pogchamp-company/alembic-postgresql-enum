from typing import Iterable, Union

import alembic
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps, CreateTableOp

from alembic_postgresql_enum.add_create_type_false import add_create_type_false
from alembic_postgresql_enum.add_postgres_using_to_text import add_postgres_using_to_text
from alembic_postgresql_enum.detection_of_changes import sync_changed_enums, create_new_enums, drop_unused_enums
from alembic_postgresql_enum.get_enum_data import get_defined_enums, get_declared_enums


@alembic.autogenerate.comparators.dispatch_for("schema")
def compare_enums(autogen_context: AutogenContext, upgrade_ops: UpgradeOps, schema_names: Iterable[Union[str, None]]):
    """
    Walk the declared SQLAlchemy schema for every referenced Enum, walk the PG
    schema for every defined Enum, then generate SyncEnumValuesOp migrations
    for each defined enum that has changed new entries when compared to its
    declared version.
    """
    add_create_type_false(upgrade_ops)
    add_postgres_using_to_text(upgrade_ops)

    schema_names = list(schema_names)

    # Issue #40
    # Add schema if it is gonna be created inside the migration
    for operations_group in upgrade_ops.ops:
        if isinstance(operations_group, CreateTableOp) and operations_group.schema not in schema_names:
            schema_names.append(operations_group.schema)

    for schema in schema_names:
        default_schema = autogen_context.dialect.default_schema_name
        if schema is None:
            schema = default_schema

        definitions = get_defined_enums(autogen_context.connection, schema)
        declarations = get_declared_enums(autogen_context.metadata, schema, default_schema, autogen_context.connection)

        create_new_enums(definitions, declarations.enum_values, schema, upgrade_ops)

        drop_unused_enums(definitions, declarations.enum_values, schema, upgrade_ops)

        sync_changed_enums(definitions, declarations.enum_values,
                           declarations.enum_table_references, schema, upgrade_ops)
