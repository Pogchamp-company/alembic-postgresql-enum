"""
Alembic extension to generate ALTER TYPE .. ADD VALUE statements to update
SQLAlchemy enums.

"""

from typing import List, Tuple

import alembic.autogenerate
import alembic.operations.base
import alembic.operations.ops
import sqlalchemy

from .get_enum_data import get_connection, get_defined_enums, get_declared_enums


@alembic.operations.base.Operations.register_operation("sync_enum_values")
class SyncEnumValuesOp(alembic.operations.ops.MigrateOperation):
    def __init__(
            self,
            schema: str,
            name: str,
            old_values: List[str],
            new_values: List[str],
            affected_columns: 'List[Tuple[str, str]]',
    ):
        self.schema = schema
        self.name = name
        self.old_values = old_values
        self.new_values = new_values
        self.affected_columns = affected_columns

    def reverse(self):
        """
        See MigrateOperation.reverse().
        """
        return SyncEnumValuesOp(
            self.schema,
            self.name,
            old_values=self.new_values,
            new_values=self.old_values,
            affected_columns=self.affected_columns,
        )

    @classmethod
    def sync_enum_values(
            cls,
            operations,
            schema: str,
            enum_name: str,
            new_values: List[str],
            affected_columns: 'List[Tuple[str, str]]' = None,
    ):
        """
        Replace enum values with `new_values`
        :param operations:
            ...
        :param str schema:
            Schema name.
        :param enum_name:
            Enumeration type name.
        :param list new_values:
            List of enumeration values that should exist after this migration
            executes.
        :param list affected_columns:
            List of columns that references this enum.
            First value is table_name,
            second value is column_name
        """
        with get_connection(operations) as conn:
            enum_type_name = f"{schema}.{enum_name}"
            temporary_enum_name = f"{enum_name}_old"

            query_str = (
                f"""ALTER TYPE {enum_type_name} RENAME TO {temporary_enum_name};"""
                f"""CREATE TYPE {enum_type_name} AS ENUM({', '.join(f"'{value}'" for value in new_values)});"""
            )

            for table_name, column_name in affected_columns:
                query_str += (
                    f"""ALTER TABLE {table_name} ALTER COLUMN {column_name} DROP DEFAULT;"""
                    f"""ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {enum_type_name} USING {column_name}::text::{enum_type_name};"""
                )

            query_str += f"""DROP TYPE {temporary_enum_name}"""

            for q in query_str.split(';'):
                conn.execute(sqlalchemy.text(q))


@alembic.autogenerate.render.renderers.dispatch_for(SyncEnumValuesOp)
def render_sync_enum_value_op(autogen_context, op: SyncEnumValuesOp):
    return "op.sync_enum_values(%r, %r, %r, %r)" % (
        op.schema,
        op.name,
        op.new_values,
        op.affected_columns
    )


@alembic.autogenerate.comparators.dispatch_for("schema")
def compare_enums(autogen_context, upgrade_ops, schema_names):
    """
    Walk the declared SQLAlchemy schema for every referenced Enum, walk the PG
    schema for every defined Enum, then generate SyncEnumValuesOp migrations
    for each defined enum that has grown new entries when compared to its
    declared version.
    Enums that don't exist in the database yet are ignored, since
    SQLAlchemy/Alembic will create them as part of the usual migration process.
    """
    to_add = set()
    for schema in schema_names:
        default_schema = autogen_context.dialect.default_schema_name
        if schema is None:
            schema = default_schema

        defined = get_defined_enums(autogen_context.connection, schema)
        declared = get_declared_enums(autogen_context.metadata, schema, default_schema, autogen_context.dialect)

        for name, new_values in declared.enum_definitions.items():
            old_values = defined.enum_definitions.get(name)
            if name in defined.enum_definitions and new_values != old_values:
                affected_columns = frozenset(
                    (table_definition.table_name, table_definition.column_name)
                    for table_definition in declared.table_definitions
                    if table_definition.enum_name == name
                )
                to_add.add((schema, name, old_values, new_values, affected_columns))

    for schema, name, old_values, new_values, affected_columns in sorted(to_add):
        op = SyncEnumValuesOp(schema, name, list(old_values), list(new_values), list(affected_columns))
        upgrade_ops.ops.append(op)
