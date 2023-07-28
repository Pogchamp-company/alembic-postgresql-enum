"""
Alembic extension to generate ALTER TYPE .. ADD VALUE statements to update
SQLAlchemy enums.

"""

from typing import List, Tuple, Any, Iterable, TYPE_CHECKING

import alembic.autogenerate
import alembic.operations.base
import alembic.operations.ops
import sqlalchemy
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

from .get_enum_data import get_connection, EnumNamesToValues, EnumNamesToTableReferences


@alembic.operations.base.Operations.register_operation("sync_enum_values")
class SyncEnumValuesOp(alembic.operations.ops.MigrateOperation):
    operation_name = 'change_enum_variants'

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
    def _set_enum_values(cls, connection: 'Connection',
                         schema: str,
                         enum_name: str,
                         new_values: List[str],
                         affected_columns: 'List[Tuple[str, str]]',
                         ):
        enum_type_name = f"{schema}.{enum_name}"
        temporary_enum_name = f"{enum_name}_old"

        query_str = (
            f"""ALTER TYPE {enum_type_name} RENAME TO {temporary_enum_name};"""
            f"""CREATE TYPE {enum_type_name} AS ENUM({', '.join(f"'{value}'" for value in new_values)});"""
        )

        for table_name, column_name in affected_columns:
            query_str += (
                # fixme default is dropped but not being restored
                f"""ALTER TABLE {table_name} ALTER COLUMN {column_name} DROP DEFAULT;"""
                f"""ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {enum_type_name} USING {column_name}::text::{enum_type_name};"""
            )

        query_str += f"""DROP TYPE {temporary_enum_name}"""

        for q in query_str.split(';'):
            connection.execute(sqlalchemy.text(q))

    @classmethod
    def _update_affected_columns(cls, connection: 'Connection',
                                 schema: str,
                                 affected_columns: 'List[Tuple[str, str]]',
                                 enum_values_to_rename: 'Iterable[Tuple[str, str]]'
                                 ):
        for (old_value, new_value) in enum_values_to_rename:
            for table_name, column_name in affected_columns:
                connection.execute(sqlalchemy.text(f'''
                    UPDATE {schema}.{table_name} SET {column_name} = '{new_value}' WHERE {column_name} = '{old_value}'
                '''))

    @classmethod
    def sync_enum_values(
            cls,
            operations,
            schema: str,
            enum_name: str,
            new_values: List[str],
            affected_columns: 'List[Tuple[str, str]]' = None,
            enum_values_to_rename: 'Iterable[Tuple[str, str]]' = tuple()
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
        :param enum_values_to_rename:
            Iterable of tuples containing old_name and new_name
            enum_values_to_rename=[
                ('tree', 'three') # to fix typo
            ]
        """
        with get_connection(operations) as connection:
            if enum_values_to_rename:
                for (old_value, _) in enum_values_to_rename:
                    new_values.append(old_value)
                cls._set_enum_values(connection, schema, enum_name, new_values, affected_columns)

                cls._update_affected_columns(connection, schema, affected_columns, enum_values_to_rename)

                for (old_value, _) in enum_values_to_rename:
                    new_values.remove(old_value)
                cls._set_enum_values(connection, schema, enum_name, new_values, affected_columns)

                return

            cls._set_enum_values(connection, schema, enum_name, new_values, affected_columns)

    def to_diff_tuple(self) -> 'Tuple[Any, ...]':
        return self.operation_name, self.old_values, self.new_values, self.affected_columns


@alembic.autogenerate.render.renderers.dispatch_for(SyncEnumValuesOp)
def render_sync_enum_value_op(autogen_context: AutogenContext, op: SyncEnumValuesOp):
    return (f"op.sync_enum_values({op.schema!r}, {op.name!r}, {op.new_values!r},\n"
            f"                    {op.affected_columns!r},\n"
            f"                    enum_values_to_rename=())")


def sync_changed_enums(defined_enums: EnumNamesToValues, declared_enums: EnumNamesToValues,
                       table_references: EnumNamesToTableReferences,
                       schema: str, upgrade_ops: UpgradeOps):
    for enum_name, new_values in declared_enums.items():
        if enum_name not in defined_enums:
            # That is work for create_new_enums function
            continue

        old_values = defined_enums[enum_name]

        if new_values == old_values:
            # Enum definition and declaration are in sync
            continue

        affected_columns = table_references[enum_name]
        op = SyncEnumValuesOp(schema, enum_name, list(old_values), list(new_values),
                              [column_reference.to_tuple() for column_reference in affected_columns])
        upgrade_ops.ops.append(op)
