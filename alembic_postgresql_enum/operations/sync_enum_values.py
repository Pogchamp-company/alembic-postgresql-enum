from typing import List, Tuple, Any, Iterable, TYPE_CHECKING

import alembic.autogenerate
import alembic.operations.base
import alembic.operations.ops
from alembic.autogenerate.api import AutogenContext
from sqlalchemy.exc import DataError

from alembic_postgresql_enum.sql_commands.column_default import get_column_default, drop_default, set_default, \
    rename_default_if_required
from alembic_postgresql_enum.sql_commands.comparison_operators import (create_comparison_operators,
                                                                       drop_comparison_operators)
from alembic_postgresql_enum.sql_commands.enum_type import cast_old_enum_type_to_new, drop_type, rename_type, \
    create_type

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

from alembic_postgresql_enum.connection import get_connection
from alembic_postgresql_enum.get_enum_data import TableReference, ColumnType


@alembic.operations.base.Operations.register_operation("sync_enum_values")
class SyncEnumValuesOp(alembic.operations.ops.MigrateOperation):
    operation_name = 'change_enum_variants'

    def __init__(self,
                 schema: str,
                 name: str,
                 old_values: List[str],
                 new_values: List[str],
                 affected_columns: List[TableReference]
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
    def _set_enum_values(cls,
                         connection: 'Connection',
                         schema: str,
                         enum_name: str,
                         new_values: List[str],
                         affected_columns: List[TableReference],
                         enum_values_to_rename: List[Tuple[str, str]]
                         ):
        enum_type_name = f"{schema}.{enum_name}"
        temporary_enum_name = f"{enum_name}_old"

        rename_type(connection, schema, enum_name, temporary_enum_name)
        create_type(connection, schema, enum_name, new_values)

        create_comparison_operators(connection, schema, enum_name, temporary_enum_name, enum_values_to_rename)

        for table_reference in affected_columns:
            column_default = table_reference.existing_server_default

            if column_default is not None:
                drop_default(connection, schema, table_reference)

            try:
                cast_old_enum_type_to_new(connection,
                                          schema, table_reference, enum_type_name, enum_values_to_rename)
            except DataError as error:
                raise ValueError(
                    f'''New enum values can not be set due to some row containing reference to old enum value.
                        Please consider using enum_values_to_rename parameter or "
                    f"updating/deleting these row before calling sync_enum_values.'''
                ) from error

            if column_default is not None:
                column_default = rename_default_if_required(column_default, enum_name,
                                                            enum_values_to_rename)

                set_default(connection, schema, table_reference, column_default)

        drop_comparison_operators(connection, schema, enum_name, temporary_enum_name)
        drop_type(connection, schema, temporary_enum_name)

    @classmethod
    def sync_enum_values(cls,
                         operations,
                         schema: str,
                         enum_name: str,
                         new_values: List[str],
                         affected_columns: List[Tuple[str, str]],
                         enum_values_to_rename: Iterable[Tuple[str, str]] = tuple()
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
            If there was server default with old_name it will be renamed accordingly
        """
        enum_values_to_rename = list(enum_values_to_rename)

        with get_connection(operations) as connection:
            table_references = []
            for affected_column in affected_columns:
                if isinstance(affected_column, tuple):  # This is considered old style
                    table_name = affected_column[0]
                    column_name = affected_column[1]
                    if len(affected_column) > 2:
                        column_type = affected_column[2]
                    else:
                        column_type = ColumnType.COMMON
                    column_default = get_column_default(connection, schema, table_name, column_name)
                    table_references.append(TableReference(
                        table_name, column_name, column_type, column_default
                    ))

                elif isinstance(affected_column, TableReference):
                    table_references.append(affected_column)
                else:
                    raise ValueError("Affected columns must contain tuples or TableReferences")

            cls._set_enum_values(connection, schema, enum_name, new_values, table_references, enum_values_to_rename)

    def to_diff_tuple(self) -> Tuple[Any, ...]:
        return self.operation_name, self.old_values, self.new_values, self.affected_columns

    @property
    def is_column_type_import_needed(self) -> bool:
        return any((affected_column.is_column_type_import_needed for affected_column in self.affected_columns))


@alembic.autogenerate.render.renderers.dispatch_for(SyncEnumValuesOp)
def render_sync_enum_value_op(autogen_context: AutogenContext, op: SyncEnumValuesOp):
    if op.is_column_type_import_needed:
        autogen_context.imports.add('from alembic_postgresql_enum import ColumnType')
    autogen_context.imports.add('from alembic_postgresql_enum import TableReference')

    return (f"op.sync_enum_values({op.schema!r}, {op.name!r}, {op.new_values!r},\n"
            f"                    {op.affected_columns!r},\n"
            f"                    enum_values_to_rename=[])")
