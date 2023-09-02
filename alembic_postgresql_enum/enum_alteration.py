"""
Alembic extension to generate ALTER TYPE .. ADD VALUE statements to update
SQLAlchemy enums.

"""
import logging
from typing import List, Tuple, Any, Iterable, TYPE_CHECKING, Union

import alembic.autogenerate
import alembic.operations.base
import alembic.operations.ops
import sqlalchemy
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps
from sqlalchemy.exc import DataError

from .sql_commands.comparison_operators import create_comparison_operators, drop_comparison_operators

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

from .get_enum_data import get_connection, EnumNamesToValues, EnumNamesToTableReferences, TableReference, ColumnType


@alembic.operations.base.Operations.register_operation("sync_enum_values")
class SyncEnumValuesOp(alembic.operations.ops.MigrateOperation):
    operation_name = 'change_enum_variants'

    def __init__(self,
                 schema: str,
                 name: str,
                 old_values: List[str],
                 new_values: List[str],
                 affected_columns: 'List[Union[Tuple[str, str], Tuple[str, str, ColumnType]]]'
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
    def _get_column_default(cls,
                            connection: 'Connection',
                            schema: str,
                            table_reference: TableReference,
                            ) -> Union[str, None]:
        """Result example: "'active'::order_status" """
        default_value = connection.execute(sqlalchemy.text(f"""
            SELECT column_default
            FROM information_schema.columns
            WHERE 
                table_schema = '{schema}' AND 
                table_name = '{table_reference.table_name}' AND 
                column_name = '{table_reference.column_name}';
        """)).scalar()
        return default_value

    @classmethod
    def _rename_default_if_required(cls,
                                    default_value: str,
                                    enum_name: str,
                                    enum_values_to_rename: 'List[Tuple[str, str]]'
                                    ) -> str:
        # remove old type postfix
        column_default_value = default_value[:default_value.find("::")]

        for old_value, new_value in enum_values_to_rename:
            column_default_value = column_default_value.replace(f"'{old_value}'", f"'{new_value}'")
            column_default_value = column_default_value.replace(f'"{old_value}"', f'"{new_value}"')

        return f"{column_default_value}::{enum_name}"

    @classmethod
    def _drop_default(cls,
                      connection: 'Connection',
                      schema: str,
                      table_reference: TableReference,
                      ):
        connection.execute(sqlalchemy.text(
            f"""ALTER TABLE {schema}.{table_reference.table_name} ALTER COLUMN {table_reference.column_name} DROP DEFAULT;"""
        ))

    @classmethod
    def _cast_old_array_enum_type_to_new(cls,
                                         connection: 'Connection',
                                         schema: str,
                                         table_reference: TableReference,
                                         enum_type_name: str,
                                         enum_values_to_rename: 'List[Tuple[str, str]]'
                                         ):
        cast_clause = f'{table_reference.column_name}::text[]'

        for old_value, new_value in enum_values_to_rename:
            cast_clause = f'''array_replace({cast_clause}, '{old_value}', '{new_value}')'''

        connection.execute(sqlalchemy.text(
            f"""ALTER TABLE {schema}.{table_reference.table_name} ALTER COLUMN {table_reference.column_name} TYPE {enum_type_name}[]
                USING {cast_clause}::{enum_type_name}[];
                """
        ))

    @classmethod
    def _cast_old_enum_type_to_new(cls,
                                   connection: 'Connection',
                                   schema: str,
                                   table_reference: TableReference,
                                   enum_type_name: str,
                                   enum_values_to_rename: 'List[Tuple[str, str]]'
                                   ):
        if table_reference.column_type == ColumnType.ARRAY:
            cls._cast_old_array_enum_type_to_new(
                connection,
                schema,
                table_reference,
                enum_type_name,
                enum_values_to_rename,
            )
            return

        if enum_values_to_rename:
            connection.execute(sqlalchemy.text(
                f"""ALTER TABLE {schema}.{table_reference.table_name} ALTER COLUMN {table_reference.column_name} TYPE {enum_type_name} 
                    USING CASE 
                    {' '.join(
                    f"WHEN {table_reference.column_name}::text = '{old_value}' THEN '{new_value}'::{enum_type_name}"
                    for old_value, new_value in enum_values_to_rename)}
                    
                    ELSE {table_reference.column_name}::text::{enum_type_name}
                    END;
                    """
            ))
        else:
            connection.execute(sqlalchemy.text(
                f"""ALTER TABLE {schema}.{table_reference.table_name} ALTER COLUMN {table_reference.column_name} TYPE {enum_type_name} 
                    USING {table_reference.column_name}::text::{enum_type_name};
                    """
            ))

    @classmethod
    def _set_default(cls,
                     connection: 'Connection',
                     schema: str,
                     table_reference: TableReference,
                     default_value: str
                     ):
        connection.execute(sqlalchemy.text(
            f"""ALTER TABLE {schema}.{table_reference.table_name} ALTER COLUMN {table_reference.column_name} SET DEFAULT {default_value};"""
        ))

    @classmethod
    def _set_enum_values(cls,
                         connection: 'Connection',
                         schema: str,
                         enum_name: str,
                         new_values: List[str],
                         affected_columns: 'List[Tuple[str, str]]',
                         enum_values_to_rename: 'List[Tuple[str, str]]'
                         ):
        enum_type_name = f"{schema}.{enum_name}"
        temporary_enum_name = f"{enum_name}_old"

        connection.execute(sqlalchemy.text(
            f"""ALTER TYPE {enum_type_name} RENAME TO {temporary_enum_name};"""
        ))
        connection.execute(sqlalchemy.text(
            f"""CREATE TYPE {enum_type_name} AS ENUM({', '.join(f"'{value}'" for value in new_values)});"""
        ))

        create_comparison_operators(connection, schema, enum_name, temporary_enum_name, enum_values_to_rename)

        for affected_column_tuple in affected_columns:
            table_reference = TableReference(*affected_column_tuple)
            column_default = cls._get_column_default(connection, schema, table_reference)

            if column_default is not None:
                cls._drop_default(connection, schema, table_reference)

            try:
                cls._cast_old_enum_type_to_new(connection,
                                               schema, table_reference, enum_type_name, enum_values_to_rename)
            except DataError as error:
                raise ValueError(
                    f'''New enum values can not be set due to some row containing reference to old enum value.
                        Please consider using enum_values_to_rename parameter or "
                    f"updating/deleting these row before calling sync_enum_values.'''
                ) from error

            if column_default is not None:
                column_default = cls._rename_default_if_required(column_default, enum_name,
                                                                 enum_values_to_rename)

                cls._set_default(connection, schema, table_reference, column_default)

        drop_comparison_operators(connection, schema, enum_name, temporary_enum_name)
        connection.execute(sqlalchemy.text(
            f"""DROP TYPE {temporary_enum_name};"""
        ))

    @classmethod
    def sync_enum_values(cls,
                         operations,
                         schema: str,
                         enum_name: str,
                         new_values: List[str],
                         affected_columns: 'List[Tuple[str, str]]',
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
            If there was server default with old_name it will be renamed accordingly
        """
        enum_values_to_rename = list(enum_values_to_rename)

        with get_connection(operations) as connection:
            cls._set_enum_values(connection, schema, enum_name, new_values, affected_columns, enum_values_to_rename)

    def to_diff_tuple(self) -> 'Tuple[Any, ...]':
        return self.operation_name, self.old_values, self.new_values, self.affected_columns

    @property
    def is_column_type_import_needed(self) -> bool:
        for affected_column_tuple in self.affected_columns:
            if len(affected_column_tuple) == 3:
                return True
        return False


@alembic.autogenerate.render.renderers.dispatch_for(SyncEnumValuesOp)
def render_sync_enum_value_op(autogen_context: AutogenContext, op: SyncEnumValuesOp):
    if op.is_column_type_import_needed:
        autogen_context.imports.add('from alembic_postgresql_enum import ColumnType')

    return (f"op.sync_enum_values({op.schema!r}, {op.name!r}, {op.new_values!r},\n"
            f"                    {op.affected_columns!r},\n"
            f"                    enum_values_to_rename=[])")


log = logging.getLogger(f'alembic.{__name__}')


def sync_changed_enums(defined_enums: EnumNamesToValues,
                       declared_enums: EnumNamesToValues,
                       table_references: EnumNamesToTableReferences,
                       schema: str,
                       upgrade_ops: UpgradeOps
                       ):
    for enum_name, new_values in declared_enums.items():
        if enum_name not in defined_enums:
            # That is work for create_new_enums function
            continue

        old_values = defined_enums[enum_name]

        if new_values == old_values:
            # Enum definition and declaration are in sync
            continue

        log.info("Detected changed enum values in %r\nWas: %r\nBecome^ %r", enum_name,
                 list(old_values), list(new_values))
        affected_columns = table_references[enum_name]
        op = SyncEnumValuesOp(schema, enum_name, list(old_values), list(new_values),
                              [column_reference.to_tuple() for column_reference in affected_columns])
        upgrade_ops.ops.append(op)
