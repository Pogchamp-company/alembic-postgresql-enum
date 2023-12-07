from typing import TYPE_CHECKING, Union

import sqlalchemy

from alembic_postgresql_enum.get_enum_data import TableReference

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


def get_column_default(connection: 'Connection',
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


def drop_default(connection: 'Connection',
                 schema: str,
                 table_reference: TableReference,
                 ):
    connection.execute(sqlalchemy.text(
        f"""ALTER TABLE {schema}.{table_reference.table_name} ALTER COLUMN {table_reference.column_name} DROP DEFAULT;"""
    ))


def set_default(connection: 'Connection',
                schema: str,
                table_reference: TableReference,
                default_value: str
                ):
    connection.execute(sqlalchemy.text(
        f"""ALTER TABLE {schema}.{table_reference.table_name} ALTER COLUMN {table_reference.column_name} SET DEFAULT {default_value};"""
    ))


def rename_default_if_required(default_value: str,
                               enum_name: str,
                               enum_values_to_rename: 'List[Tuple[str, str]]'
                               ) -> str:
    # remove old type postfix
    column_default_value = default_value[:default_value.find("::")]

    for old_value, new_value in enum_values_to_rename:
        column_default_value = column_default_value.replace(f"'{old_value}'", f"'{new_value}'")
        column_default_value = column_default_value.replace(f'"{old_value}"', f'"{new_value}"')

    return f"{column_default_value}::{enum_name}"
