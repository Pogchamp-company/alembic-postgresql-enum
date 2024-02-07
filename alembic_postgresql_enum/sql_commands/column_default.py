from typing import TYPE_CHECKING, Union, List, Tuple

import sqlalchemy

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


def get_column_default(
    connection: "Connection", table_schema: str, table_name: str, column_name: str
) -> Union[str, None]:
    """Result example: "'active'::order_status" """
    default_value = connection.execute(
        sqlalchemy.text(
            f"""
        SELECT column_default
        FROM information_schema.columns
        WHERE 
            table_schema = '{table_schema}' AND 
            table_name = '{table_name}' AND 
            column_name = '{column_name}'
    """
        )
    ).scalar()
    return default_value


def drop_default(connection: "Connection", table_name_with_schema: str, column_name: str):
    connection.execute(
        sqlalchemy.text(
            f"""ALTER TABLE {table_name_with_schema}
             ALTER COLUMN {column_name} DROP DEFAULT"""
        )
    )


def set_default(
    connection: "Connection",
    table_name_with_schema: str,
    column_name: str,
    default_value: str,
):
    connection.execute(
        sqlalchemy.text(
            f"""ALTER TABLE {table_name_with_schema}
            ALTER COLUMN {column_name} SET DEFAULT {default_value}"""
        )
    )


def rename_default_if_required(
    schema: str,
    default_value: str,
    enum_name: str,
    enum_values_to_rename: List[Tuple[str, str]],
) -> str:
    is_array = default_value.endswith("[]")
    # remove old type postfix
    column_default_value = default_value[: default_value.find("::")]

    for old_value, new_value in enum_values_to_rename:
        column_default_value = column_default_value.replace(f"'{old_value}'", f"'{new_value}'")
        column_default_value = column_default_value.replace(f'"{old_value}"', f'"{new_value}"')

    suffix = "[]" if is_array else ""
    if schema:
        return f"{column_default_value}::{schema}.{enum_name}{suffix}"
    return f"{column_default_value}::{enum_name}{suffix}"
