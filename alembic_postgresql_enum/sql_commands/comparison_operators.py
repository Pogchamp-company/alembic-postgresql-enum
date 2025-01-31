from typing import TYPE_CHECKING
from typing import Tuple, List

import sqlalchemy

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

OPERATORS_TO_CREATE = (("!=", "new_old_not_equals"), ("=", "new_old_equals"))


def _get_escaped_enum_type_name(enum_schema: str, enum_name: str):
    return f'"{enum_schema}"."{enum_name}"'


def _create_comparison_operator(
    connection: "Connection",
    new_enum_type_name: str,
    old_enum_type_name: str,
    enum_values_to_rename: List[Tuple[str, str]],
    operator: str,
    comparison_function_name: str,
):
    if enum_values_to_rename:
        connection.execute(
            sqlalchemy.text(
                f"""
            CREATE FUNCTION {comparison_function_name}(
                new_enum_val {new_enum_type_name}, old_enum_val {old_enum_type_name}
            )
            RETURNS boolean AS $$
                SELECT new_enum_val::text {operator} CASE
                    {' '.join(
            f"WHEN old_enum_val::text = '{old_value}' THEN '{new_value}'"
            for old_value, new_value in enum_values_to_rename)}

                    ELSE old_enum_val::text
                END;
            $$ LANGUAGE SQL IMMUTABLE
        """
            )
        )
    else:
        connection.execute(
            sqlalchemy.text(
                f"""
            CREATE FUNCTION {comparison_function_name}(
                new_enum_val {new_enum_type_name}, old_enum_val {old_enum_type_name}
            )
            RETURNS boolean AS $$
                SELECT new_enum_val::text {operator} old_enum_val::text;
            $$ LANGUAGE SQL IMMUTABLE
        """
            )
        )
    connection.execute(
        sqlalchemy.text(
            f"""
        CREATE OPERATOR {operator} (
            leftarg = {new_enum_type_name},
            rightarg = {old_enum_type_name},
            procedure = {comparison_function_name}
        )
    """
        )
    )


def create_comparison_operators(
    connection: "Connection",
    schema: str,
    enum_name: str,
    old_enum_name: str,
    enum_values_to_rename: List[Tuple[str, str]],
):
    for operator, comparison_function_name in OPERATORS_TO_CREATE:
        new_enum_type_name = _get_escaped_enum_type_name(schema, enum_name)
        old_enum_type_name = _get_escaped_enum_type_name(schema, old_enum_name)
        _create_comparison_operator(
            connection,
            new_enum_type_name,
            old_enum_type_name,
            enum_values_to_rename,
            operator,
            comparison_function_name,
        )


def _drop_comparison_operator(
    connection: "Connection",
    new_enum_type_name: str,
    old_enum_type_name: str,
    comparison_function_name: str,
):
    connection.execute(
        sqlalchemy.text(
            f"""
        DROP FUNCTION {comparison_function_name}(
            new_enum_val {new_enum_type_name}, old_enum_val {old_enum_type_name}
        ) CASCADE
    """
        )
    )


def drop_comparison_operators(
    connection: "Connection",
    schema: str,
    enum_name: str,
    old_enum_name: str,
):
    for _, comparison_function_name in OPERATORS_TO_CREATE:
        new_enum_type_name = _get_escaped_enum_type_name(schema, enum_name)
        old_enum_type_name = _get_escaped_enum_type_name(schema, old_enum_name)
        _drop_comparison_operator(connection, new_enum_type_name, old_enum_type_name, comparison_function_name)
