import random
from typing import TYPE_CHECKING

from alembic_postgresql_enum.get_enum_data import TableReference, get_declared_enums
from tests.schemas import (
    get_schema_with_enum_variants,
    DEFAULT_SCHEMA,
    USER_STATUS_ENUM_NAME,
    USER_TABLE_NAME,
    USER_STATUS_COLUMN_NAME,
    get_schema_by_declared_enum_values,
    get_declared_enum_values_with_orders_and_users,
)

if TYPE_CHECKING:
    from sqlalchemy import Connection


def test_with_user_schema(connection: "Connection"):
    enum_variants = ["active", "passive"]
    declared_schema = get_schema_with_enum_variants(enum_variants)

    function_result = get_declared_enums(declared_schema, DEFAULT_SCHEMA, DEFAULT_SCHEMA, connection)

    assert function_result.enum_values == {USER_STATUS_ENUM_NAME: tuple(enum_variants)}
    assert function_result.enum_table_references == {
        USER_STATUS_ENUM_NAME: frozenset(
            (
                TableReference(
                    table_schema=DEFAULT_SCHEMA, table_name=USER_TABLE_NAME, column_name=USER_STATUS_COLUMN_NAME
                ),
            )
        )
    }


def test_with_multiple_enums(connection: "Connection"):
    declared_enum_values = get_declared_enum_values_with_orders_and_users()
    declared_schema = get_schema_by_declared_enum_values(declared_enum_values)

    function_result = get_declared_enums(declared_schema, DEFAULT_SCHEMA, DEFAULT_SCHEMA, connection)

    assert function_result == declared_enum_values


def test_can_filter_by_name(connection: "Connection"):
    declared_enum_values = get_declared_enum_values_with_orders_and_users()
    declared_schema = get_schema_by_declared_enum_values(declared_enum_values)

    all_declared_enums = declared_enum_values.enum_values
    enum_to_exclude = random.choice(list(all_declared_enums))
    enum_to_include = [e for e in all_declared_enums if e != enum_to_exclude]

    function_result = get_declared_enums(
        declared_schema,
        DEFAULT_SCHEMA,
        DEFAULT_SCHEMA,
        connection,
        None,
        lambda enum_name: enum_name not in [enum_to_exclude],
    )

    assert enum_to_exclude not in function_result.enum_values
    assert enum_to_exclude not in function_result.enum_table_references

    assert set(enum_to_include) == set(function_result.enum_values)
    assert set(enum_to_include) == set(function_result.enum_table_references)
