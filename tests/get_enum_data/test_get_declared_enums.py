from sqlalchemy.dialects import postgresql

from alembic_postgresql_enum import get_declared_enums
from alembic_postgresql_enum.get_enum_data import TableReference
from tests.schemas import get_schema_with_enum_variants, DEFAULT_SCHEMA, USER_STATUS_ENUM_NAME, USER_TABLE_NAME, \
    USER_STATUS_COLUMN_NAME, get_schema_by_declared_enum_values, get_declared_enum_values_with_orders_and_users


def test_with_user_schema():
    enum_variants = ["active", "passive"]
    declared_schema = get_schema_with_enum_variants(enum_variants)

    function_result = get_declared_enums(declared_schema, DEFAULT_SCHEMA, DEFAULT_SCHEMA, postgresql.dialect)

    assert function_result.enum_values == {
        USER_STATUS_ENUM_NAME: tuple(enum_variants)
    }
    assert function_result.enum_table_references == {
        USER_STATUS_ENUM_NAME: (TableReference(USER_TABLE_NAME, USER_STATUS_COLUMN_NAME),)
    }


def test_with_multiple_enums():
    declared_enum_values = get_declared_enum_values_with_orders_and_users()
    declared_schema = get_schema_by_declared_enum_values(declared_enum_values)

    function_result = get_declared_enums(declared_schema, DEFAULT_SCHEMA, DEFAULT_SCHEMA, postgresql.dialect)

    assert function_result == declared_enum_values
