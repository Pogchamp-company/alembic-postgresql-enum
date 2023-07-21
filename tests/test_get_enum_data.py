from sqlalchemy.dialects import postgresql

from alembic_postgresql_enum import get_declared_enums, get_defined_enums
from alembic_postgresql_enum.get_enum_data import EnumToTable
from tests.schemas import get_schema_with_enum_variants, user_status_enum_name, user_table_name, \
    user_status_column_name, default_schema


def test_get_declared_enums(connection):
    enum_variants = ["active", "passive"]
    declared_schema = get_schema_with_enum_variants(enum_variants)

    function_result = get_declared_enums(declared_schema, default_schema, default_schema, postgresql.dialect)

    assert function_result.enum_definitions == {
        user_status_enum_name: tuple(enum_variants)
    }
    assert function_result.table_definitions == [
        EnumToTable(user_table_name, user_status_column_name, user_status_enum_name)
    ]


def test_get_defined_enums(connection):
    enum_variants = ["active", "passive"]
    defined_schema = get_schema_with_enum_variants(enum_variants)
    defined_schema.create_all(connection)

    function_result = get_defined_enums(connection, default_schema)

    assert function_result.enum_definitions == {
        user_status_enum_name: tuple(enum_variants)
    }

