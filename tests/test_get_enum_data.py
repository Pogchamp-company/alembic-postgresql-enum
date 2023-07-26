from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from sqlalchemy import Connection
from sqlalchemy.dialects import postgresql

from alembic_postgresql_enum import get_declared_enums, get_defined_enums
from alembic_postgresql_enum.get_enum_data import TableReference
from tests.schemas import get_schema_with_enum_variants, USER_STATUS_ENUM_NAME, USER_TABLE_NAME, \
    USER_STATUS_COLUMN_NAME, DEFAULT_SCHEMA


def test_get_declared_enums(connection: 'Connection'):
    enum_variants = ["active", "passive"]
    declared_schema = get_schema_with_enum_variants(enum_variants)

    function_result = get_declared_enums(declared_schema, DEFAULT_SCHEMA, DEFAULT_SCHEMA, postgresql.dialect)

    assert function_result.enum_values == {
        USER_STATUS_ENUM_NAME: tuple(enum_variants)
    }
    assert function_result.enum_table_references == {
        USER_STATUS_ENUM_NAME: (TableReference(USER_TABLE_NAME, USER_STATUS_COLUMN_NAME),)
    }


def test_get_defined_enums(connection: 'Connection'):
    enum_variants = ["active", "passive"]
    defined_schema = get_schema_with_enum_variants(enum_variants)
    defined_schema.create_all(connection)

    function_result = get_defined_enums(connection, DEFAULT_SCHEMA)

    assert function_result == {
        USER_STATUS_ENUM_NAME: tuple(enum_variants)
    }

