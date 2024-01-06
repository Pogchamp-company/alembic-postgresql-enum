from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy import Connection

from alembic_postgresql_enum.get_enum_data import get_defined_enums
from tests.schemas import (
    get_schema_with_enum_variants,
    DEFAULT_SCHEMA,
    USER_STATUS_ENUM_NAME,
    get_schema_by_declared_enum_values,
    get_declared_enum_values_with_orders_and_users,
)


def test_get_defined_enums(connection: "Connection"):
    enum_variants = ["active", "passive"]
    defined_schema = get_schema_with_enum_variants(enum_variants)
    defined_schema.create_all(connection)

    function_result = get_defined_enums(connection, DEFAULT_SCHEMA)

    assert function_result == {USER_STATUS_ENUM_NAME: tuple(enum_variants)}


def test_with_multiple_enums(connection: "Connection"):
    declared_enum_values = get_declared_enum_values_with_orders_and_users()
    defined_schema = get_schema_by_declared_enum_values(declared_enum_values)

    defined_schema.create_all(connection)
    function_result = get_defined_enums(connection, DEFAULT_SCHEMA)

    assert function_result == declared_enum_values.enum_values
