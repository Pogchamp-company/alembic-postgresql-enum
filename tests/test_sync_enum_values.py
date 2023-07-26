from alembic import op
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext

from alembic_postgresql_enum import get_defined_enums
from alembic_postgresql_enum.enum_alteration import SyncEnumValuesOp
from alembic_postgresql_enum.get_enum_data import TableReference
from tests.schemas import get_schema_with_enum_variants, DEFAULT_SCHEMA, USER_STATUS_ENUM_NAME, USER_STATUS_COLUMN_NAME, \
    USER_TABLE_NAME


def test_sync_enum_values_with_new_value(connection: 'Connection'):
    old_enum_variants = ["active", "passive"]

    database_schema = get_schema_with_enum_variants(old_enum_variants)
    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.append('banned')

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(DEFAULT_SCHEMA, USER_STATUS_ENUM_NAME, new_enum_variants,
                         ((USER_TABLE_NAME, USER_STATUS_COLUMN_NAME),))

    defined = get_defined_enums(connection, DEFAULT_SCHEMA)

    assert defined == {
        USER_STATUS_ENUM_NAME: tuple(new_enum_variants)
    }
