from typing import TYPE_CHECKING

import sqlalchemy
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext

from sqlalchemy.engine import Connection

from alembic_postgresql_enum import get_defined_enums
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


def test_sync_enum_values_with_renamed_value(connection: 'Connection'):
    old_enum_variants = ["active", "passive"]

    database_schema = get_schema_with_enum_variants(old_enum_variants)
    database_schema.create_all(connection)
    connection.execute(sqlalchemy.text(f'''
        INSERT INTO {USER_TABLE_NAME} ({USER_STATUS_COLUMN_NAME}) VALUES ('active'), ('passive')
    '''))

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.remove('passive')
    new_enum_variants.append('inactive')

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(DEFAULT_SCHEMA, USER_STATUS_ENUM_NAME, new_enum_variants,
                         ((USER_TABLE_NAME, USER_STATUS_COLUMN_NAME),),
                         enum_values_to_rename=[
                             ('passive', 'inactive')
                         ])

    defined = get_defined_enums(connection, DEFAULT_SCHEMA)

    assert defined == {
        USER_STATUS_ENUM_NAME: tuple(new_enum_variants)
    }

    users_entries = connection.execute(sqlalchemy.text(f'''
        SELECT {USER_STATUS_COLUMN_NAME} FROM {USER_TABLE_NAME}
    ''')).scalars().all()

    assert users_entries == ['active', 'inactive']
