import pytest
import sqlalchemy
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Table, Column, Integer, MetaData
from sqlalchemy.engine import Connection

from alembic_postgresql_enum.get_enum_data import ColumnType, get_defined_enums
from alembic_postgresql_enum.sql_commands.column_default import get_column_default
from tests.schemas import (
    get_schema_with_enum_variants,
    DEFAULT_SCHEMA,
    USER_STATUS_ENUM_NAME,
    USER_STATUS_COLUMN_NAME,
    USER_TABLE_NAME,
    get_schema_with_enum_in_array_variants,
    CAR_TABLE_NAME,
    CAR_COLORS_COLUMN_NAME,
    CAR_COLORS_ENUM_NAME,
)


def test_sync_enum_values_with_new_value(connection: "Connection"):
    old_enum_variants = ["active", "passive"]

    database_schema = get_schema_with_enum_variants(old_enum_variants)
    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.append("banned")

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(
        DEFAULT_SCHEMA,
        USER_STATUS_ENUM_NAME,
        new_enum_variants,
        ((USER_TABLE_NAME, USER_STATUS_COLUMN_NAME),),
    )

    defined = get_defined_enums(connection, DEFAULT_SCHEMA)

    assert defined == {USER_STATUS_ENUM_NAME: tuple(new_enum_variants)}


def test_sync_enum_values_with_renamed_value(connection: "Connection"):
    old_enum_variants = ["active", "passive"]

    database_schema = get_schema_with_enum_variants(old_enum_variants)
    database_schema.create_all(connection)
    connection.execute(
        sqlalchemy.text(
            f"""
        INSERT INTO {USER_TABLE_NAME} ({USER_STATUS_COLUMN_NAME}) VALUES ('active'), ('passive')
    """
        )
    )

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.remove("passive")
    new_enum_variants.append("inactive")

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(
        DEFAULT_SCHEMA,
        USER_STATUS_ENUM_NAME,
        new_enum_variants,
        ((USER_TABLE_NAME, USER_STATUS_COLUMN_NAME),),
        enum_values_to_rename=[("passive", "inactive")],
    )

    defined = get_defined_enums(connection, DEFAULT_SCHEMA)

    assert defined == {USER_STATUS_ENUM_NAME: tuple(new_enum_variants)}

    users_entries = (
        connection.execute(
            sqlalchemy.text(
                f"""
        SELECT {USER_STATUS_COLUMN_NAME} FROM {USER_TABLE_NAME}
    """
            )
        )
        .scalars()
        .all()
    )

    assert users_entries == ["active", "inactive"]


def test_sync_enum_values_with_server_default(connection: "Connection"):
    old_enum_variants = ["active", "passive"]

    database_schema = MetaData()
    Table(
        "orders",
        database_schema,
        Column("id", Integer, primary_key=True),
        Column(
            "status",
            sqlalchemy.Enum(*old_enum_variants, name="order_status"),
            server_default=old_enum_variants[0],
        ),
    )

    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.append("banned")

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(DEFAULT_SCHEMA, "order_status", new_enum_variants, (("orders", "status"),))

    defined = get_defined_enums(connection, DEFAULT_SCHEMA)

    assert defined == {"order_status": tuple(new_enum_variants)}


def test_sync_enum_values_with_server_default_renamed(connection: "Connection"):
    old_enum_variants = ["active", "passive"]

    database_schema = MetaData()
    Table(
        "orders",
        database_schema,
        Column("id", Integer, primary_key=True),
        Column(
            "status",
            sqlalchemy.Enum(*old_enum_variants, name="order_status"),
            server_default=old_enum_variants[1],
        ),
    )

    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants[1] = "inactive"

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(
        DEFAULT_SCHEMA,
        "order_status",
        new_enum_variants,
        (("orders", "status"),),
        enum_values_to_rename=[("passive", "inactive")],
    )

    defined = get_defined_enums(connection, DEFAULT_SCHEMA)
    order_status_default = get_column_default(connection, DEFAULT_SCHEMA, "orders", "status")

    assert order_status_default == "'inactive'::order_status"
    assert defined == {"order_status": tuple(new_enum_variants)}


def test_sync_enum_values_raise_custom_exception(connection: "Connection"):
    old_enum_variants = ["active", "passive", "banned"]

    database_schema = get_schema_with_enum_variants(old_enum_variants)
    database_schema.create_all(connection)
    connection.execute(
        sqlalchemy.text(
            f"""
        INSERT INTO {USER_TABLE_NAME} ({USER_STATUS_COLUMN_NAME}) VALUES ('active'), ('passive'), ('banned')
    """
        )
    )

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.remove("banned")

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    with pytest.raises(ValueError):
        ops.sync_enum_values(
            DEFAULT_SCHEMA,
            USER_STATUS_ENUM_NAME,
            new_enum_variants,
            ((USER_TABLE_NAME, USER_STATUS_COLUMN_NAME),),
            enum_values_to_rename=[],
        )


def test_sync_enum_values_with_renamed_value_with_array(connection: "Connection"):
    old_enum_variants = ["black", "white", "red", "green", "blue", "violet", "other"]

    database_schema = get_schema_with_enum_in_array_variants(old_enum_variants)
    database_schema.create_all(connection)
    connection.execute(
        sqlalchemy.text(
            f"""
        INSERT INTO {CAR_TABLE_NAME} ({CAR_COLORS_COLUMN_NAME}) VALUES ('{{"black"}}'), ('{{"white", "violet"}}')
    """
        )
    )

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.remove("violet")
    new_enum_variants.append("purple")

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(
        DEFAULT_SCHEMA,
        CAR_COLORS_ENUM_NAME,
        new_enum_variants,
        ((CAR_TABLE_NAME, CAR_COLORS_COLUMN_NAME, ColumnType.ARRAY),),
        enum_values_to_rename=[("violet", "purple")],
    )

    defined = get_defined_enums(connection, DEFAULT_SCHEMA)

    assert defined == {CAR_COLORS_ENUM_NAME: tuple(new_enum_variants)}

    users_entries = (
        connection.execute(
            sqlalchemy.text(
                f"""
        SELECT {CAR_COLORS_COLUMN_NAME} FROM {CAR_TABLE_NAME}
    """
            )
        )
        .scalars()
        .all()
    )

    assert users_entries == ["{black}", "{white,purple}"]
