import sqlalchemy
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import MetaData, Table, Column, Integer, Index
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection

from tests.schemas import USER_TABLE_NAME, DEFAULT_SCHEMA


def test_sync_enum_values_unique_column_with_equals(connection: "Connection"):
    old_enum_variants = ["admin", "writer"]

    database_schema = MetaData()

    Table(
        USER_TABLE_NAME,
        database_schema,
        Column("id", Integer, primary_key=True),
        Column("role", postgresql.ENUM(*old_enum_variants, name="user_role"), nullable=True),
        Index(
            "admin_unique",
            "role",
            unique=True,
            postgresql_where=sqlalchemy.text(f"role = 'admin'::user_role"),
        ),
    )

    database_schema.create_all(connection)
    connection.execute(
        sqlalchemy.text(
            f"""
        INSERT INTO {USER_TABLE_NAME} (role) VALUES ('admin'), ('writer'), (null)
    """
        )
    )

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.remove("admin")
    new_enum_variants.insert(0, "administrator")

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(
        DEFAULT_SCHEMA,
        "user_role",
        new_enum_variants,
        [(USER_TABLE_NAME, "role")],
        enum_values_to_rename=[("admin", "administrator")],
    )


def test_sync_enum_values_unique_column_with_not_equals(connection: "Connection"):
    old_enum_variants = ["admin", "writer"]

    database_schema = MetaData()

    Table(
        USER_TABLE_NAME,
        database_schema,
        Column("id", Integer, primary_key=True),
        Column("role", postgresql.ENUM(*old_enum_variants, name="user_role"), nullable=True),
        Index(
            "admin_unique",
            "role",
            unique=True,
            postgresql_where=sqlalchemy.text(f"role <> 'admin'::user_role"),
        ),
    )

    database_schema.create_all(connection)
    connection.execute(
        sqlalchemy.text(
            f"""
        INSERT INTO {USER_TABLE_NAME} (role) VALUES ('admin'), ('writer'), (null)
    """
        )
    )

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.remove("admin")
    new_enum_variants.insert(0, "administrator")

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(
        DEFAULT_SCHEMA,
        "user_role",
        new_enum_variants,
        [(USER_TABLE_NAME, "role")],
        enum_values_to_rename=[("admin", "administrator")],
    )
