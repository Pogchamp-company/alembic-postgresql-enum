# Case No. 2 from https://github.com/Pogchamp-company/alembic-postgresql-enum/issues/26
import enum
from typing import TYPE_CHECKING

import sqlalchemy
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext

from alembic_postgresql_enum import TableReference
from alembic_postgresql_enum.get_enum_data import get_defined_enums
from tests.schemas import DEFAULT_SCHEMA
from tests.schemas import ANOTHER_SCHEMA_NAME

if TYPE_CHECKING:
    from sqlalchemy import Connection
from sqlalchemy import MetaData, Column, Integer
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import ENUM

my_metadata = MetaData(schema=ANOTHER_SCHEMA_NAME)

Base = declarative_base(metadata=my_metadata)


class _TestStatus(enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class TableWithExplicitEnumSchema(Base):
    __tablename__ = "test"
    id = Column(Integer, primary_key=True)

    status = Column(
        ENUM(_TestStatus, name="test_status"),
        nullable=False,
    )


def test_run_in_different_schema(connection: "Connection"):
    """https://github.com/Pogchamp-company/alembic-postgresql-enum/issues/34"""
    old_enum_variants = list(map(lambda item: item.name, _TestStatus))

    database_schema = my_metadata
    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.append("WAITING_FOR_APPROVAL")

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(
        DEFAULT_SCHEMA,
        "test_status",
        new_enum_variants,
        [
            TableReference(
                table_name=TableWithExplicitEnumSchema.__tablename__,
                column_name="status",
                table_schema=Base.metadata.schema,
            )
        ],
    )

    defined = get_defined_enums(connection, DEFAULT_SCHEMA)

    assert defined == {"test_status": tuple(new_enum_variants)}


def test_sync_enum_values_in_non_public_schema(connection: "Connection"):
    """Comparison functions and operators should be created in the enum's schema,
    not in the public schema. This matters when the migration user lacks CREATE
    privileges on the public schema."""
    connection.execute(
        sqlalchemy.text(f"CREATE TYPE \"{ANOTHER_SCHEMA_NAME}\".\"task_status\" AS ENUM ('pending', 'running', 'done')")
    )
    connection.execute(
        sqlalchemy.text(
            f'CREATE TABLE "{ANOTHER_SCHEMA_NAME}"."tasks" ('
            f"id serial PRIMARY KEY, "
            f'status "{ANOTHER_SCHEMA_NAME}"."task_status" NOT NULL)'
        )
    )

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(
        ANOTHER_SCHEMA_NAME,
        "task_status",
        ["pending", "running", "done", "failed"],
        [
            TableReference(
                table_name="tasks",
                column_name="status",
                table_schema=ANOTHER_SCHEMA_NAME,
            )
        ],
    )

    defined = get_defined_enums(connection, ANOTHER_SCHEMA_NAME)
    assert defined == {"task_status": ("pending", "running", "done", "failed")}


def test_sync_enum_values_with_rename_in_non_public_schema(connection: "Connection"):
    """Comparison functions and operators for renames should also be created
    in the enum's schema."""
    connection.execute(
        sqlalchemy.text(f"CREATE TYPE \"{ANOTHER_SCHEMA_NAME}\".\"color\" AS ENUM ('red', 'green', 'blue')")
    )
    connection.execute(
        sqlalchemy.text(
            f'CREATE TABLE "{ANOTHER_SCHEMA_NAME}"."items" ('
            f"id serial PRIMARY KEY, "
            f'color "{ANOTHER_SCHEMA_NAME}"."color" NOT NULL)'
        )
    )
    connection.execute(
        sqlalchemy.text(f"INSERT INTO \"{ANOTHER_SCHEMA_NAME}\".\"items\" (color) VALUES ('red'), ('green')")
    )

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(
        ANOTHER_SCHEMA_NAME,
        "color",
        ["red", "lime", "blue"],
        [
            TableReference(
                table_name="items",
                column_name="color",
                table_schema=ANOTHER_SCHEMA_NAME,
            )
        ],
        enum_values_to_rename=[("green", "lime")],
    )

    defined = get_defined_enums(connection, ANOTHER_SCHEMA_NAME)
    assert defined == {"color": ("red", "lime", "blue")}

    rows = (
        connection.execute(sqlalchemy.text(f'SELECT color FROM "{ANOTHER_SCHEMA_NAME}"."items" ORDER BY id'))
        .scalars()
        .all()
    )
    assert rows == ["red", "lime"]


RESTRICTED_USER = "restricted_migration_user"


def test_sync_enum_values_without_public_create_privilege(connection: "Connection"):
    """When the migration user lacks CREATE on public, comparison functions
    and operators must be created in the enum's own schema. Without
    schema-qualification this test fails with 'permission denied for schema public'."""
    # Create a restricted role that owns the 'another' schema but cannot create in public.
    # Use SET LOCAL ROLE to assume its identity within this transaction.
    connection.execute(sqlalchemy.text(f"DROP ROLE IF EXISTS {RESTRICTED_USER}"))
    connection.execute(sqlalchemy.text(f"CREATE ROLE {RESTRICTED_USER} NOLOGIN"))
    connection.execute(sqlalchemy.text(f'ALTER SCHEMA "{ANOTHER_SCHEMA_NAME}" OWNER TO {RESTRICTED_USER}'))
    connection.execute(sqlalchemy.text(f"REVOKE CREATE ON SCHEMA public FROM {RESTRICTED_USER}"))
    # Create enum and table owned by the restricted user
    connection.execute(
        sqlalchemy.text(f'CREATE TYPE "{ANOTHER_SCHEMA_NAME}"."priority" ' f"AS ENUM ('low', 'medium', 'high')")
    )
    connection.execute(sqlalchemy.text(f'ALTER TYPE "{ANOTHER_SCHEMA_NAME}"."priority" ' f"OWNER TO {RESTRICTED_USER}"))
    connection.execute(
        sqlalchemy.text(
            f'CREATE TABLE "{ANOTHER_SCHEMA_NAME}"."tickets" ('
            f"id serial PRIMARY KEY, "
            f'priority "{ANOTHER_SCHEMA_NAME}"."priority" NOT NULL)'
        )
    )
    connection.execute(sqlalchemy.text(f'ALTER TABLE "{ANOTHER_SCHEMA_NAME}"."tickets" ' f"OWNER TO {RESTRICTED_USER}"))
    connection.execute(
        sqlalchemy.text(f'ALTER SEQUENCE "{ANOTHER_SCHEMA_NAME}"."tickets_id_seq" ' f"OWNER TO {RESTRICTED_USER}")
    )
    connection.execute(
        sqlalchemy.text(f'INSERT INTO "{ANOTHER_SCHEMA_NAME}"."tickets" (priority) ' f"VALUES ('low'), ('high')")
    )

    # Switch to the restricted role — permission checks now apply
    connection.execute(sqlalchemy.text(f"SET LOCAL ROLE {RESTRICTED_USER}"))

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(
        ANOTHER_SCHEMA_NAME,
        "priority",
        ["low", "medium", "high", "critical"],
        [
            TableReference(
                table_name="tickets",
                column_name="priority",
                table_schema=ANOTHER_SCHEMA_NAME,
            )
        ],
    )

    defined = get_defined_enums(connection, ANOTHER_SCHEMA_NAME)
    assert defined == {"priority": ("low", "medium", "high", "critical")}

    rows = (
        connection.execute(sqlalchemy.text(f'SELECT priority FROM "{ANOTHER_SCHEMA_NAME}"."tickets" ORDER BY id'))
        .scalars()
        .all()
    )
    assert rows == ["low", "high"]

    # Restore original role
    connection.execute(sqlalchemy.text("RESET ROLE"))
