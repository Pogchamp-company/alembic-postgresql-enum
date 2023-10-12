# Case No. 2 from https://github.com/Pogchamp-company/alembic-postgresql-enum/issues/26
import enum
from typing import TYPE_CHECKING

from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext

from alembic_postgresql_enum import get_defined_enums
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
        ENUM(_TestStatus, name="test_status", schema=Base.metadata.schema),
        nullable=False,
    )


def test_run_in_different_schema(connection: 'Connection'):
    """https://github.com/Pogchamp-company/alembic-postgresql-enum/issues/34"""
    old_enum_variants = list(map(lambda item: item.name, _TestStatus))

    database_schema = my_metadata
    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.append('WAITING_FOR_APPROVAL')

    mc = MigrationContext.configure(connection)
    ops = Operations(mc)

    ops.sync_enum_values(Base.metadata.schema, 'test_status', new_enum_variants,
                         [(TableWithExplicitEnumSchema.__tablename__, 'status')])

    defined = get_defined_enums(connection, Base.metadata.schema)

    assert defined == {
        'test_status': tuple(new_enum_variants)
    }
