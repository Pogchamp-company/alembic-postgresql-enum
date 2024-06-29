from enum import Enum
from typing import TYPE_CHECKING

import sqlalchemy
from sqlalchemy import MetaData, Table, Column, insert
from sqlalchemy.dialects import postgresql

from tests.base.run_migration_test_abc import CompareAndRunTestCase

if TYPE_CHECKING:
    from sqlalchemy import Connection


class OldEnum(Enum):
    A = "a"
    B = "b"


class NewEnum(Enum):
    A = "a"
    B = "b"
    C = "c"


class TestNewArrayColumnColumn(CompareAndRunTestCase):
    def get_database_schema(self) -> MetaData:
        database_schema = MetaData()
        Table("a", database_schema)  # , Column("value", postgresql.ARRAY(postgresql.ENUM(OldEnum)))
        Table(
            "b",
            database_schema,
            Column(
                "value",
                postgresql.ARRAY(postgresql.ENUM(OldEnum, name="my_enum")),
                server_default=sqlalchemy.text("ARRAY['A', 'B']::my_enum[]"),
            ),
        )
        return database_schema

    def get_target_schema(self) -> MetaData:
        target_schema = MetaData()
        Table(
            "a",
            target_schema,
            Column(
                "value",
                postgresql.ARRAY(postgresql.ENUM(NewEnum, name="my_enum")),
                server_default=sqlalchemy.text("ARRAY['A', 'B']::my_enum[]"),
            ),
        )
        Table(
            "b",
            target_schema,
            Column(
                "value",
                postgresql.ARRAY(postgresql.ENUM(NewEnum, name="my_enum")),
                server_default=sqlalchemy.text("ARRAY['A', 'B']::my_enum[]"),
            ),
        )
        return target_schema

    def get_expected_upgrade(self) -> str:
        return """
            # ### commands auto generated by Alembic - please adjust! ###
            op.add_column('a', sa.Column('value', postgresql.ARRAY(postgresql.ENUM('A', 'B', 'C', name='my_enum', create_type=False)), server_default=sa.text("ARRAY['A', 'B']::my_enum[]"), nullable=True))
            op.sync_enum_values('public', 'my_enum', ['A', 'B', 'C'],
                                [TableReference(table_schema='public', table_name='b', column_name='value', column_type=ColumnType.ARRAY, existing_server_default="ARRAY['A'::my_enum, 'B'::my_enum]"), TableReference(table_schema='public', table_name='a', column_name='value', column_type=ColumnType.ARRAY, existing_server_default="ARRAY['A', 'B']::my_enum[]")],
                                enum_values_to_rename=[])
            # ### end Alembic commands ###
        """

    def get_expected_downgrade(self) -> str:
        return """
            # ### commands auto generated by Alembic - please adjust! ###
            op.sync_enum_values('public', 'my_enum', ['A', 'B'],
                                [TableReference(table_schema='public', table_name='b', column_name='value', column_type=ColumnType.ARRAY, existing_server_default="ARRAY['A'::my_enum, 'B'::my_enum]"), TableReference(table_schema='public', table_name='a', column_name='value', column_type=ColumnType.ARRAY, existing_server_default="ARRAY['A', 'B']::my_enum[]")],
                                enum_values_to_rename=[])
            op.drop_column('a', 'value')
            # ### end Alembic commands ###
        """
