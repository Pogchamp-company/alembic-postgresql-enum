from typing import TYPE_CHECKING, List

import sqlalchemy

from tests.base.render_and_run import compare_and_run
from tests.schemas import (
    get_car_schema_without_enum,
    CAR_TABLE_NAME,
    CAR_COLORS_COLUMN_NAME,
    CAR_COLORS_ENUM_NAME,
)

if TYPE_CHECKING:
    from sqlalchemy import Connection
from sqlalchemy import Table, Column, Integer, MetaData


def get_schema_with_enum_in_sqlalchemy_array_variants(variants: List[str]) -> MetaData:
    schema = MetaData()

    Table(
        CAR_TABLE_NAME,
        schema,
        Column("id", Integer, primary_key=True),
        Column(
            CAR_COLORS_COLUMN_NAME,
            sqlalchemy.ARRAY(sqlalchemy.Enum(*variants, name=CAR_COLORS_ENUM_NAME)),
        ),
    )

    return schema


def test_create_enum_on_create_table_with_array(connection: "Connection"):
    """Check that library correctly creates enum before its use inside create_table. Enum is used in ARRAY"""
    database_schema = get_car_schema_without_enum()
    database_schema.create_all(connection)

    new_enum_variants = ["black", "white", "red", "green", "blue", "other"]

    target_schema = get_schema_with_enum_in_sqlalchemy_array_variants(new_enum_variants)

    compare_and_run(
        connection,
        target_schema,
        expected_upgrade=f"""
        # ### commands auto generated by Alembic - please adjust! ###
        sa.Enum({', '.join(map(repr, new_enum_variants))}, name='{CAR_COLORS_ENUM_NAME}').create(op.get_bind())
        op.add_column('{CAR_TABLE_NAME}', sa.Column('{CAR_COLORS_COLUMN_NAME}', sa.ARRAY(postgresql.ENUM({', '.join(map(repr, new_enum_variants))}, name='{CAR_COLORS_ENUM_NAME}', create_type=False)), nullable=True))
        # ### end Alembic commands ###
    """,
        expected_downgrade=f"""
        # ### commands auto generated by Alembic - please adjust! ###
        op.drop_column('{CAR_TABLE_NAME}', '{CAR_COLORS_COLUMN_NAME}')
        sa.Enum({', '.join(map(repr, new_enum_variants))}, name='{CAR_COLORS_ENUM_NAME}').drop(op.get_bind())
        # ### end Alembic commands ###
    """,
    )
