from typing import List

from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.dialects import postgresql

DEFAULT_SCHEMA = 'public'
USER_TABLE_NAME = 'user'
USER_STATUS_COLUMN_NAME = 'status'
USER_STATUS_ENUM_NAME = 'user_status'


def get_schema_with_enum_variants(variants: List[str]) -> MetaData:
    schema = MetaData()

    Table(
        USER_TABLE_NAME,
        schema,
        Column("id", Integer, primary_key=True),
        Column(USER_STATUS_COLUMN_NAME, postgresql.ENUM(*variants, name=USER_STATUS_ENUM_NAME))
    )

    return schema


def get_schema_without_enum() -> MetaData:
    schema = MetaData()

    Table(
        USER_TABLE_NAME,
        schema,
        Column("id", Integer, primary_key=True),
    )

    return schema
