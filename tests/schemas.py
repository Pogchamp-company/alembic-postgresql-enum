from typing import List

from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.dialects import postgresql

default_schema = 'public'
user_table_name = 'user'
user_status_column_name = 'status'
user_status_enum_name = 'user_status'


def get_schema_with_enum_variants(variants: List[str]) -> MetaData:
    schema = MetaData()

    Table(
        user_table_name,
        schema,
        Column("id", Integer, primary_key=True),
        Column(user_status_column_name, postgresql.ENUM(*variants, name=user_status_enum_name))
    )

    return schema


def get_schema_without_enum() -> MetaData:
    schema = MetaData()

    Table(
        user_table_name,
        schema,
        Column("id", Integer, primary_key=True),
    )

    return schema
