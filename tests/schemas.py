from collections import defaultdict
from typing import List

from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.dialects import postgresql

from alembic_postgresql_enum.get_enum_data import DeclaredEnumValues, TableReference

DEFAULT_SCHEMA = 'public'
USER_TABLE_NAME = 'users'
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


def get_declared_enum_values_with_orders_and_users() -> DeclaredEnumValues:
    return DeclaredEnumValues(
        enum_values={
            'user_status_enum': ('active', 'inactive', 'banned'),
            'order_status_enum': (
                'waiting_for_worker',
                'waiting_for_worker_to_arrive',
                'worker_arrived',
                'in_progress',
                'waiting_for_approval',
                'disapproved',
                'done',
                'refunded',
                'banned',
                'canceled',
            )
        },
        enum_table_references={
            'user_status_enum': (
                TableReference('users', 'user_status'),
                TableReference('users', 'last_user_status'),
                TableReference('orders', 'user_status'),
            ),
            'order_status_enum': (
                TableReference('orders', 'order_status'),
            )
        }
    )



def get_schema_by_declared_enum_values(target: DeclaredEnumValues) -> MetaData:
    schema = MetaData()

    tables_to_columns = defaultdict(set)
    for enum_name, references in target.enum_table_references.items():
        for reference in references:
            tables_to_columns[reference.table_name].add((reference.column_name, enum_name))

    for table_name, columns_with_enum_names in tables_to_columns.items():
        Table(
            table_name,
            schema,
            *(
                Column(column_name, postgresql.ENUM(*target.enum_values[enum_name], name=enum_name))
                for column_name, enum_name in columns_with_enum_names
            )
        )

    return schema
