from collections import defaultdict
from typing import List, DefaultDict, Any, Set, Tuple

from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.dialects import postgresql

from alembic_postgresql_enum.get_enum_data import (
    DeclaredEnumValues,
    TableReference,
    ColumnType,
)

DEFAULT_SCHEMA = "public"
USER_TABLE_NAME = "users"
USER_STATUS_COLUMN_NAME = "status"
USER_STATUS_ENUM_NAME = "user_status"

CAR_TABLE_NAME = "cars"
CAR_COLORS_COLUMN_NAME = "colors"
CAR_COLORS_ENUM_NAME = "car_color"

ANOTHER_SCHEMA_NAME = "another"


def get_schema_with_enum_variants(variants: List[str]) -> MetaData:
    schema = MetaData()

    Table(
        USER_TABLE_NAME,
        schema,
        Column("id", Integer, primary_key=True),
        Column(
            USER_STATUS_COLUMN_NAME,
            postgresql.ENUM(*variants, name=USER_STATUS_ENUM_NAME),
        ),
    )

    return schema


def get_schema_with_enum_in_array_variants(variants: List[str]) -> MetaData:
    schema = MetaData()

    Table(
        CAR_TABLE_NAME,
        schema,
        Column("id", Integer, primary_key=True),
        Column(
            CAR_COLORS_COLUMN_NAME,
            postgresql.ARRAY(postgresql.ENUM(*variants, name=CAR_COLORS_ENUM_NAME)),
        ),
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


def get_car_schema_without_enum() -> MetaData:
    schema = MetaData()

    Table(
        CAR_TABLE_NAME,
        schema,
        Column("id", Integer, primary_key=True),
    )

    return schema


def get_declared_enum_values_with_orders_and_users() -> DeclaredEnumValues:
    return DeclaredEnumValues(
        enum_values={
            "user_status_enum": ("active", "inactive", "banned"),
            "order_status_enum": (
                "waiting_for_worker",
                "waiting_for_worker_to_arrive",
                "worker_arrived",
                "in_progress",
                "waiting_for_approval",
                "disapproved",
                "done",
                "refunded",
                "banned",
                "canceled",
            ),
            "car_color_enum": ("black", "white", "red", "green", "blue", "other"),
        },
        enum_table_references={
            "user_status_enum": frozenset(
                (
                    TableReference(table_schema=DEFAULT_SCHEMA, table_name="users", column_name="user_status"),
                    TableReference(table_schema=DEFAULT_SCHEMA, table_name="users", column_name="last_user_status"),
                    TableReference(table_schema=DEFAULT_SCHEMA, table_name="orders", column_name="user_status"),
                )
            ),
            "order_status_enum": frozenset(
                (TableReference(table_schema=DEFAULT_SCHEMA, table_name="orders", column_name="order_status"),)
            ),
            "car_color_enum": frozenset(
                (
                    TableReference(
                        table_schema=DEFAULT_SCHEMA,
                        table_name="cars",
                        column_name="colors",
                        column_type=ColumnType.ARRAY,
                    ),
                )
            ),
        },
    )


def _enum_column_factory(
    target: DeclaredEnumValues,
    column_name: str,
    enum_name: str,
    column_type: ColumnType,
) -> Column:
    if column_type == ColumnType.COMMON:
        return Column(column_name, postgresql.ENUM(*target.enum_values[enum_name], name=enum_name))
    return Column(
        column_name,
        column_type.value(postgresql.ENUM(*target.enum_values[enum_name], name=enum_name)),
    )


def get_schema_by_declared_enum_values(target: DeclaredEnumValues) -> MetaData:
    schema = MetaData()

    tables_to_columns: DefaultDict[Any, Set[Tuple[str, str, ColumnType]]] = defaultdict(set)
    for enum_name, references in target.enum_table_references.items():
        for reference in references:
            tables_to_columns[reference.table_name].add((reference.column_name, enum_name, reference.column_type))

    for table_name, columns_with_enum_names in tables_to_columns.items():
        Table(
            table_name,
            schema,
            *(
                _enum_column_factory(target, column_name, enum_name, column_type)
                for column_name, enum_name, column_type in columns_with_enum_names
            )
        )

    return schema
