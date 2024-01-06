import enum
from copy import copy
from enum import Enum
from typing import TYPE_CHECKING

import sqlalchemy.types
from sqlalchemy import Table, Column, MetaData

from alembic_postgresql_enum.get_enum_data import get_declared_enums, TableReference
from tests.schemas import DEFAULT_SCHEMA

if TYPE_CHECKING:
    from sqlalchemy import Connection


class ValuesEnum(sqlalchemy.types.TypeDecorator):
    """Custom enum wrapper that forces columns to store enum values, not names"""

    impl = sqlalchemy.types.Enum

    cache_ok = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        objects = copy(self._object_lookup)
        objects.pop(None)
        self._reversed_object_lookup = {v.value: v for v in objects.values()}
        self._reversed_object_lookup[None] = None

    def process_bind_param(self, value, dialect):
        if isinstance(value, Enum):
            return value.value
        return value

    def _object_value_for_elem(self, value):
        return self._reversed_object_lookup[value]

    def result_processor(self, dialect, coltype):
        def process(value):
            value = self._object_value_for_elem(value)
            return value

        return process


ORDER_TABLE_NAME = "order"
ORDER_DELIVERY_STATUS_COLUMN_NAME = "delivery_status"
ORDER_DELIVERY_STATUS_ENUM_NAME = "order_delivery_status"


class OrderDeliveryStatus(enum.Enum):
    WAITING_FOR_WORKER = "waiting_for_worker"
    WAITING_FOR_WORKER_TO_ARRIVE = "waiting_for_worker_to_arrive"
    WORKER_ARRIVED = "worker_arrived"
    IN_PROGRESS = "in_progress"
    WAITING_FOR_APPROVAL = "waiting_for_approval"
    DISAPPROVED = "disapproved"
    DONE = "done"
    REFUNDED = "refunded"
    BANNED = "banned"
    CANCELED = "canceled"


def get_schema_with_custom_enum() -> MetaData:
    schema = MetaData()

    Table(
        ORDER_TABLE_NAME,
        schema,
        Column(
            ORDER_DELIVERY_STATUS_COLUMN_NAME,
            ValuesEnum(OrderDeliveryStatus, name=ORDER_DELIVERY_STATUS_ENUM_NAME),
        ),
    )

    return schema


def test_get_declared_enums_for_custom_enum(connection: "Connection"):
    declared_schema = get_schema_with_custom_enum()

    function_result = get_declared_enums(declared_schema, DEFAULT_SCHEMA, DEFAULT_SCHEMA, connection)

    assert function_result.enum_values == {
        # All declared enum variants must be taken from OrderDeliveryStatus values, see ValuesEnum
        ORDER_DELIVERY_STATUS_ENUM_NAME: tuple(enum_item.value for enum_item in OrderDeliveryStatus)
    }
    assert function_result.enum_table_references == {
        ORDER_DELIVERY_STATUS_ENUM_NAME: frozenset(
            (
                TableReference(
                    table_schema=DEFAULT_SCHEMA,
                    table_name=ORDER_TABLE_NAME,
                    column_name=ORDER_DELIVERY_STATUS_COLUMN_NAME,
                ),
            )
        )
    }
