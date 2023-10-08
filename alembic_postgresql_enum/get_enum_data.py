# Based on https://github.com/dw/alembic-autogenerate-enums
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Tuple, TYPE_CHECKING, Any, Set, FrozenSet, Union, List
from enum import Enum as PyEnum

import sqlalchemy
from sqlalchemy import MetaData, Enum
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import ARRAY


class ColumnType(PyEnum):
    COMMON = Enum
    ARRAY = ARRAY

    def __repr__(self):
        return f'{self.__class__.__name__}.{self.name}'


@dataclass(frozen=True)
class TableReference:
    table_name: str
    column_name: str
    column_type: ColumnType = ColumnType.COMMON

    def to_tuple(self) -> Union[Tuple[str, str], Tuple[str, str, ColumnType]]:
        if self.column_type == ColumnType.COMMON:
            return self.table_name, self.column_name
        return self.table_name, self.column_name, self.column_type


EnumNamesToValues = Dict[str, Tuple[str, ...]]
EnumNamesToTableReferences = Dict[str, FrozenSet[TableReference]]


@dataclass
class DeclaredEnumValues:
    enum_values: EnumNamesToValues
    enum_table_references: EnumNamesToTableReferences


def _remove_schema_prefix(enum_name: str, schema: str) -> str:
    schema_prefix = f'{schema}.'

    if enum_name.startswith(schema_prefix):
        enum_name = enum_name[len(schema_prefix):]

    return enum_name


def get_defined_enums(conn, schema: str) -> EnumNamesToValues:
    """
    Return a dict mapping PostgreSQL defined enumeration types to the set of their
    defined values.
    :param conn:
        SQLAlchemy connection instance.
    :param str schema:
        Schema name (e.g. "public").
    :returns DeclaredEnumValues:
        enum_definitions={
            "my_enum": tuple(["a", "b", "c"]),
        }
    """
    sql = """
        SELECT
            pg_catalog.format_type(t.oid, NULL),
            ARRAY(SELECT enumlabel
                  FROM pg_catalog.pg_enum
                  WHERE enumtypid = t.oid
                  ORDER BY enumsortorder)
        FROM pg_catalog.pg_type t
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE
            t.typtype = 'e'
            AND n.nspname = :schema
    """
    return {
        _remove_schema_prefix(name, schema): tuple(values)
        for name, values in conn.execute(sqlalchemy.text(sql), dict(schema=schema))
    }


def get_enum_values(enum_type: sqlalchemy.Enum) -> 'Tuple[str, ...]':
    # For specific case when types.TypeDecorator is used
    if isinstance(enum_type, sqlalchemy.types.TypeDecorator):
        dialect = postgresql.dialect

        def value_processor(value):
            return enum_type.process_bind_param(
                enum_type.impl.result_processor(dialect, enum_type)(value),
                dialect
            )
    else:
        def value_processor(enum_value):
            return enum_value
    return tuple(value_processor(value) for value in enum_type.enums)


def column_type_is_enum(column_type: Any) -> bool:
    if isinstance(column_type, sqlalchemy.Enum):
        return column_type.native_enum

    # For specific case when types.TypeDecorator is used
    if isinstance(getattr(column_type, 'impl', None), sqlalchemy.Enum):
        return True

    return False


def get_declared_enums(metadata: Union[MetaData, List[MetaData]], schema: str, default_schema: str) -> DeclaredEnumValues:
    """
    Return a dict mapping SQLAlchemy declared enumeration types to the set of their values
    with columns where enums are used.
    :param metadata:
        SqlAlchemy schema
    :param str schema:
        Schema name (e.g. "public").
    :param default_schema:
        Default schema name, likely will be "public"
    :returns DeclaredEnumValues:
        enum_values: {
            "my_enum": tuple(["a", "b", "c"]),
        },
        enum_table_references: {
            "my_enum": {
                EnumToTable(table_name="my_table", column_name="my_column")
            }
        }
    """
    enum_name_to_values = dict()
    enum_name_to_table_references: defaultdict[str, Set[TableReference]] = defaultdict(set)

    if isinstance(metadata, list):
        metadata_list = metadata
    else:
        metadata_list = [metadata]

    for metadata in metadata_list:
        for table in metadata.tables.values():
            for column in table.columns:
                column_type = column.type
                column_type_wrapper = ColumnType.COMMON

                # if column is array of enums
                if isinstance(column_type, sqlalchemy.ARRAY):
                    column_type = column_type.item_type
                    column_type_wrapper = ColumnType.ARRAY

                if not column_type_is_enum(column_type):
                    continue

                column_type_schema = column_type.schema or default_schema
                if column_type_schema != schema:
                    continue

                if column_type.name not in enum_name_to_values:
                    enum_name_to_values[column_type.name] = get_enum_values(column_type)

                enum_name_to_table_references[column_type.name].add(
                    TableReference(table.name, column.name, column_type_wrapper)
                )

    return DeclaredEnumValues(
        enum_values=enum_name_to_values,
        enum_table_references={enum_name: frozenset(table_references)
                               for enum_name, table_references
                               in enum_name_to_table_references.items()},
    )


@contextmanager
def get_connection(operations) -> sqlalchemy.engine.Connection:
    """
    SQLAlchemy 2.0 changes the operation binding location; bridge function to support
    both 1.x and 2.x.

    """
    binding = operations.get_bind()
    if isinstance(binding, sqlalchemy.engine.Connection):
        yield binding
        return
    yield binding.connect()
