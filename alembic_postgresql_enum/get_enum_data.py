# Based on https://github.com/dw/alembic-autogenerate-enums
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

import sqlalchemy


@dataclass
class EnumToTable:
    table_name: str
    column_name: str
    enum_name: str


@dataclass
class DeclaredEnumValues:
    # enum name -> tuple of values
    enum_definitions: Dict[str, Tuple[str]]
    table_definitions: Optional[List[EnumToTable]] = None


def get_defined_enums(conn, schema: str):
    """
    Return a dict mapping PostgreSQL enumeration types to the set of their
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
                  WHERE enumtypid = t.oid)
        FROM pg_catalog.pg_type t
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE
            t.typtype = 'e'
            AND n.nspname = :schema
    """
    return DeclaredEnumValues({
        r[0]: tuple(r[1])
        for r in conn.execute(sqlalchemy.text(sql), dict(schema=schema))
    })


def get_enum_values(enum_type: sqlalchemy.Enum, dialect) -> 'Tuple[str, ...]':
    # For specific case when types.TypeDecorator is used
    if isinstance(enum_type, sqlalchemy.types.TypeDecorator):
        def value_processor(value):
            return enum_type.process_bind_param(
                enum_type.impl.result_processor(dialect, enum_type)(value),
                dialect
            )
    else:
        def value_processor(enum_value):
            return enum_value
    return tuple(value_processor(value) for value in enum_type.enums)


def get_declared_enums(metadata, schema: str, default_schema: str, dialect):
    """
    Return a dict mapping SQLAlchemy enumeration types to the set of their
    declared values.
    :param metadata:
        ...
    :param str schema:
        Schema name (e.g. "public").
    :param default_schema:
        Default schema name, likely will be "public"
    :param dialect:
        Current sql dialect
    :returns DeclaredEnumValues:
        enum_definitions: {
            "my_enum": tuple(["a", "b", "c"]),
        },
        table_definitions: [
            EnumToTable(table_name="my_table", column_name="my_column", enum_name="my_enum"),
        ]
    """
    types = set()
    table_definitions = []

    for table in metadata.tables.values():
        for column in table.columns:
            if not hasattr(column.type, 'schema'):
                continue

            if schema != (column.type.schema or default_schema):
                continue

            if isinstance(column.type, sqlalchemy.Enum):
                types.add(column.type)

            # For specific case when types.TypeDecorator is used
            elif isinstance(getattr(column.type, 'impl', None), sqlalchemy.Enum):
                types.add(column.type)

            else:
                continue

            table_definitions.append(
                EnumToTable(table.name, column.name, column.type.name)
            )

    return DeclaredEnumValues(
        enum_definitions={
            t.name: get_enum_values(t, dialect) for t in types
        },
        table_definitions=table_definitions,
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
