# Based on https://github.com/dw/alembic-autogenerate-enums
from dataclasses import dataclass
import sqlalchemy
from contextlib import contextmanager
from typing import Optional, Dict, List, Tuple


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


def get_defined_enums(conn, schema):
    """
    Return a dict mapping PostgreSQL enumeration types to the set of their
    defined values.
    :param conn:
        SQLAlchemy connection instance.
    :param str schema:
        Schema name (e.g. "public").
    :returns DeclaredEnumValues:
        enum_definitions={
            "my_enum": frozenset(["a", "b", "c"]),
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


def get_declared_enums(metadata, schema, default):
    """
    Return a dict mapping SQLAlchemy enumeration types to the set of their
    declared values.
    :param metadata:
        ...
    :param str schema:
        Schema name (e.g. "public").
    :returns DeclaredEnumValues:
        enum_definitions: {
            "my_enum": frozenset(["a", "b", "c"]),
        },
        table_definitions: [
            EnumToTable(table_name="my_table", column_name="my_column", enum_name="my_enum"),
        ]
    """
    types = set()
    table_definitions = []

    for table in metadata.tables.values():
        for column in table.columns:
            if isinstance(column.type, sqlalchemy.Enum) and schema == (column.type.schema or default):
                types.add(column.type)
                table_definitions.append(
                    EnumToTable(table.name, column.name, column.type.name)
                )

    return DeclaredEnumValues(
        enum_definitions={
            t.name: tuple(t.enums) for t in types
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
