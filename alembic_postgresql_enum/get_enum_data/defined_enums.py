import sqlalchemy

from alembic_postgresql_enum.get_enum_data.types import EnumNamesToValues


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
