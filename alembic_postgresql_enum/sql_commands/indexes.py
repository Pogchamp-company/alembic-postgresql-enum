from typing import TYPE_CHECKING, List, Tuple, Set

import sqlalchemy

from alembic_postgresql_enum.sql_commands.enum_type import get_enum_values

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


def get_dependent_indexes(
    connection: "Connection",
    enum_schema: str,
    enum_name: str,
) -> List[Tuple[str, str]]:
    """
    Get all indexes that depend on the enum type.
    Returns a list of tuples containing (index_name, index_definition).
    """
    enum_oid_result = connection.execute(
        sqlalchemy.text(
            """
            SELECT t.oid
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname = :schema AND t.typname = :name
            """
        ),
        {"schema": enum_schema, "name": enum_name}
    ).fetchone()
    
    if not enum_oid_result:
        return []
    
    enum_oid = enum_oid_result.oid
    
    # Find all indexes that depend on this enum type
    # This includes indexes on columns of this enum type and partial indexes referencing it
    result = connection.execute(
        sqlalchemy.text(
            """
            SELECT DISTINCT
                idx_ns.nspname || '.' || idx_class.relname as index_name,
                pg_get_indexdef(idx_class.oid) as index_def
            FROM pg_depend dep
            -- Join to get the index details
            JOIN pg_class idx_class ON dep.objid = idx_class.oid
            JOIN pg_namespace idx_ns ON idx_class.relnamespace = idx_ns.oid
            -- Join to get the index info
            JOIN pg_index idx ON idx.indexrelid = idx_class.oid
            WHERE 
                -- The dependency is on our enum type
                dep.refobjid = :enum_oid
                AND dep.refclassid = 'pg_type'::regclass
                -- The dependent object is an index
                AND dep.classid = 'pg_class'::regclass
                AND idx_class.relkind = 'i'
                -- Only include partial indexes (those with WHERE clauses)
                AND idx.indpred IS NOT NULL
            
            UNION
            
            -- Also check for indexes where the predicate references the enum
            -- This catches cases where the enum is used in the WHERE clause
            -- but might not have a direct dependency (e.g., cast expressions)
            SELECT DISTINCT
                n.nspname || '.' || c.relname as index_name,
                pg_get_indexdef(i.indexrelid) as index_def
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indpred IS NOT NULL
                AND pg_get_expr(i.indpred, i.indrelid)::text LIKE '%' || :enum_name || '%'
                -- Additional check to ensure it's actually our enum
                AND EXISTS (
                    SELECT 1 FROM pg_depend d
                    WHERE d.objid = i.indexrelid
                    AND d.refobjid = :enum_oid
                    AND d.refclassid = 'pg_type'::regclass
                )
            """
        ),
        {"enum_oid": enum_oid, "enum_name": enum_name}
    )
    
    indexes = []
    for row in result:
        indexes.append((row.index_name, row.index_def))
    
    return indexes


def drop_indexes(connection: "Connection", indexes: List[Tuple[str, str]]):
    """
    Drop the specified indexes.
    """
    for index_name, _ in indexes:
        # Extract schema and index name if qualified
        if '.' in index_name:
            schema_name, idx_name = index_name.rsplit('.', 1)
            schema_name = schema_name.strip('"')
            idx_name = idx_name.strip('"')
            connection.execute(
                sqlalchemy.text(f'DROP INDEX IF EXISTS "{schema_name}"."{idx_name}"')
            )
        else:
            idx_name = index_name.strip('"')
            connection.execute(
                sqlalchemy.text(f'DROP INDEX IF EXISTS "{idx_name}"')
            )


def recreate_indexes(connection: "Connection", indexes: List[Tuple[str, str]]):
    """
    Recreate the indexes using their stored definitions.
    """
    for _, index_def in indexes:
        connection.execute(sqlalchemy.text(index_def))


def extract_enum_values_from_index(
    index_definition: str, 
    enum_name: str, 
    enum_schema: str,
    connection: "Connection"
) -> Set[str]:
    """
    Extract enum values referenced in an index definition.
    
    Instead of parsing SQL with regex, this function:
    1. Gets all valid enum values from the database
    2. Checks which ones appear in the index definition as casts
    
    This approach is more reliable as it only finds actual enum values
    and avoids complex SQL parsing.
    """
    all_enum_values = get_enum_values(connection, enum_schema, enum_name)
    
    referenced_values = set()
    for value in all_enum_values:
        unqualified_cast = f"'{value}'::{enum_name}"
        qualified_cast = f"'{value}'::{enum_schema}.{enum_name}"
        
        if unqualified_cast in index_definition or qualified_cast in index_definition:
            referenced_values.add(value)
    
    return referenced_values


def transform_index_definition_for_renamed_values(
    index_definition: str,
    enum_name: str,
    enum_values_to_rename: List[Tuple[str, str]],
    enum_schema: str
) -> str:
    """
    Transform an index definition to use renamed enum values.
    
    For each (old_value, new_value) pair, replaces occurrences of:
    - 'old_value'::enum_name with 'new_value'::enum_name (unqualified)
    - 'old_value'::schema.enum_name with 'new_value'::schema.enum_name (schema-qualified)
    """
    transformed_def = index_definition
    
    for old_value, new_value in enum_values_to_rename:
        old_pattern = f"'{old_value}'::{enum_name}"
        new_pattern = f"'{new_value}'::{enum_name}"
        transformed_def = transformed_def.replace(old_pattern, new_pattern)
        
        # If schema provided, also replace any schema-qualified references
        if enum_schema:
            old_qualified = f"'{old_value}'::{enum_schema}.{enum_name}"
            new_qualified = f"'{new_value}'::{enum_schema}.{enum_name}"
            transformed_def = transformed_def.replace(old_qualified, new_qualified)
    
    return transformed_def


def validate_indexes_compatibility(
    indexes: List[Tuple[str, str]],
    enum_name: str,
    new_values: set[str],
    enum_values_to_rename: List[Tuple[str, str]],
    enum_schema: str,
    connection: "Connection"
) -> None:
    """
    Validate that all indexes can be recreated with the new enum values.
    Raises ValueError if any index references a value that will be dropped.
    """
    old_values = get_enum_values(connection, enum_schema, enum_name)

    rename_map = dict(enum_values_to_rename)
    
    dropped_values = old_values - new_values - set(rename_map.keys())
    
    if len(dropped_values) == 0:
        return
    
    for index_name, index_def in indexes:
        referenced_values = extract_enum_values_from_index(index_def, enum_name, enum_schema, connection)
        
        in_use_dropped_values = referenced_values & dropped_values
        
        if len(in_use_dropped_values) > 0:
            clean_index_name = index_name.split('.')[-1].strip('"')
            
            raise ValueError(
                f"Cannot drop enum value(s) {', '.join(repr(v) for v in sorted(in_use_dropped_values))} "
                f"because they are referenced in partial index '{clean_index_name}'\n"
                f"Index definition: {index_def}\n\n"
                f"To resolve this issue, either:\n"
                f"1. Use enum_values_to_rename to rename {', '.join(repr(v) for v in sorted(in_use_dropped_values))} "
                f"to other values instead of dropping\n"
                f"2. Manually drop the index '{clean_index_name}' before running this migration and recreate after\n"
                f"3. Update your code to not drop these enum values"
            )