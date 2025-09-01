"""
Tests for handling partial indexes when modifying enum values.
"""
import pytest
import sqlalchemy
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import MetaData, Table, Column, Integer, String, Index
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DataError

from tests.schemas import USER_TABLE_NAME, DEFAULT_SCHEMA
from alembic_postgresql_enum.sql_commands.indexes import get_dependent_indexes


def test_partial_index_preserved_during_enum_modification(connection: "Connection"):
    """Test that partial indexes with enum comparisons are preserved during enum modification."""
    
    old_enum_variants = ["active", "deleted"]
    
    database_schema = MetaData()
    
    Table(
        USER_TABLE_NAME,
        database_schema,
        Column("id", Integer, primary_key=True),
        Column("username", String, nullable=False),
        Column("status", postgresql.ENUM(*old_enum_variants, name="userstatus"), nullable=False),
        Index(
            "uq_user_username",
            "username",
            unique=True,
            postgresql_where=sqlalchemy.text("status != 'deleted'::userstatus"),
        ),
    )
    
    database_schema.create_all(connection)
    
    # Insert test data
    connection.execute(
        sqlalchemy.text(
            f"""
            INSERT INTO {USER_TABLE_NAME} (username, status) 
            VALUES ('user1', 'active'), ('user2', 'active'), ('user3', 'deleted')
            """
        )
    )
    
    # Verify index exists before migration
    result = connection.execute(
        sqlalchemy.text(
            """
            SELECT indexname FROM pg_indexes 
            WHERE tablename = :table_name AND indexname = :index_name
            """,
        ),
        {"table_name": USER_TABLE_NAME, "index_name": "uq_user_username"}
    )
    assert result.fetchone() is not None, "Index should exist before migration"
    
    # Add "pending" to the enum
    new_enum_variants = ["pending", "active", "deleted"]
    
    # Get dependent indexes before modifying the enum
    indexes_to_recreate = get_dependent_indexes(connection, DEFAULT_SCHEMA, "userstatus")
    
    mc = MigrationContext.configure(connection)
    ops = Operations(mc)
    
    ops.sync_enum_values(
        DEFAULT_SCHEMA,
        "userstatus",
        new_enum_variants,
        [(USER_TABLE_NAME, "status")],
        enum_values_to_rename=[],
        indexes_to_recreate=indexes_to_recreate,
    )
    
    # Verify index still exists after migration
    result = connection.execute(
        sqlalchemy.text(
            """
            SELECT indexname FROM pg_indexes 
            WHERE tablename = :table_name AND indexname = :index_name
            """,
        ),
        {"table_name": USER_TABLE_NAME, "index_name": "uq_user_username"}
    )
    assert result.fetchone() is not None, "Index should still exist after migration"
    
    # Verify the enum was updated
    result = connection.execute(
        sqlalchemy.text(
            """
            SELECT unnest(enum_range(NULL::userstatus))::text as value
            ORDER BY value
            """
        )
    )
    enum_values = [row.value for row in result]
    assert enum_values == sorted(new_enum_variants), f"Enum values should be {sorted(new_enum_variants)}, got {enum_values}"
    
    # Verify the index still works (uniqueness constraint)
    # Use a nested transaction (savepoint) to handle the expected constraint violation
    with connection.begin_nested() as sp:
        try:
            connection.execute(
                sqlalchemy.text(
                    f"""
                    INSERT INTO {USER_TABLE_NAME} (username, status) 
                    VALUES ('user1', 'active')
                    """
                )
            )
            assert False, "Should have raised uniqueness constraint violation"
        except Exception as e:
            sp.rollback()
            assert "uq_user_username" in str(e) or "duplicate" in str(e).lower(), f"Expected uniqueness violation, got: {e}"
    
    # Verify we can insert with deleted status (index allows this)
    connection.execute(
        sqlalchemy.text(
            f"""
            INSERT INTO {USER_TABLE_NAME} (username, status) 
            VALUES ('user1', 'deleted')
            """
        )
    )
    
    # Verify we can use the new enum value
    connection.execute(
        sqlalchemy.text(
            f"""
            INSERT INTO {USER_TABLE_NAME} (username, status) 
            VALUES ('user4', 'pending')
            """
        )
    )


def test_error_when_dropping_enum_value_referenced_in_index(connection: "Connection"):
    """
    Test that PostgreSQL throws an error when trying to recreate an index
    that references a dropped enum value. This happens because the index
    definition contains a reference to an enum value that no longer exists.
    """
    
    # Create initial enum and table with partial index
    old_enum_variants = ["active", "pending", "deleted"]
    
    database_schema = MetaData()
    
    Table(
        USER_TABLE_NAME,
        database_schema,
        Column("id", Integer, primary_key=True),
        Column("username", String, nullable=False),
        Column("status", postgresql.ENUM(*old_enum_variants, name="userstatus"), nullable=False),
        Index(
            "idx_not_deleted",
            "username",
            postgresql_where=sqlalchemy.text("status != 'deleted'::userstatus"),
        ),
    )
    
    database_schema.create_all(connection)
    
    # Insert test data - only use values that will exist in the new enum
    connection.execute(
        sqlalchemy.text(
            f"""
            INSERT INTO {USER_TABLE_NAME} (username, status) 
            VALUES ('user1', 'active'), ('user2', 'pending')
            """
        )
    )
    
    # Try to drop the 'deleted' value - this will fail when recreating the index
    new_enum_variants = ["active", "pending"]  # 'deleted' is removed
    
    # Get dependent indexes - validation will happen during sync_enum_values
    indexes_to_recreate = get_dependent_indexes(connection, DEFAULT_SCHEMA, "userstatus")
    
    mc = MigrationContext.configure(connection)
    ops = Operations(mc)
    
    # This will fail when trying to recreate the index because 'deleted' no longer exists
    with pytest.raises(DataError) as exc_info:
        ops.sync_enum_values(
            DEFAULT_SCHEMA,
            "userstatus",
            new_enum_variants,
            [(USER_TABLE_NAME, "status")],
            enum_values_to_rename=[],
            indexes_to_recreate=indexes_to_recreate,
        )
    
    # PostgreSQL will complain about the invalid enum value when recreating the index
    error_message = str(exc_info.value)
    assert "deleted" in error_message.lower() or "invalid" in error_message.lower()


def test_success_when_renaming_enum_value_in_index(connection: "Connection"):
    """
    Test that renaming an enum value that's referenced in a partial index works correctly.
    The index should be updated to use the new value name.
    """
    
    # Create initial enum and table with partial index
    old_enum_variants = ["active", "pending", "deleted"]
    
    database_schema = MetaData()
    
    Table(
        USER_TABLE_NAME,
        database_schema,
        Column("id", Integer, primary_key=True),
        Column("username", String, nullable=False),
        Column("status", postgresql.ENUM(*old_enum_variants, name="userstatus"), nullable=False),
        Index(
            "idx_not_deleted",
            "username",
            postgresql_where=sqlalchemy.text("status != 'deleted'::userstatus"),
        ),
    )
    
    database_schema.create_all(connection)
    
    # Insert test data
    connection.execute(
        sqlalchemy.text(
            f"""
            INSERT INTO {USER_TABLE_NAME} (username, status) 
            VALUES ('user1', 'active'), ('user2', 'deleted')
            """
        )
    )
    
    # Rename 'deleted' to 'archived' - this should succeed
    new_enum_variants = ["active", "pending", "archived"]
    
    # Get dependent indexes before modifying the enum
    indexes_to_recreate = get_dependent_indexes(connection, DEFAULT_SCHEMA, "userstatus")
    
    mc = MigrationContext.configure(connection)
    ops = Operations(mc)
    
    ops.sync_enum_values(
        DEFAULT_SCHEMA,
        "userstatus",
        new_enum_variants,
        [(USER_TABLE_NAME, "status")],
        enum_values_to_rename=[("deleted", "archived")],
        indexes_to_recreate=indexes_to_recreate,
    )
    
    # Verify the index still exists
    result = connection.execute(
        sqlalchemy.text(
            """
            SELECT COUNT(*) as count FROM pg_indexes 
            WHERE tablename = :table_name AND indexname = 'idx_not_deleted'
            """
        ),
        {"table_name": USER_TABLE_NAME}
    )
    assert result.fetchone().count == 1, "Index should still exist after renaming"
    
    # Verify the enum was updated
    result = connection.execute(
        sqlalchemy.text(
            """
            SELECT unnest(enum_range(NULL::userstatus))::text as value
            ORDER BY value
            """
        )
    )
    enum_values = [row.value for row in result]
    assert "archived" in enum_values, "Should have 'archived' value"
    assert "deleted" not in enum_values, "Should not have 'deleted' value"
    
    # Verify the data was migrated correctly
    result = connection.execute(
        sqlalchemy.text(
            f"""
            SELECT username, status FROM {USER_TABLE_NAME}
            WHERE username = 'user2'
            """
        )
    )
    row = result.fetchone()
    assert row.status == "archived", "Status should be renamed from 'deleted' to 'archived'"


def test_fallback_detection_of_partial_indexes_with_enum_expressions(connection: "Connection"):
    """
    Test that get_dependent_indexes can find partial indexes that reference enums
    in complex expressions that might not create direct pg_depend entries.
    
    This tests the fallback behavior that uses text matching when pg_depend
    might not capture all enum references in WHERE clauses.
    """
    from alembic_postgresql_enum.sql_commands.indexes import get_dependent_indexes
    
    # Create enum type
    connection.execute(
        sqlalchemy.text(
            """
            CREATE TYPE task_priority AS ENUM ('low', 'medium', 'high', 'urgent')
            """
        )
    )
    
    # Create table with enum column
    connection.execute(
        sqlalchemy.text(
            """
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                priority task_priority NOT NULL,
                assigned_to TEXT,
                completed BOOLEAN DEFAULT FALSE
            )
            """
        )
    )
    
    # Create various partial indexes that reference the enum in different ways
    
    # 1. Simple enum comparison in WHERE clause
    connection.execute(
        sqlalchemy.text(
            """
            CREATE INDEX idx_high_priority_tasks 
            ON tasks (title) 
            WHERE priority = 'high'::task_priority
            """
        )
    )
    
    # 2. Enum in complex expression (IN clause)
    connection.execute(
        sqlalchemy.text(
            """
            CREATE INDEX idx_urgent_tasks 
            ON tasks (assigned_to) 
            WHERE priority IN ('high'::task_priority, 'urgent'::task_priority)
            """
        )
    )
    
    # 3. Enum with NOT equals
    connection.execute(
        sqlalchemy.text(
            """
            CREATE INDEX idx_active_tasks 
            ON tasks (title, assigned_to) 
            WHERE priority != 'low'::task_priority AND NOT completed
            """
        )
    )
    
    # 4. Schema-qualified enum reference
    connection.execute(
        sqlalchemy.text(
            """
            CREATE INDEX idx_medium_priority 
            ON tasks (assigned_to) 
            WHERE priority = 'medium'::public.task_priority
            """
        )
    )
    
    # Now test that get_dependent_indexes finds all these indexes
    dependent_indexes = get_dependent_indexes(connection, "public", "task_priority")
    
    # Extract index names
    index_names = {idx.name.split('.')[-1] for idx in dependent_indexes}
    
    # Verify all partial indexes were found
    expected_indexes = {
        'idx_high_priority_tasks',
        'idx_urgent_tasks', 
        'idx_active_tasks',
        'idx_medium_priority'
    }
    
    assert expected_indexes.issubset(index_names), (
        f"Expected indexes {expected_indexes} not found. "
        f"Found indexes: {index_names}"
    )
    
    # Verify the index definitions are correct
    for idx in dependent_indexes:
        if 'idx_high_priority_tasks' in idx.name:
            assert "'high'::task_priority" in idx.definition or "'high'::public.task_priority" in idx.definition
        elif 'idx_urgent_tasks' in idx.name:
            assert "IN" in idx.definition and "'urgent'" in idx.definition
        elif 'idx_active_tasks' in idx.name:
            assert "'low'" in idx.definition and ("<>" in idx.definition or "!=" in idx.definition)
        elif 'idx_medium_priority' in idx.name:
            assert "'medium'" in idx.definition
    
    # Test with non-existent enum
    no_indexes = get_dependent_indexes(connection, "public", "nonexistent_enum")
    assert no_indexes == [], "Should return empty list for non-existent enum"
    
    # Test with enum in different schema
    connection.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS other_schema"))
    connection.execute(
        sqlalchemy.text(
            """
            CREATE TYPE other_schema.task_priority AS ENUM ('p1', 'p2', 'p3')
            """
        )
    )
    
    # Create an index using the other schema's enum (with same name)
    connection.execute(
        sqlalchemy.text(
            """
            CREATE TABLE other_schema.tasks (
                id INTEGER PRIMARY KEY,
                priority other_schema.task_priority
            )
            """
        )
    )
    
    connection.execute(
        sqlalchemy.text(
            """
            CREATE INDEX idx_other_schema_priority 
            ON other_schema.tasks (id) 
            WHERE priority = 'p1'::other_schema.task_priority
            """
        )
    )
    
    # Verify that searching for public.task_priority doesn't find other_schema's index
    public_indexes = get_dependent_indexes(connection, "public", "task_priority")
    public_index_names = {idx.name for idx in public_indexes}
    assert not any('idx_other_schema_priority' in name for name in public_index_names), (
        "Should not find indexes from other schemas"
    )
    
    # Verify that searching for other_schema.task_priority finds its index
    other_indexes = get_dependent_indexes(connection, "other_schema", "task_priority")
    other_index_names = {idx.name for idx in other_indexes}
    assert any('idx_other_schema_priority' in name for name in other_index_names), (
        "Should find index in other_schema"
    )


def test_non_partial_indexes_not_explicitly_dropped(connection: "Connection"):
    """
    Test that regular (non-partial) indexes on enum columns are NOT explicitly tracked
    and dropped by our code. Only partial indexes with WHERE clauses should be in the
    dependent_indexes list.
    
    Note: PostgreSQL will still rebuild indexes when ALTER COLUMN TYPE is used to change
    from the old enum to the new enum type. This is unavoidable PostgreSQL behavior.
    This test verifies that we're not unnecessarily dropping additional indexes beyond
    what PostgreSQL does automatically.
    """
    from alembic_postgresql_enum.sql_commands.indexes import get_dependent_indexes
    from alembic.operations import Operations
    from alembic.runtime.migration import MigrationContext
    
    # Create enum type
    connection.execute(
        sqlalchemy.text(
            """
            CREATE TYPE item_status AS ENUM ('draft', 'published', 'archived')
            """
        )
    )
    
    # Create table with enum column
    connection.execute(
        sqlalchemy.text(
            """
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                status item_status NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    )
    
    # Insert some test data
    connection.execute(
        sqlalchemy.text(
            """
            INSERT INTO items (id, title, status) VALUES 
            (1, 'Item 1', 'draft'),
            (2, 'Item 2', 'published'),
            (3, 'Item 3', 'archived')
            """
        )
    )
    
    # Create a REGULAR index on the enum column (no WHERE clause)
    connection.execute(
        sqlalchemy.text(
            """
            CREATE INDEX idx_items_status 
            ON items (status)
            """
        )
    )
    
    # Create a PARTIAL index with WHERE clause referencing enum value
    connection.execute(
        sqlalchemy.text(
            """
            CREATE INDEX idx_items_published 
            ON items (title) 
            WHERE status = 'published'::item_status
            """
        )
    )
    
    # Create another regular index that includes the enum column
    connection.execute(
        sqlalchemy.text(
            """
            CREATE INDEX idx_items_status_created 
            ON items (status, created_at)
            """
        )
    )
    
    # Get the OIDs of the regular indexes before migration
    regular_index_oid = connection.execute(
        sqlalchemy.text(
            "SELECT oid FROM pg_class WHERE relname = 'idx_items_status'"
        )
    ).scalar()
    
    composite_index_oid = connection.execute(
        sqlalchemy.text(
            "SELECT oid FROM pg_class WHERE relname = 'idx_items_status_created'"
        )
    ).scalar()
    
    partial_index_oid = connection.execute(
        sqlalchemy.text(
            "SELECT oid FROM pg_class WHERE relname = 'idx_items_published'"
        )
    ).scalar()
    
    # Verify get_dependent_indexes only returns partial indexes
    dependent_indexes = get_dependent_indexes(connection, "public", "item_status")
    index_names = {idx.name.split('.')[-1] for idx in dependent_indexes}
    
    # Should only find the partial index, not the regular ones
    assert 'idx_items_published' in index_names, "Should find partial index"
    assert 'idx_items_status' not in index_names, "Should NOT find regular index on enum column"
    assert 'idx_items_status_created' not in index_names, "Should NOT find composite regular index"
    
    # Verify we got exactly one index (the partial one)
    assert len(dependent_indexes) == 1, f"Should find exactly 1 partial index, found {len(dependent_indexes)}"
    
    # Now modify the enum values (add a new value)
    mc = MigrationContext.configure(connection)
    ops = Operations(mc)
    
    new_enum_values = ['draft', 'published', 'archived', 'deleted']
    
    # Pass the dependent_indexes we already fetched above
    ops.sync_enum_values(
        enum_schema="public",
        enum_name="item_status", 
        new_values=new_enum_values,
        affected_columns=[('items', 'status')],
        enum_values_to_rename=[],
        indexes_to_recreate=dependent_indexes,
    )
    
    # Get the OIDs of the indexes after migration
    regular_index_oid_after = connection.execute(
        sqlalchemy.text(
            "SELECT oid FROM pg_class WHERE relname = 'idx_items_status'"
        )
    ).scalar()
    
    composite_index_oid_after = connection.execute(
        sqlalchemy.text(
            "SELECT oid FROM pg_class WHERE relname = 'idx_items_status_created'"
        )
    ).scalar()
    
    partial_index_oid_after = connection.execute(
        sqlalchemy.text(
            "SELECT oid FROM pg_class WHERE relname = 'idx_items_published'"
        )
    ).scalar()
    
    # Note: ALL indexes on the enum column will be rebuilt by PostgreSQL during
    # ALTER COLUMN TYPE. This is unavoidable. The important thing is that we're
    # only explicitly tracking and managing partial indexes.
    
    # All indexes will have different OIDs because PostgreSQL rebuilds them
    # during ALTER COLUMN TYPE
    assert regular_index_oid != regular_index_oid_after, (
        "Regular index will be rebuilt by ALTER COLUMN TYPE (different OID expected)"
    )
    assert composite_index_oid != composite_index_oid_after, (
        "Composite index will be rebuilt by ALTER COLUMN TYPE (different OID expected)"
    )
    assert partial_index_oid != partial_index_oid_after, (
        "Partial index will be rebuilt (different OID expected)"
    )
    
    # Verify all indexes still exist and are valid
    # Check that all three indexes still exist in pg_indexes
    result = connection.execute(
        sqlalchemy.text(
            """
            SELECT COUNT(*) FROM pg_indexes 
            WHERE tablename = 'items' 
            AND indexname IN ('idx_items_status', 'idx_items_status_created', 'idx_items_published')
            """
        )
    ).scalar()
    assert result == 3, "All three indexes should still exist"
    
    # Verify the enum was updated correctly
    result = connection.execute(
        sqlalchemy.text(
            "SELECT unnest(enum_range(NULL::item_status))::text as value ORDER BY value"
        )
    )
    enum_values = [row.value for row in result]
    assert enum_values == sorted(new_enum_values), "Enum should have new values"