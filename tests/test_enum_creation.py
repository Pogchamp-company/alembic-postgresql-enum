from alembic import autogenerate
from alembic.autogenerate import api
from alembic.operations import ops

from alembic_postgresql_enum import CreateEnumOp
from tests.schemas import get_schema_without_enum, get_schema_with_enum_variants, user_status_enum_name, default_schema
from tests.utils.migration_context import create_migration_context


def test_create_enum(connection):
    """Check that library correctly creates enum before its use inside add_column"""
    database_schema = get_schema_without_enum()
    database_schema.create_all(connection)

    new_enum_variants = ["active", "passive"]

    target_schema = get_schema_with_enum_variants(new_enum_variants)

    context = create_migration_context(connection, target_schema)

    autogen_context = api.AutogenContext(context, target_schema)

    uo = ops.UpgradeOps(ops=[])
    autogenerate._produce_net_changes(autogen_context, uo)

    diffs = uo.as_diffs()
    print(diffs)
    assert len(diffs) == 2
    create_enum_tuple, add_column_tuple = diffs

    assert create_enum_tuple == (
        CreateEnumOp.create_operation_name,
        user_status_enum_name,
        default_schema,
        tuple(new_enum_variants),
        False
    )
    assert add_column_tuple[0] == 'add_column'


def test_delete_enum(connection):
    """Check that library correctly removes unused enum"""
    old_enum_variants = ["active", "passive"]
    database_schema = get_schema_with_enum_variants(old_enum_variants)
    database_schema.create_all(connection)

    target_schema = get_schema_without_enum()

    context = create_migration_context(connection, target_schema)

    autogen_context = api.AutogenContext(context, target_schema)

    uo = ops.UpgradeOps(ops=[])
    autogenerate._produce_net_changes(autogen_context, uo)

    diffs = uo.as_diffs()
    print(diffs)
    assert len(diffs) == 2
    remove_column_tuple, create_enum_tuple = diffs

    assert remove_column_tuple[0] == 'remove_column'
    assert create_enum_tuple == (
        CreateEnumOp.drop_operation_name,
        user_status_enum_name,
        default_schema,
        tuple(old_enum_variants),
        True
    )
