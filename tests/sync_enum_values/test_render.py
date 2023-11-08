from typing import TYPE_CHECKING

from alembic import autogenerate
from alembic.autogenerate import api
from alembic.operations import ops

from alembic_postgresql_enum.enum_alteration import SyncEnumValuesOp

if TYPE_CHECKING:
    from sqlalchemy import Connection

from tests.schemas import (get_schema_with_enum_variants,
                           USER_TABLE_NAME,
                           USER_STATUS_ENUM_NAME,
                           USER_STATUS_COLUMN_NAME,
                           DEFAULT_SCHEMA
                           )
from tests.utils.migration_context import create_migration_context


def test_add_new_enum_value_render(connection: 'Connection'):
    """Check that enum variants are updated when new variant is added"""
    old_enum_variants = ["active", "passive"]

    database_schema = get_schema_with_enum_variants(old_enum_variants)
    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.append('banned')

    target_schema = get_schema_with_enum_variants(new_enum_variants)

    context = create_migration_context(connection, target_schema)

    template_args = {}
    autogenerate._render_migration_diffs(context, template_args)

    assert (template_args["upgrades"] ==
            f"""# ### commands auto generated by Alembic - please adjust! ###
    op.sync_enum_values('{DEFAULT_SCHEMA}', '{USER_STATUS_ENUM_NAME}', [{', '.join(map(repr, new_enum_variants))}],
                        [('{USER_TABLE_NAME}', '{USER_STATUS_COLUMN_NAME}')],
                        enum_values_to_rename=[])
    # ### end Alembic commands ###""")
    assert (template_args["downgrades"] ==
            f"""# ### commands auto generated by Alembic - please adjust! ###
    op.sync_enum_values('{DEFAULT_SCHEMA}', '{USER_STATUS_ENUM_NAME}', [{', '.join(map(repr, old_enum_variants))}],
                        [('{USER_TABLE_NAME}', '{USER_STATUS_COLUMN_NAME}')],
                        enum_values_to_rename=[])
    # ### end Alembic commands ###""")


def test_add_new_enum_value_diff_tuple(connection: 'Connection'):
    """Check that enum variants are updated when new variant is added"""
    old_enum_variants = ["active", "passive"]

    database_schema = get_schema_with_enum_variants(old_enum_variants)
    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.append('banned')

    target_schema = get_schema_with_enum_variants(new_enum_variants)

    context = create_migration_context(connection, target_schema)

    autogen_context = api.AutogenContext(context, target_schema)

    uo = ops.UpgradeOps(ops=[])
    autogenerate._produce_net_changes(autogen_context, uo)

    diffs = uo.as_diffs()
    assert len(diffs) == 1
    sync_diff_tuple = diffs[0]

    assert sync_diff_tuple == (
        SyncEnumValuesOp.operation_name,
        old_enum_variants,
        new_enum_variants,
        [(USER_TABLE_NAME, USER_STATUS_COLUMN_NAME)]
    )


def test_remove_enum_value_diff_tuple(connection: 'Connection'):
    """Check that enum variants are updated when new variant is removed"""
    old_enum_variants = ["active", "passive", "banned"]

    database_schema = get_schema_with_enum_variants(old_enum_variants)
    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.remove('banned')

    target_schema = get_schema_with_enum_variants(new_enum_variants)

    context = create_migration_context(connection, target_schema)

    autogen_context = api.AutogenContext(context, target_schema)

    uo = ops.UpgradeOps(ops=[])
    autogenerate._produce_net_changes(autogen_context, uo)

    diffs = uo.as_diffs()
    assert len(diffs) == 1

    change_variants_diff_tuple = diffs[0]
    operation_name, old_values, new_values, affected_columns = change_variants_diff_tuple

    assert operation_name == SyncEnumValuesOp.operation_name
    assert old_values == old_enum_variants
    assert new_values == new_enum_variants
    assert affected_columns == [
        (USER_TABLE_NAME, USER_STATUS_COLUMN_NAME)
    ]


def test_rename_enum_value_diff_tuple(connection: 'Connection'):
    """Check that enum variants are updated when a variant is renamed"""
    old_enum_variants = ["active", "passive", "banned"]

    database_schema = get_schema_with_enum_variants(old_enum_variants)
    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.remove('banned')
    new_enum_variants.append('inactive')

    target_schema = get_schema_with_enum_variants(new_enum_variants)

    context = create_migration_context(connection, target_schema)

    autogen_context = api.AutogenContext(context, target_schema)

    uo = ops.UpgradeOps(ops=[])
    autogenerate._produce_net_changes(autogen_context, uo)

    diffs = uo.as_diffs()
    assert len(diffs) == 1

    change_variants_diff_tuple = diffs[0]
    operation_name, old_values, new_values, affected_columns = change_variants_diff_tuple

    assert operation_name == SyncEnumValuesOp.operation_name
    assert old_values == old_enum_variants
    assert new_values == new_enum_variants
    assert affected_columns == [
        (USER_TABLE_NAME, USER_STATUS_COLUMN_NAME)
    ]

