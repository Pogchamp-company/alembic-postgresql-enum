from typing import TYPE_CHECKING, Optional

from alembic import autogenerate
from alembic.autogenerate import api
from alembic.operations import ops

from alembic_postgresql_enum import ColumnType
from alembic_postgresql_enum.get_enum_data import TableReference
from alembic_postgresql_enum.operations import SyncEnumValuesOp
from tests.base.run_migration_test_abc import CompareAndRunTestCase

if TYPE_CHECKING:
    from sqlalchemy import Connection
from sqlalchemy import MetaData

from tests.schemas import (
    get_schema_with_enum_in_array_variants,
    DEFAULT_SCHEMA,
    CAR_TABLE_NAME,
    CAR_COLORS_COLUMN_NAME,
    CAR_COLORS_ENUM_NAME,
)
from tests.utils.migration_context import create_migration_context


class TestAddNewEnumValueRenderWithArray(CompareAndRunTestCase):
    """Check that enum variants are updated when new variant is added"""

    old_enum_variants = ["black", "white", "red", "green", "blue", "other"]
    new_enum_variants = old_enum_variants + ["violet"]

    def get_database_schema(self) -> MetaData:
        schema = get_schema_with_enum_in_array_variants(self.old_enum_variants)
        return schema

    def get_target_schema(self) -> MetaData:
        schema = get_schema_with_enum_in_array_variants(self.new_enum_variants)
        return schema

    def get_expected_upgrade(self) -> str:
        return f"""
        # ### commands auto generated by Alembic - please adjust! ###
        op.sync_enum_values(
            enum_schema='{DEFAULT_SCHEMA}',
            enum_name='{CAR_COLORS_ENUM_NAME}',
            new_values=[{', '.join(map(repr, self.new_enum_variants))}],
            affected_columns=[TableReference(table_schema='{DEFAULT_SCHEMA}', table_name='{CAR_TABLE_NAME}', column_name='{CAR_COLORS_COLUMN_NAME}', column_type=ColumnType.ARRAY)],
            enum_values_to_rename=[],
        )
        # ### end Alembic commands ###
        """

    def get_expected_downgrade(self) -> str:
        return f"""
        # ### commands auto generated by Alembic - please adjust! ###
        op.sync_enum_values(
            enum_schema='{DEFAULT_SCHEMA}',
            enum_name='{CAR_COLORS_ENUM_NAME}',
            new_values=[{', '.join(map(repr, self.old_enum_variants))}],
            affected_columns=[TableReference(table_schema='{DEFAULT_SCHEMA}', table_name='{CAR_TABLE_NAME}', column_name='{CAR_COLORS_COLUMN_NAME}', column_type=ColumnType.ARRAY)],
            enum_values_to_rename=[],
        )
        # ### end Alembic commands ###
        """

    def get_expected_imports(self) -> Optional[str]:
        return "from alembic_postgresql_enum import ColumnType" "\nfrom alembic_postgresql_enum import TableReference"


def test_add_new_enum_value_diff_tuple_with_array(connection: "Connection"):
    """Check that enum variants are updated when new variant is added"""
    old_enum_variants = ["black", "white", "red", "green", "blue", "other"]

    database_schema = get_schema_with_enum_in_array_variants(old_enum_variants)
    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.append("violet")

    target_schema = get_schema_with_enum_in_array_variants(new_enum_variants)

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
        [
            TableReference(
                table_schema=DEFAULT_SCHEMA,
                table_name=CAR_TABLE_NAME,
                column_name=CAR_COLORS_COLUMN_NAME,
                column_type=ColumnType.ARRAY,
            )
        ],
    )


def test_remove_enum_value_diff_tuple_with_array(connection: "Connection"):
    """Check that enum variants are updated when new variant is removed"""
    old_enum_variants = ["black", "white", "red", "green", "blue", "violet", "other"]

    database_schema = get_schema_with_enum_in_array_variants(old_enum_variants)
    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.remove("violet")

    target_schema = get_schema_with_enum_in_array_variants(new_enum_variants)

    context = create_migration_context(connection, target_schema)

    autogen_context = api.AutogenContext(context, target_schema)

    uo = ops.UpgradeOps(ops=[])
    autogenerate._produce_net_changes(autogen_context, uo)

    diffs = uo.as_diffs()
    assert len(diffs) == 1

    change_variants_diff_tuple = diffs[0]
    (
        operation_name,
        old_values,
        new_values,
        affected_columns,
    ) = change_variants_diff_tuple

    assert operation_name == SyncEnumValuesOp.operation_name
    assert old_values == old_enum_variants
    assert new_values == new_enum_variants
    assert affected_columns == [
        TableReference(
            table_schema=DEFAULT_SCHEMA,
            table_name=CAR_TABLE_NAME,
            column_name=CAR_COLORS_COLUMN_NAME,
            column_type=ColumnType.ARRAY,
        )
    ]


def test_rename_enum_value_diff_tuple_with_array(connection: "Connection"):
    """Check that enum variants are updated when a variant is renamed"""
    old_enum_variants = ["black", "white", "red", "green", "blue", "other"]

    database_schema = get_schema_with_enum_in_array_variants(old_enum_variants)
    database_schema.create_all(connection)

    new_enum_variants = old_enum_variants.copy()
    new_enum_variants.remove("green")
    new_enum_variants.append("violet")

    target_schema = get_schema_with_enum_in_array_variants(new_enum_variants)

    context = create_migration_context(connection, target_schema)

    autogen_context = api.AutogenContext(context, target_schema)

    uo = ops.UpgradeOps(ops=[])
    autogenerate._produce_net_changes(autogen_context, uo)

    diffs = uo.as_diffs()
    assert len(diffs) == 1

    change_variants_diff_tuple = diffs[0]
    (
        operation_name,
        old_values,
        new_values,
        affected_columns,
    ) = change_variants_diff_tuple

    assert operation_name == SyncEnumValuesOp.operation_name
    assert old_values == old_enum_variants
    assert new_values == new_enum_variants
    assert affected_columns == [
        TableReference(
            table_schema=DEFAULT_SCHEMA,
            table_name=CAR_TABLE_NAME,
            column_name=CAR_COLORS_COLUMN_NAME,
            column_type=ColumnType.ARRAY,
        )
    ]
