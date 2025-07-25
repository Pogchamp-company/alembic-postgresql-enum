from sqlalchemy import MetaData

from alembic_postgresql_enum.configuration import Config
from tests.base.run_migration_test_abc import CompareAndRunTestCase
from tests.schemas import get_schema_with_enum_variants


class TestEnumValuesReorderingIgnoreValuesOrder(CompareAndRunTestCase):
    """Check if enum is not updated when the variants are reordered and
    ignore_enum_values_order config variable is true
    """

    config = Config(ignore_enum_values_order=True)

    old_enum_variants = ["active", "passive"]
    new_enum_variants = ["passive", "active"]

    def get_database_schema(self) -> MetaData:
        schema = get_schema_with_enum_variants(self.old_enum_variants)
        return schema

    def get_target_schema(self) -> MetaData:
        schema = get_schema_with_enum_variants(self.new_enum_variants)
        return schema

    def get_expected_upgrade(self) -> str:
        return """
        # ### commands auto generated by Alembic - please adjust! ###
        pass
        # ### end Alembic commands ###
        """

    def get_expected_downgrade(self) -> str:
        return """
        # ### commands auto generated by Alembic - please adjust! ###
        pass
        # ### end Alembic commands ###
        """


class TestEnumValuesReorderingDoNotIgnoreValuesOrder(CompareAndRunTestCase):
    """Check if enum is updated when the variants are reordered and
    ignore_enum_values_order config variable is false
    """

    config = Config(ignore_enum_values_order=False)

    old_enum_variants = ["active", "passive"]
    new_enum_variants = ["passive", "active"]

    def get_database_schema(self) -> MetaData:
        schema = get_schema_with_enum_variants(self.old_enum_variants)
        return schema

    def get_target_schema(self) -> MetaData:
        schema = get_schema_with_enum_variants(self.new_enum_variants)
        return schema

    def get_expected_upgrade(self) -> str:
        return """
        # ### commands auto generated by Alembic - please adjust! ###
        op.sync_enum_values(
            enum_schema='public',
            enum_name='user_status',
            new_values=['passive', 'active'],
            affected_columns=[TableReference(table_schema='public', table_name='users', column_name='status')],
            enum_values_to_rename=[],
        )
        # ### end Alembic commands ###
        """

    def get_expected_downgrade(self) -> str:
        return """
        # ### commands auto generated by Alembic - please adjust! ###
        op.sync_enum_values(
            enum_schema='public',
            enum_name='user_status',
            new_values=['active', 'passive'],
            affected_columns=[TableReference(table_schema='public', table_name='users', column_name='status')],
            enum_values_to_rename=[],
        )
        # ### end Alembic commands ###
        """
