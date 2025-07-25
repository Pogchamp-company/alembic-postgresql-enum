import sys

import pytest
from sqlalchemy import MetaData

from tests.base.run_migration_test_abc import CompareAndRunTestCase
from tests.schemas import (
    get_schema_with_enum_variants,
    USER_TABLE_NAME,
    USER_STATUS_ENUM_NAME,
)


@pytest.mark.skipif(sys.version_info < (3, 9), reason="sa.PrimaryKeyConstraint.name generation changed in alembic 1.15")
class TestDropEnumAfterDropTable(CompareAndRunTestCase):
    """Check that library correctly drop enum after drop_table"""

    dropped_enum_variants = ["active", "passive"]

    def get_database_schema(self) -> MetaData:
        return get_schema_with_enum_variants(self.dropped_enum_variants)

    def get_target_schema(self) -> MetaData:
        return MetaData()

    def get_expected_upgrade(self) -> str:
        return f"""
        # ### commands auto generated by Alembic - please adjust! ###
        op.drop_table('{USER_TABLE_NAME}')
        sa.Enum({', '.join(map(repr, self.dropped_enum_variants))}, name='{USER_STATUS_ENUM_NAME}').drop(op.get_bind())
        # ### end Alembic commands ###
        """

    def get_expected_downgrade(self) -> str:
        return f"""
        # ### commands auto generated by Alembic - please adjust! ###
        sa.Enum('active', 'passive', name='user_status').create(op.get_bind())
        op.create_table('users',
        sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column('status', postgresql.ENUM('active', 'passive', name='user_status', create_type=False), autoincrement=False, nullable=True),
        sa.PrimaryKeyConstraint('id', name=op.f('users_pkey'))
        )
        # ### end Alembic commands ###
        """
