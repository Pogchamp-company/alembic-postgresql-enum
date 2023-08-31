from alembic.operations.ops import UpgradeOps, ModifyTableOps, AddColumnOp, CreateTableOp
from sqlalchemy import Column
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import ENUM


class ReprWorkaround(postgresql.ENUM):
    __module__ = 'sqlalchemy.dialects.postgresql'

    def __repr__(self):
        return (
                super().__repr__()[:-1] + ', create_type=False)'
        ).replace('ReprWorkaround', 'ENUM')


def inject_repr_into_enums(column: Column):
    if isinstance(column.type, ENUM):
        replacement_enum_type = column.type
        replacement_enum_type.__class__ = ReprWorkaround

        column.type = replacement_enum_type


def add_create_type_false(upgrade_ops: UpgradeOps):
    for operations_group in upgrade_ops.ops:
        if isinstance(operations_group, ModifyTableOps):
            for operation in operations_group.ops:
                print(operation)
                if isinstance(operation, AddColumnOp):
                    column: Column = operation.column

                    inject_repr_into_enums(column)

        elif isinstance(operations_group, CreateTableOp):
            for column in operations_group.columns:
                if isinstance(column, Column):
                    inject_repr_into_enums(column)
