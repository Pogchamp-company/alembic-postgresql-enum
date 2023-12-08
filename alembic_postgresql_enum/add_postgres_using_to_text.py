import logging

from alembic.autogenerate import renderers, render
from alembic.autogenerate.api import AutogenContext
from alembic.operations import ops
from alembic.operations.ops import UpgradeOps, ModifyTableOps, AlterColumnOp
from sqlalchemy import String

from alembic_postgresql_enum.get_enum_data.declared_enums import column_type_is_enum


class PostgresUsingAlterColumnOp(AlterColumnOp):
    """Class to add postgresql_using to AlterColumnOp rendering"""

    def reverse(self):
        reversed_object = super().reverse()
        reversed_object.kw.pop('postgresql_using', None)
        return reversed_object


@renderers.dispatch_for(PostgresUsingAlterColumnOp)
def _postgres_using_alter_column(
        autogen_context: AutogenContext, op: ops.AlterColumnOp
) -> str:
    alter_column_expression = render._alter_column(autogen_context, op)

    postgresql_using = op.kw.get('postgresql_using', None)
    indent = " " * 11

    # To remove closing bracket
    alter_column_expression = alter_column_expression[:-1]

    if postgresql_using:
        alter_column_expression += f",\n{indent}postgresql_using='{postgresql_using}'"
    alter_column_expression += ")"

    return alter_column_expression


log = logging.getLogger(f'alembic.{__name__}')


def add_postgres_using_to_alter_operation(op: AlterColumnOp):
    op.kw['postgresql_using'] = f'{op.column_name}::{op.modify_type.name}'
    log.info("postgresql_using added to %r.%r alteration", op.table_name, op.column_name)
    op.__class__ = PostgresUsingAlterColumnOp


def add_postgres_using_to_text(upgrade_ops: UpgradeOps):
    """Add postgresql_using to alter_column expressions that changes type from string to enum"""
    for group_op in upgrade_ops.ops:
        if isinstance(group_op, ModifyTableOps):
            for i, op in enumerate(group_op.ops):
                if isinstance(op, AlterColumnOp):
                    if isinstance(op.existing_type, String) and column_type_is_enum(op.modify_type):
                        add_postgres_using_to_alter_operation(op)
