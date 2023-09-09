import logging

import alembic
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps

from .enum_op_base import EnumOp
from .get_enum_data import EnumNamesToValues


class DropEnumOp(EnumOp):
    operation_name = 'drop_enum'

    def reverse(self):
        from .enum_creation import CreateEnumOp
        return CreateEnumOp(
            name=self.name,
            schema=self.schema,
            enum_values=self.enum_values,
        )


@alembic.autogenerate.render.renderers.dispatch_for(DropEnumOp)
def render_drop_enum_op(autogen_context: AutogenContext, op: DropEnumOp):
    if op.schema != autogen_context.dialect.default_schema_name:
        return f"""
            sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}', schema='{op.schema}').drop(op.get_bind())
            """.strip()

    return f"""
        sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}').drop(op.get_bind())
        """.strip()


log = logging.getLogger(f'alembic.{__name__}')


def drop_unused_enums(defined_enums: EnumNamesToValues, declared_enums: EnumNamesToValues,
                      schema: str, upgrade_ops: UpgradeOps):
    """
    Drop enums that are in Postgres schema but not declared in SqlAlchemy schema
    """
    for name, new_values in defined_enums.items():
        if name not in declared_enums:
            log.info("Detected unused enum %r with values %r", name, new_values)
            upgrade_ops.ops.append(DropEnumOp(name=name, schema=schema, enum_values=new_values))
