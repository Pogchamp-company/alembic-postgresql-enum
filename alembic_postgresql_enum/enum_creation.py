import alembic
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps

from .enum_op_base import EnumOp
from .get_enum_data import EnumNamesToValues


class CreateEnumOp(EnumOp):
    operation_name = 'create_enum'

    def reverse(self):
        from .enum_deletion import DropEnumOp
        return DropEnumOp(
            name=self.name,
            schema=self.schema,
            enum_values=self.enum_values,
        )


@alembic.autogenerate.render.renderers.dispatch_for(CreateEnumOp)
def render_create_enum_op(autogen_context: AutogenContext, op: CreateEnumOp):
    return f"""
    sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}').create(op.get_bind())
    """.strip()


def create_new_enums(defined_enums: EnumNamesToValues, declared_enums: EnumNamesToValues,
                     schema: str, upgrade_ops: UpgradeOps):
    """
    Create enums that are not in Postgres schema
    """
    for name, new_values in declared_enums.items():
        if name not in defined_enums:
            upgrade_ops.ops.insert(0, CreateEnumOp(name=name, schema=schema, enum_values=new_values))
