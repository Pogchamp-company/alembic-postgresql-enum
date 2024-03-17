import alembic
from alembic.autogenerate.api import AutogenContext

from .enum_lifecycle_base import EnumLifecycleOp


class CreateEnumOp(EnumLifecycleOp):
    operation_name = "create_enum"

    def reverse(self):
        from .drop_enum import DropEnumOp

        return DropEnumOp(
            name=self.name,
            schema=self.schema,
            enum_values=self.enum_values,
        )


@alembic.autogenerate.render.renderers.dispatch_for(CreateEnumOp)
def render_create_enum_op(autogen_context: AutogenContext, op: CreateEnumOp):
    assert autogen_context.dialect is not None
    if op.schema != autogen_context.dialect.default_schema_name:
        return f"""
            sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}', schema='{op.schema}').create(op.get_bind())
            """.strip()

    return f"""
        sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}').create(op.get_bind())
        """.strip()
