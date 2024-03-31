import alembic
from alembic.autogenerate.api import AutogenContext

from .enum_lifecycle_base import EnumLifecycleOp


class DropEnumOp(EnumLifecycleOp):
    operation_name = "drop_enum"

    def reverse(self):
        from .create_enum import CreateEnumOp

        return CreateEnumOp(
            name=self.name,
            schema=self.schema,
            enum_values=self.enum_values,
        )


@alembic.autogenerate.render.renderers.dispatch_for(DropEnumOp)
def render_drop_enum_op(autogen_context: AutogenContext, op: DropEnumOp):
    assert autogen_context.dialect is not None
    if op.schema != autogen_context.dialect.default_schema_name:
        return f"""
            sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}', schema='{op.schema}').drop(op.get_bind())
            """.strip()

    return f"""
        sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}').drop(op.get_bind())
        """.strip()
