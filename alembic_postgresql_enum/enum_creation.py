from typing import List

import alembic

from .get_enum_data import get_defined_enums, get_declared_enums


class CreateEnumOp(alembic.operations.ops.MigrateOperation):
    def __init__(self,
                 schema: str,
                 name: str,
                 enum_values: List[str],
                 should_reverse: bool = False
                 ):
        self.schema = schema
        self.name = name
        self.enum_values = enum_values
        self.should_reverse = should_reverse

    def reverse(self):
        return CreateEnumOp(
            name=self.name,
            schema=self.schema,
            enum_values=self.enum_values,
            should_reverse=not self.should_reverse
        )


@alembic.autogenerate.render.renderers.dispatch_for(CreateEnumOp)
def render_sync_enum_value_op(autogen_context, op: CreateEnumOp):
    if op.should_reverse:
        return f"""
        sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}').drop(op.get_bind())
        """.strip()

    return f"""
    sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}').create(op.get_bind())
    """.strip()


@alembic.autogenerate.comparators.dispatch_for("schema")
def compare_enums(autogen_context, upgrade_ops, schema_names):
    """
    Create enums that are not in Postgres schema
    """
    for schema in schema_names:
        default_schema = autogen_context.dialect.default_schema_name
        if schema is None:
            schema = default_schema

        defined = get_defined_enums(autogen_context.connection, schema)
        declared = get_declared_enums(autogen_context.metadata, schema, default_schema, autogen_context.dialect)

        for name, new_values in declared.enum_definitions.items():
            if name not in defined.enum_definitions:
                upgrade_ops.ops.insert(0, CreateEnumOp(name=name, schema=schema, enum_values=new_values))

        for name, new_values in defined.enum_definitions.items():
            if name not in declared.enum_definitions:
                upgrade_ops.ops.append(CreateEnumOp(name=name, schema=schema, enum_values=new_values,
                                                    should_reverse=True))
