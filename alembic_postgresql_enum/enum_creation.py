from abc import ABC, abstractmethod
from typing import Any, Iterable
from typing import List, Tuple

import alembic

from .get_enum_data import get_defined_enums, get_declared_enums


class EnumOp(alembic.operations.ops.MigrateOperation, ABC):
    def __init__(self,
                 schema: str,
                 name: str,
                 enum_values: Iterable[str],
                 ):
        self.schema = schema
        self.name = name
        self.enum_values = enum_values

    @property
    @abstractmethod
    def operation_name(self) -> str:
        pass

    def to_diff_tuple(self) -> 'Tuple[Any, ...]':
        return self.operation_name, self.name, self.schema, self.enum_values


class CreateEnumOp(EnumOp):
    operation_name = 'create_enum'

    def reverse(self):
        return DropEnumOp(
            name=self.name,
            schema=self.schema,
            enum_values=self.enum_values,
        )


class DropEnumOp(EnumOp):
    operation_name = 'drop_enum'

    def reverse(self):
        return CreateEnumOp(
            name=self.name,
            schema=self.schema,
            enum_values=self.enum_values,
        )


@alembic.autogenerate.render.renderers.dispatch_for(CreateEnumOp)
def render_create_enum_op(autogen_context, op: CreateEnumOp):
    return f"""
    sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}').create(op.get_bind())
    """.strip()


@alembic.autogenerate.render.renderers.dispatch_for(DropEnumOp)
def render_drop_enum_op(autogen_context, op: DropEnumOp):
    return f"""
        sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}').drop(op.get_bind())
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
            if name not in defined:
                upgrade_ops.ops.insert(0, CreateEnumOp(name=name, schema=schema, enum_values=new_values))

        for name, new_values in defined.items():
            if name not in declared.enum_definitions:
                upgrade_ops.ops.append(DropEnumOp(name=name, schema=schema, enum_values=new_values))
