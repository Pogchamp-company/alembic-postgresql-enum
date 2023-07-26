from abc import ABC, abstractmethod
from typing import Any, Iterable
from typing import List, Tuple

import alembic
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps

from .get_enum_data import get_defined_enums, get_declared_enums, EnumNamesToValues


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
def render_create_enum_op(autogen_context: AutogenContext, op: CreateEnumOp):
    return f"""
    sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}').create(op.get_bind())
    """.strip()


@alembic.autogenerate.render.renderers.dispatch_for(DropEnumOp)
def render_drop_enum_op(autogen_context: AutogenContext, op: DropEnumOp):
    return f"""
        sa.Enum({', '.join(map(repr, op.enum_values))}, name='{op.name}').drop(op.get_bind())
        """.strip()


def create_new_enums(defined_enums: EnumNamesToValues, declared_enums: EnumNamesToValues,
                     schema: str, upgrade_ops: UpgradeOps):
    """
    Create enums that are not in Postgres schema
    """
    for name, new_values in declared_enums.items():
        if name not in defined_enums:
            upgrade_ops.ops.insert(0, CreateEnumOp(name=name, schema=schema, enum_values=new_values))


def drop_unused_enums(defined_enums: EnumNamesToValues, declared_enums: EnumNamesToValues,
                      schema: str, upgrade_ops: UpgradeOps):
    """
    Drop enums that are in Postgres schema but not declared in SqlAlchemy schema
    """
    for name, new_values in defined_enums.items():
        if name not in declared_enums:
            upgrade_ops.ops.append(DropEnumOp(name=name, schema=schema, enum_values=new_values))


@alembic.autogenerate.comparators.dispatch_for("schema")
def compare_enums(autogen_context: AutogenContext, upgrade_ops: UpgradeOps, schema_names: Iterable[str]):
    """
    Compare declared and defined enums
    """
    for schema in schema_names:
        default_schema = autogen_context.dialect.default_schema_name
        if schema is None:
            schema = default_schema

        defined = get_defined_enums(autogen_context.connection, schema)
        declarations = get_declared_enums(autogen_context.metadata, schema, default_schema, autogen_context.dialect)

        create_new_enums(defined, declarations.enum_definitions, schema, upgrade_ops)

        drop_unused_enums(defined, declarations.enum_definitions, schema, upgrade_ops)
