import logging
from dataclasses import dataclass
from typing import Any, List, Tuple, Optional, Sequence

import alembic.autogenerate
import alembic.operations.base
import alembic.operations.ops
from alembic.autogenerate.api import AutogenContext

from alembic_postgresql_enum.configuration import get_configuration
from alembic_postgresql_enum.connection import get_connection
from alembic_postgresql_enum.get_enum_data import TableReference
from alembic_postgresql_enum.operations.sync_enum_values import SyncEnumValuesOp
from alembic_postgresql_enum.sql_commands.enum_type import add_value_before, add_value
from alembic_postgresql_enum.sql_commands.indexes import TableIndex

log = logging.getLogger(f"alembic.{__name__}")


def check_can_add_values(old_values: Sequence[str], new_values: Sequence[str]):
    if not set(new_values).issuperset(set(old_values)):
        return False
    i = 0
    for value in new_values:
        if value in old_values:
            if old_values.index(value) != i:
                return False
            i += 1
    return True


@alembic.operations.base.Operations.register_operation("add_enum_value")
class AddEnumValuesOp(alembic.operations.ops.MigrateOperation):
    operation_name = "add_enum_value"

    def __init__(
        self,
        schema: str,
        name: str,
        old_values: List[str],
        new_values: List[str],
        affected_columns: List[TableReference],
        affected_indexes: List[TableIndex] = None,
    ):
        self.schema = schema
        self.name = name
        self.old_values = old_values
        self.new_values = new_values
        self.affected_columns = affected_columns
        self.indexes_to_recreate = affected_indexes or []

    def reverse(self):
        """
        See MigrateOperation.reverse().
        """
        return SyncEnumValuesOp(
            self.schema,
            self.name,
            old_values=self.new_values,
            new_values=self.old_values,
            affected_columns=self.affected_columns,
            affected_indexes=self.indexes_to_recreate,
        )

    @classmethod
    def add_enum_value(
        cls, operations, enum_schema: str, enum_name: str, new_value: str, before_value: Optional[str] = None
    ):
        """
        Replace enum values with `new_values`
        :param operations:
            ...
        :param str enum_schema:
            Schema name.
        :param enum_name:
            Enumeration type name.
        :param new_value:
            enumeration value that should exist after this migration
            executes.
        """

        config = get_configuration()

        if operations.migration_context.dialect.name != "postgresql" and not config.force_dialect_support:
            log.warning(
                f"This library only supports postgresql, but you are using {operations.migration_context.dialect.name}, skipping"
            )
            return

        enum_type_name = f'"{enum_schema}"."{enum_name}"'

        with get_connection(operations) as connection:
            if before_value:
                add_value_before(connection, enum_type_name, before_value, new_value)
            else:
                add_value(connection, enum_type_name, new_value)

    def to_diff_tuple(self) -> Tuple[Any, ...]:
        return (
            self.operation_name,
            self.old_values,
            self.new_values,
            self.affected_columns,
        )


@dataclass
class AddEnumValueConfig:
    module_prefix: str
    type_ignore_comment: str
    schema: str
    name: str


@dataclass
class AddValueDTO:
    new_value: str
    before_value: Optional[str] = None


def _prepare_add_value_op(add_enum_value_config: AddEnumValueConfig, add_value_dto: AddValueDTO):
    return "".join(
        [
            f"{add_enum_value_config.module_prefix}add_enum_value({add_enum_value_config.type_ignore_comment}\n",
            f"    enum_schema={add_enum_value_config.schema!r},\n",
            f"    enum_name={add_enum_value_config.name!r},\n",
            f"    new_value={add_value_dto.new_value!r},\n",
            f"    before_value={add_value_dto.before_value!r},\n" if add_value_dto.before_value else "",
            ")",
        ]
    )


def _get_add_value_before_dto(old_values: List[str], new_values: List[str]):
    i, j = 0, 0
    while i < len(old_values):
        if old_values[i] != new_values[j]:
            yield AddValueDTO(new_values[j], old_values[i])
            j += 1
        else:
            i += 1
            j += 1


def _get_add_value_dto(old_values: List[str], new_values: List[str]):
    for value in new_values[(new_values.index(old_values[-1]) + 1) if old_values else 0 :]:
        yield AddValueDTO(value)


@alembic.autogenerate.render.renderers.dispatch_for(AddEnumValuesOp)
def render_add_enum_values_op(autogen_context: AutogenContext, op: AddEnumValuesOp):
    config = get_configuration()
    alembic_module_prefix = autogen_context.opts.get("alembic_module_prefix", "op.")

    op_config = AddEnumValueConfig(
        module_prefix=alembic_module_prefix,
        type_ignore_comment=config.type_ignore_comment if config.add_type_ignore else "",
        schema=op.schema,
        name=op.name,
    )

    ops = list(_get_add_value_before_dto(op.old_values, op.new_values)) + list(
        _get_add_value_dto(op.old_values, op.new_values)
    )

    return "\n".join((_prepare_add_value_op(op_config, add_value_dto) for add_value_dto in ops))
