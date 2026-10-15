import logging
from typing import Any, List, Tuple, Optional

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


@alembic.autogenerate.render.renderers.dispatch_for(AddEnumValuesOp)
def render_add_enum_values_op(autogen_context: AutogenContext, op: AddEnumValuesOp):
    config = get_configuration()
    alembic_module_prefix = autogen_context.opts.get("alembic_module_prefix", "op.")
    ops = []

    old_values = op.old_values
    new_values = op.new_values

    i, j = 0, 0
    while i < len(old_values):
        if old_values[i] != new_values[j]:
            ops.append(
                "\n".join(
                    [
                        f"{alembic_module_prefix}add_enum_value({config.type_ignore_comment if config.add_type_ignore else ''}",
                        f"    enum_schema={op.schema!r},",
                        f"    enum_name={op.name!r},",
                        f"    new_value={new_values[j]!r},",
                        f"    before_value={old_values[i]!r},",
                        ")",
                    ]
                )
            )
            j += 1
        else:
            i += 1
            j += 1

    for value in new_values[(new_values.index(old_values[-1]) + 1) if old_values else 0 :]:
        ops.append(
            "\n".join(
                [
                    f"{alembic_module_prefix}add_enum_value({config.type_ignore_comment if config.add_type_ignore else ''}",
                    f"    enum_schema={op.schema!r},",
                    f"    enum_name={op.name!r},",
                    f"    new_value={value!r},",
                    ")",
                ]
            )
        )

    return "\n".join(ops)
