import logging
from typing import Any, List, Tuple

import alembic.autogenerate
import alembic.operations.base
import alembic.operations.ops
from alembic.autogenerate.api import AutogenContext

from alembic_postgresql_enum.configuration import get_configuration
from alembic_postgresql_enum.get_enum_data import TableReference
from alembic_postgresql_enum.operations.sync_enum_values import SyncEnumValuesOp
from alembic_postgresql_enum.sql_commands.enum_type import add_values
from alembic_postgresql_enum.sql_commands.indexes import TableIndex

log = logging.getLogger(f"alembic.{__name__}")


@alembic.operations.base.Operations.register_operation("add_enum_values")
class AddEnumValuesOp(alembic.operations.ops.MigrateOperation):
    operation_name = "add_enum_values"

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

        if new_values[: len(old_values)] != old_values:
            raise ValueError("new_values can only append values")

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
    def add_enum_values(
        cls,
        operations,
        enum_schema: str,
        enum_name: str,
        new_values: List[str],
    ):
        """
        Replace enum values with `new_values`
        :param operations:
            ...
        :param str enum_schema:
            Schema name.
        :param enum_name:
            Enumeration type name.
        :param list new_values:
            List of enumeration values that should exist after this migration
            executes.
        """

        config = get_configuration()

        if operations.migration_context.dialect.name != "postgresql" and not config.force_dialect_support:
            log.warning(
                f"This library only supports postgresql, but you are using {operations.migration_context.dialect.name}, skipping"
            )
            return

        enum_type_name = f'"{enum_schema}"."{enum_name}"'
        add_values(operations, enum_type_name, new_values)

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

    return "\n".join(
        [
            f"{alembic_module_prefix}add_enum_values({config.type_ignore_comment if config.add_type_ignore else ''}",
            f"    enum_schema={op.schema!r},",
            f"    enum_name={op.name!r},",
            f"    new_values={op.new_values[len(op.old_values) :]!r},",
            ")",
        ]
    )
