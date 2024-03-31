import textwrap
from typing import TYPE_CHECKING, Union, List

import sqlalchemy
from alembic import autogenerate
from alembic.operations import Operations
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql

from alembic_postgresql_enum import ColumnType, TableReference
from tests.utils.migration_context import create_migration_context

if TYPE_CHECKING:
    from sqlalchemy import Connection


def compare_and_run(
    connection: "Connection",
    target_schema: Union[MetaData, List[MetaData]],
    *,
    expected_upgrade: str,
    expected_downgrade: str,
    disable_running: bool = False,
):
    """Compares generated migration script is equal to expected_upgrade and expected_downgrade, then runs it"""
    migration_context = create_migration_context(connection, target_schema)

    op = Operations(migration_context)

    template_args = {}
    # todo _render_migration_diffs marked as legacy, maybe find something else
    autogenerate._render_migration_diffs(migration_context, template_args)

    upgrade_code = textwrap.dedent("    " + template_args["upgrades"])
    downgrade_code = textwrap.dedent("    " + template_args["downgrades"])

    expected_upgrade = textwrap.dedent(expected_upgrade).strip("\n ")
    expected_downgrade = textwrap.dedent(expected_downgrade).strip("\n ")

    assert upgrade_code == expected_upgrade, f"Got:\n{upgrade_code!r}\nExpected:\n{expected_upgrade!r}"
    assert downgrade_code == expected_downgrade, f"Got:\n{downgrade_code!r}\nExpected:\n{expected_downgrade!r}"

    if disable_running:
        return

    exec(
        upgrade_code,
        {  # todo Use imports from template_args
            "op": op,
            "sa": sqlalchemy,
            "postgresql": postgresql,
            "ColumnType": ColumnType,
            "TableReference": TableReference,
        },
    )
    exec(
        downgrade_code,
        {
            "op": op,
            "sa": sqlalchemy,
            "postgresql": postgresql,
            "ColumnType": ColumnType,
            "TableReference": TableReference,
        },
    )
