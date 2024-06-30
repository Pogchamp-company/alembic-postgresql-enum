from alembic_postgresql_enum.sql_commands.column_default import (
    rename_default_if_required,
)


def test_without_renames_with_schema():
    old_default_value = "'passive'::test.order_status_old"

    assert rename_default_if_required("test", old_default_value, "order_status", []) == "'passive'::test.order_status"


def test_with_renames_with_schema():
    old_default_value = "'passive'::test.order_status_old"

    assert (
        rename_default_if_required("test", old_default_value, "order_status", [("passive", "inactive")])
        == "'inactive'::test.order_status"
    )


def test_array_with_renames_with_schema():
    old_default_value = """'{"passive"}'::test.order_status_old"""

    assert (
        rename_default_if_required("test", old_default_value, "order_status", [("passive", "inactive")])
        == """'{"inactive"}'::test.order_status"""
    )


def test_array_default_value_with_schema():
    old_default_value = """'{}'::test.order_status_old[]"""

    assert rename_default_if_required("test", old_default_value, "order_status", []) == """'{}'::test.order_status[]"""


def test_caps_array_default_value_without_schema():
    old_default_value = """ARRAY['A'::my_old_enum, 'B'::my_old_enum]"""

    assert (
        rename_default_if_required("test", old_default_value, "my_enum", [])
        == """ARRAY['A'::test.my_enum, 'B'::test.my_enum]"""
    )


def test_caps_array_default_value_with_schema():
    old_default_value = """ARRAY['A'::test.my_old_enum, 'B'::test.my_old_enum]"""

    assert (
        rename_default_if_required("test", old_default_value, "my_enum", [])
        == """ARRAY['A'::test.my_enum, 'B'::test.my_enum]"""
    )


def test_caps_array_another_default_value_without_schema():
    old_default_value = """ARRAY['A'::my_old_enum, 'B'::my_old_enum]"""

    assert (
        rename_default_if_required("test", old_default_value, "my_enum", [])
        == """ARRAY['A'::test.my_enum, 'B'::test.my_enum]"""
    )


def test_caps_array_another_default_value_with_schema():
    old_default_value = """ARRAY['A', 'B']::test.my_old_enum[]"""

    assert rename_default_if_required("test", old_default_value, "my_enum", []) == """ARRAY['A', 'B']::test.my_enum[]"""
