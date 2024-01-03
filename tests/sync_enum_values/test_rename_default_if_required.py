from alembic_postgresql_enum.sql_commands.column_default import rename_default_if_required


def test_without_renames_no_schema():
    old_default_value = "'passive'::order_status_old"

    assert (rename_default_if_required(old_default_value, 'order_status', [], None)
            == "'passive'::order_status")


def test_with_renames_no_schema():
    old_default_value = "'passive'::order_status_old"

    assert (rename_default_if_required(old_default_value, 'order_status', [
        ('passive', 'inactive')
    ], None) == "'inactive'::order_status")


def test_array_with_renames_no_schema():
    old_default_value = """'{"passive"}'::order_status_old"""

    assert (rename_default_if_required(old_default_value, 'order_status', [
        ('passive', 'inactive')
    ], None) == """'{"inactive"}'::order_status""")


def test_array_default_value_no_schema():
    old_default_value = """'{}'::order_status_old[]"""

    assert (rename_default_if_required(old_default_value, 'order_status', [], None)
            == """'{}'::order_status[]""")
    
##################################################
# Tests with schema provided
##################################################

def test_without_renames_with_schema():
    old_default_value = "'passive'::test.order_status_old"

    assert (rename_default_if_required(old_default_value, 'order_status', [], "test")
            == "'passive'::order_status")


def test_with_renames_with_schema():
    old_default_value = "'passive'::test.order_status_old"

    assert (rename_default_if_required(old_default_value, 'order_status', [
        ('passive', 'inactive')
    ], "test") == "'inactive'::test.order_status")


def test_array_with_renames_with_schema():
    old_default_value = """'{"passive"}'::test.order_status_old"""

    assert (rename_default_if_required(old_default_value, 'order_status', [
        ('passive', 'inactive')
    ], "test") == """'{"inactive"}'::test.order_status""")


def test_array_default_value_with_schema():
    old_default_value = """'{}'::test.order_status_old[]"""

    assert (rename_default_if_required(old_default_value, 'order_status', [], "test")
            == """'{}'::test.order_status[]""")