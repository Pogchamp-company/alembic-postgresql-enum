from alembic_postgresql_enum.enum_alteration import SyncEnumValuesOp


def test_without_renames():
    old_default_value = "'passive'::order_status_old"

    assert (SyncEnumValuesOp._rename_default_if_required(old_default_value, 'order_status', [])
            == "'passive'::order_status")


def test_with_renames():
    old_default_value = "'passive'::order_status_old"

    assert (SyncEnumValuesOp._rename_default_if_required(old_default_value, 'order_status', [
        ('passive', 'inactive')
    ]) == "'inactive'::order_status")


def test_array_with_renames():
    old_default_value = """'{"passive"}'::order_status_old"""

    assert (SyncEnumValuesOp._rename_default_if_required(old_default_value, 'order_status', [
        ('passive', 'inactive')
    ]) == """'{"inactive"}'::order_status""")
