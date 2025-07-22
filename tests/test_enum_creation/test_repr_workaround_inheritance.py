from alembic_postgresql_enum.add_create_type_false import ReprWorkaround, get_replacement_type
from tests.schemas import (
    USER_STATUS_ENUM_NAME,
)

class CustomEnumType(ReprWorkaround):
    """
    A custom enum type that inherits from ReprWorkaround and adds a _coerce method.
    This simulates a class that inherits from both ScalarCoercible and ReprWorkaround.
    """

    def _coerce(self, value):
        """
        A method that simulates the _coerce method required by ScalarCoercible.
        """
        if isinstance(value, str):
            # This is just a placeholder implementation
            return value
        return value


def test_repr_workaround_inheritance():
    """
    Test that verifies that a class inheriting from ReprWorkaround
    retains its custom methods after get_replacement_type is called.
    """
    # Create an instance of our custom enum type
    custom_enum = CustomEnumType("active", "passive", name=USER_STATUS_ENUM_NAME)

    # Verify that the _coerce method exists before replacement
    assert hasattr(custom_enum, "_coerce")

    # Call get_replacement_type on our custom enum
    replaced_enum = get_replacement_type(custom_enum)

    # Verify that the _coerce method still exists after replacement
    assert hasattr(replaced_enum, "_coerce")

    # Verify that the replaced enum is still an instance of CustomEnumType
    assert isinstance(replaced_enum, CustomEnumType)

    # Verify that the replaced enum is also an instance of ReprWorkaround
    assert isinstance(replaced_enum, ReprWorkaround)
