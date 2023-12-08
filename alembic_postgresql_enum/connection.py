from contextlib import contextmanager

import sqlalchemy


@contextmanager
def get_connection(operations) -> sqlalchemy.engine.Connection:
    """
    SQLAlchemy 2.0 changes the operation binding location; bridge function to support
    both 1.x and 2.x.

    """
    binding = operations.get_bind()
    if isinstance(binding, sqlalchemy.engine.Connection):
        yield binding
        return
    yield binding.connect()
