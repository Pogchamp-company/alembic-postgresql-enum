import os
from typing import Generator

import pytest
from sqlalchemy import create_engine

try:
    import dotenv
    dotenv.load_dotenv()
except ImportError:
    pass
database_uri = os.getenv('DATABASE_URI')


@pytest.fixture
def connection() -> Generator:
    engine = create_engine(database_uri)
    with engine.connect() as conn:
        yield conn
        conn.rollback()
