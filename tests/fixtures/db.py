import os
from typing import Generator

import pytest
import sqlalchemy
from sqlalchemy import create_engine

from tests.schemas import ANOTHER_SCHEMA_NAME, DEFAULT_SCHEMA

try:
    import dotenv

    dotenv.load_dotenv()
except ImportError:
    pass
database_uri = os.getenv("DATABASE_URI")


@pytest.fixture
def connection() -> Generator:
    engine = create_engine(database_uri)
    with engine.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                f"""
            DROP SCHEMA {DEFAULT_SCHEMA} CASCADE;
            CREATE SCHEMA {DEFAULT_SCHEMA};
            DROP SCHEMA IF EXISTS {ANOTHER_SCHEMA_NAME} CASCADE;
            CREATE SCHEMA {ANOTHER_SCHEMA_NAME};
        """
            )
        )
        yield conn
