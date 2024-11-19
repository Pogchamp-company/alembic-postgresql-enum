from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

import alembic_postgresql_enum
from alembic_postgresql_enum.configuration import Config, get_configuration
from tests.base.render_and_run import compare_and_run

if TYPE_CHECKING:
    from sqlalchemy import Connection
from sqlalchemy import MetaData


class CompareAndRunTestCase(ABC):
    """
    Base class for all tests that expect specific alembic generated code
    """

    disable_running = False
    config = Config()

    @abstractmethod
    def get_database_schema(self) -> MetaData: ...

    @abstractmethod
    def get_target_schema(self) -> MetaData: ...

    def insert_migration_data(self, connection: "Connection", database_schema: MetaData) -> None:
        pass

    @abstractmethod
    def get_expected_upgrade(self) -> str: ...

    @abstractmethod
    def get_expected_downgrade(self) -> str: ...

    def get_expected_imports(self) -> Optional[str]:
        return None

    def test_run(self, connection: "Connection"):
        old_config = get_configuration()
        alembic_postgresql_enum.set_configuration(self.config)
        database_schema = self.get_database_schema()
        target_schema = self.get_target_schema()

        database_schema.create_all(connection)
        self.insert_migration_data(connection, database_schema)

        compare_and_run(
            connection,
            target_schema,
            expected_upgrade=self.get_expected_upgrade(),
            expected_downgrade=self.get_expected_downgrade(),
            expected_imports=self.get_expected_imports(),
            disable_running=self.disable_running,
        )
        alembic_postgresql_enum.set_configuration(old_config)
