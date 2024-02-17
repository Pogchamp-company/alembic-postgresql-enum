from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from tests.base.render_and_run import compare_and_run

if TYPE_CHECKING:
    from sqlalchemy import Connection
from sqlalchemy import MetaData


class CompareAndRunTestCase(ABC):
    @abstractmethod
    def get_database_schema(self) -> MetaData:
        ...

    @abstractmethod
    def get_target_schema(self) -> MetaData:
        ...

    def insert_migration_data(self, connection: "Connection"):
        pass

    @abstractmethod
    def get_expected_upgrade(self) -> str:
        ...

    @abstractmethod
    def get_expected_downgrade(self) -> str:
        ...

    def test_run(self, connection: "Connection"):
        database_schema = self.get_database_schema()
        target_schema = self.get_target_schema()

        database_schema.create_all(connection)
        self.insert_migration_data(connection)

        compare_and_run(
            connection,
            target_schema,
            expected_upgrade=self.get_expected_upgrade(),
            expected_downgrade=self.get_expected_downgrade(),
        )
