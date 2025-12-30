from sqlite3 import Connection
from typing import Generator

import pytest

from pyorm.backends.sqlite import SQLiteBackend
from pyorm.database import Database


@pytest.fixture(scope="function")
def db_connection(prepare_sqlite_database) -> Generator[Connection, None, None]:
    """Fixture to create an in-memory SQLite database"""

    connection: Connection = Database.get_backend().connection
    yield connection  # Provide the connection to the test


@pytest.fixture(autouse=True, scope="function")
def prepare_sqlite_database():
    instance = SQLiteBackend(":memory:")
    Database.configure_database(instance)
    yield
    Database.configure_database(None)
    del instance
