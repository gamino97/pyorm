from typing import Any

from pyorm.backends.sqlite import SQLiteBackend


class Database:
    _backend: SQLiteBackend | None = None

    @classmethod
    def configure_database(cls, backend_instance: Any):
        cls._backend = backend_instance

    @classmethod
    def get_backend(cls) -> SQLiteBackend:
        if cls._backend is None:
            raise Exception(
                "Database backend not configured. Call configure_database() first."
            )
        return cls._backend
