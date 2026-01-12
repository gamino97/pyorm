from typing import Any

from pyorm.backends.base import BaseBackend


class Database:
    _backend: BaseBackend | None = None

    @classmethod
    def configure_database(cls, backend_instance: Any):
        cls._backend = backend_instance

    @classmethod
    def get_backend(cls) -> BaseBackend:
        if cls._backend is None:
            raise Exception(
                "Database backend not configured. Call configure_database() first."
            )
        return cls._backend
