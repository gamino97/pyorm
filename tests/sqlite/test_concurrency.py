import concurrent.futures
from sqlite3 import Connection
from typing import ClassVar

from pydantic import Field

from pyorm.models import Model


class ConcurrentModel(Model):
    table_name: ClassVar[str] = "concurrent_model"
    id: int | None = Field(default=None, json_schema_extra={"primary_key": True})
    name: str


def test_concurrency_failure(db_connection: Connection):
    """
    This test attempts to use the shared database connection/cursor from multiple threads.
    It is EXPECTED to fail if the backend is not thread-safe.
    """
    ConcurrentModel.create_model()

    def worker(i):
        # Depending on the backend implementation, this might fail immediately
        # if check_same_thread is True (default for sqlite3), or cause race conditions
        ConcurrentModel(name=f"Thread-{i}").save()
        return True

    # Increase iterations to Ensure concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(worker, i) for i in range(100)]
        results = [f.result() for f in futures]
    for result in results:
        assert not isinstance(result, Exception)
