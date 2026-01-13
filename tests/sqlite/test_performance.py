from sqlite3 import Connection
from typing import ClassVar

import pytest
from pydantic import Field

from pyorm.models import Model


class Movie(Model):
    table_name: ClassVar[str] = "test_movie_creation"
    title: str
    id: int | None = Field(default=None, json_schema_extra={"primary_key": True})
    year: int
    score: float


@pytest.mark.parametrize("count", [10, 100, 1000])
def test_raw_sql_select(benchmark, db_connection: Connection, count: int):
    Movie.create_model()
    rows = []
    for i in range(count):
        rows.append(dict(title=f"Movie {i}", year=1900 + i, score=7.8))
    db_connection.executemany(
        "INSERT INTO test_movie_creation(title, year, score) VALUES(:title, :year, :score)",
        rows,
    )
    db_connection.commit()

    def raw_fetch():
        cursor = db_connection.execute("SELECT * FROM test_movie_creation")
        return cursor.fetchall()

    results = benchmark(raw_fetch)
    assert len(results) == count


@pytest.mark.parametrize("count", [10, 100, 1000])
def test_orm_select_all(benchmark, db_connection: Connection, count: int):
    Movie.create_model()
    rows = []
    for i in range(count):
        rows.append(dict(title=f"Movie {i}", year=1900 + i, score=7.8))
    db_connection.executemany(
        "INSERT INTO test_movie_creation(title, year, score) VALUES(:title, :year, :score)",
        rows,
    )
    db_connection.commit()

    def fetch_all():
        return list(Movie.filter())

    results = benchmark(fetch_all)
    assert len(results) == count


@pytest.mark.parametrize("count", [10, 100, 1000])
def test_orm_insert(benchmark, count):
    Movie.create_model()

    def insert_users():
        for i in range(count):
            Movie(title=f"Movie {i}", year=1900 + i, score=7.8).save()

    benchmark(insert_users)


@pytest.mark.parametrize("count", [10, 100, 1000])
def test_raw_sql_insert(benchmark, db_connection: Connection, count):
    Movie.create_model()

    def raw_insert():
        for i in range(count):
            db_connection.execute(
                "INSERT INTO test_movie_creation(title, year, score) VALUES(:title, :year, :score) RETURNING id, title, year, score",
                dict(title=f"Movie {i}", year=1900 + i, score=7.8),
            )
            db_connection.commit()

    benchmark(raw_insert)
