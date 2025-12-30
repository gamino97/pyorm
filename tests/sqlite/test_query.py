import decimal
from sqlite3 import Connection, Cursor
from typing import ClassVar

from pydantic import Field

from pyorm.models import Model


def test_create_table(db_connection: Connection):

    class Movie(Model):
        table_name: ClassVar[str] = "test_movie_creation"
        id: int = Field(json_schema_extra={"primary_key": True})
        title: str
        year: int
        score: float
        is_published: bool
        description: str | None = None
        budget: decimal.Decimal

    Movie.create_model()

    cursor: Cursor = db_connection.cursor()

    # Verify table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (Movie.table_name,),
    )
    assert cursor.fetchone() is not None

    # Verify column definitions
    cursor.execute(f"PRAGMA table_info({Movie.table_name})")
    columns = cursor.fetchall()

    column_map = {col[1]: col for col in columns}

    # Check 'id' column
    assert "id" in column_map
    assert column_map["id"][2] == "INTEGER"
    assert column_map["id"][3] == 0
    assert column_map["id"][5] == 1  # IS PRIMARY KEY

    # Check 'title' column
    assert "title" in column_map
    assert column_map["title"][2] == "TEXT"
    assert column_map["title"][3] == 1  # NOT NULL

    # Check 'year' column
    assert "year" in column_map
    assert column_map["year"][2] == "INTEGER"
    assert column_map["year"][3] == 1  # NOT NULL

    # Check 'score' column
    assert "score" in column_map
    assert column_map["score"][2] == "REAL"
    assert column_map["score"][3] == 1  # NOT NULL

    # Check 'is_published' column (bool should map to INTEGER)
    assert "is_published" in column_map
    assert column_map["is_published"][2] == "INTEGER"
    assert column_map["is_published"][3] == 1  # NOT NULL

    # Check 'description' column (nullable str should map to TEXT, NULL allowed)
    assert "description" in column_map
    assert column_map["description"][2] == "TEXT"
    assert column_map["description"][3] == 0  # NULL allowed

    # Check 'budget' column (Decimal should map to NUMERIC)
    assert "budget" in column_map
    assert column_map["budget"][2] == "NUMERIC"
    assert column_map["budget"][3] == 1  # NOT NULL


def test_drop_table(db_connection: Connection):

    class Movie(Model):
        table_name: ClassVar[str] = "test_movie_creation"
        title: str

    Movie.drop_model()
    cursor = db_connection.cursor()

    # Verify table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (Movie.table_name,),
    )
    assert cursor.fetchone() is None


def test_insert_table(db_connection: Connection):
    class Movie(Model):
        table_name: ClassVar[str] = "test_movie_creation"
        title: str
        id: int | None = Field(default=None, json_schema_extra={"primary_key": True})
        year: int
        score: float

    Movie.create_model()
    m1 = Movie(title="Hola", year=1997, score=7.8)
    m1.save()
    assert m1.title == "Hola"
    assert m1.year == 1997
    assert m1.score == 7.8
    assert m1.id is not None
    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT id, title, year, score FROM test_movie_creation where id = ?", (m1.id,)
    )
    result = cursor.fetchone()
    id, title, year, score = result
    assert id == m1.id
    assert title == m1.title
    assert year == m1.year
    assert score == m1.score


def test_select_many(db_connection: Connection):
    class Movie(Model):
        table_name: ClassVar[str] = "test_movie_creation"
        title: str
        id: int | None = Field(default=None, json_schema_extra={"primary_key": True})
        year: int
        score: float

    Movie.create_model()
    m1 = Movie(title="Movie 1", year=1997, score=7.8)
    m1.save()
    movies = Movie.filter(title="Movie 1")
    assert isinstance(movies, list)
    assert len(movies) == 1
    movie = movies[0]
    assert isinstance(movie, Movie)
    assert movie.id == m1.id
    assert movie.title == "Movie 1"
    assert movie.year == 1997
    assert movie.score == 7.8
    # Test multiple results
    Movie(title="Movie 2", year=1997, score=8.0).save()
    Movie(title="Movie 3", year=1996, score=7.8).save()
    Movie(title="Movie 4", year=1979, score=6.5).save()
    movies = Movie.filter(year=1997)
    assert len(movies) == 2
    # Test no result
    movies = Movie.filter(score=1.0)
    assert len(movies) == 0
