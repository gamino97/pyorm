import decimal
from sqlite3 import Connection, Cursor
from typing import ClassVar

from pyorm.models import Model

# def test_select():
#     class Movie(Model):
#         table_name: ClassVar[str] = "movie"
#         title: str
#         year: int
#         score: float

#     m = Movie.findAll()
#     print(m)
#     for x in m:
#         assert x.title != ""


def test_create_table(db_connection: Connection):

    class Movie(Model):
        table_name: ClassVar[str] = "test_movie_creation"
        title: str
        year: int
        score: float
        is_published: bool
        description: str | None = None
        budget: decimal.Decimal

    Movie.createDb()

    cursor: Cursor = db_connection.cursor()

    # Verify table exists
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{Movie.table_name}'"
    )
    assert cursor.fetchone() is not None

    # Verify column definitions
    cursor.execute(f"PRAGMA table_info({Movie.table_name})")
    columns = cursor.fetchall()

    column_map = {col[1]: col for col in columns}

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
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{Movie.table_name}'"
    )
    assert cursor.fetchone() is None
