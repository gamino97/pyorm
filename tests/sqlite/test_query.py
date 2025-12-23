import decimal
import logging
from typing import ClassVar

from pyorm.models import Model


def test_select():
    class Movie(Model):
        table_name: ClassVar[str] = "movie"
        title: str
        year: int
        score: float

    m = Movie.findAll()
    print(m)
    for x in m:
        assert x.title != ""


def test_create_table(caplog):
    class Movie(Model):
        table_name: ClassVar[str] = "movie11"
        string_field: str
        int_year: int
        score: float
        null_field: str | None = None
        decimal_field: decimal.Decimal

    caplog.set_level(logging.DEBUG)
    Movie.createDb()
    assert False
