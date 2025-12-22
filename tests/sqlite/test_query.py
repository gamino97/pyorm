from typing import ClassVar

from pyorm.models import Model


class Movie(Model):
    table_name: ClassVar[str] = "movie"
    title: str
    year: int
    score: float


def test_select():
    m = Movie.findAll()
    print(m)
    for x in m:
        assert x.title != ""
