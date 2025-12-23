from typing import ClassVar, TypeVar

from pydantic import BaseModel

from .backends.sqlite import SQLiteBackend
from .utils import make_fields_optional

T = TypeVar("T", bound="Model")


class Model(BaseModel):
    table_name: ClassVar[str]

    def model_post_init(self, context) -> None:
        self._modified_fields: list[str] = []
        if self.table_name is None or len(self.table_name) == 0:
            raise Exception("Table name must be defined")

    def clean_modified_fields(self):
        self._modified_fields = []

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name in self.model_fields_set:
            self._modified_fields.append(name)

    @classmethod
    def findAll(cls: type[T], **kwargs) -> list[T]:
        ModelOptional = make_fields_optional(cls)(**kwargs)
        query_fields = list(cls.model_fields.keys())
        res = SQLiteBackend().get_many(
            cls.table_name,
            ModelOptional.model_dump(exclude_unset=True),
            query_fields=query_fields,
        )
        instances = [ModelOptional.model_validate(result) for result in res]
        return instances

    @classmethod
    def createDb(cls: type[T]):
        SQLiteBackend().sql_create_db(cls.table_name, cls.__pydantic_fields__)
