import logging
from typing import Any, ClassVar, TypeVar

from pydantic import BaseModel

from pyorm.database import Database
from pyorm.utils import is_field_primary_key

from .utils import make_fields_optional

T = TypeVar("T", bound="Model")

logger = logging.getLogger("pyorm_model")


class Model(BaseModel):
    table_name: ClassVar[str]

    def model_post_init(self, context) -> None:
        self._modified_fields: list[str] = []
        if self.table_name is None or len(self.table_name) == 0:
            raise Exception("Table name must be defined")

    @classmethod
    def get_pk_field_name(cls) -> str:
        for field_name, field in cls.model_fields.items():
            if is_field_primary_key(field):
                return field_name
        return ""

    def clean_modified_fields(self):
        self._modified_fields = []

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name in self.model_fields_set:
            self._modified_fields.append(name)

    @classmethod
    def filter(cls: type[T], **kwargs) -> list[T]:
        ModelOptional: T = make_fields_optional(cls)(**kwargs)
        query_fields = list(cls.model_fields.keys())
        res = Database.get_backend().get_many(
            cls.table_name,
            ModelOptional.model_dump(exclude_unset=True),
            query_fields=query_fields,
        )
        instances: list[T] = [cls.model_validate(result) for result in res]
        return instances

    @classmethod
    def create_model(cls: type[T]) -> None:
        Database.get_backend().sql_create_db(cls.table_name, cls.__pydantic_fields__)

    @classmethod
    def drop_model(cls: type[T]) -> None:
        Database.get_backend().sql_drop_table(cls.table_name)

    def save(self) -> None:
        pk_field_name: str = self.get_pk_field_name()
        if pk_field_name:
            pk: Any | None = getattr(self, pk_field_name, None)
            model_data: dict[str, Any] = self.model_dump()
            if pk is None:
                res: tuple[Any] | None = Database.get_backend().insert_item(
                    self.table_name, model_data
                )
                if res is not None:
                    for attribute, value in zip(model_data.keys(), res):
                        print(f"attribute {attribute}, value: {value}")
                        setattr(self, attribute, value)
                    self.clean_modified_fields()
                else:
                    logger.warning("No result was returned after insert")
        else:
            pass
