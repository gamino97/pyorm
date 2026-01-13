import logging
from typing import Any, ClassVar, TypeVar

from pydantic import BaseModel

from pyorm.database import Database
from pyorm.exceptions import DoesNotExist, MultipleObjectsReturned
from pyorm.utils import is_field_primary_key

from .utils import make_fields_optional

T = TypeVar("T", bound="Model")

logger = logging.getLogger("pyorm_model")


class Model(BaseModel):
    table_name: ClassVar[str]
    DoesNotExist: ClassVar[type[DoesNotExist]] = DoesNotExist
    MultipleObjectsReturned: ClassVar[type[MultipleObjectsReturned]] = (
        MultipleObjectsReturned
    )
    _pk_field: ClassVar[str | None] = None

    def model_post_init(self, context) -> None:
        self._modified_fields: list[str] = []
        if self.table_name is None or len(self.table_name) == 0:
            raise Exception("Table name must be defined")

    @classmethod
    def get_pk_field_name(cls) -> str:
        if cls._pk_field is not None:
            return cls._pk_field
        for field_name, field in cls.model_fields.items():
            if is_field_primary_key(field):
                cls._pk_field = field_name
                return field_name
        cls._pk_field = ""
        return ""

    def clean_modified_fields(self):
        self._modified_fields = []

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name in self.model_fields_set:
            self._modified_fields.append(name)

    @classmethod
    def filter(cls: type[T], _limit: None | int = None, **kwargs) -> list[T]:
        ModelOptional: T = make_fields_optional(cls)(**kwargs)
        query_fields = list(cls.__pydantic_fields__.keys())
        res = Database.get_backend().get_many(
            cls.table_name,
            ModelOptional.model_dump(exclude_unset=True),
            query_fields=query_fields,
        )
        instances: list[T] = [cls.model_validate(result) for result in res]
        return instances

    @classmethod
    def get(cls: type[T], **kwargs) -> T:
        instances: list[T] = cls.filter(**kwargs, _limit=2)
        if len(instances) == 1:
            return instances[0]
        if not instances:
            raise DoesNotExist
        raise MultipleObjectsReturned

    @classmethod
    def create_model(cls: type[T]) -> None:
        Database.get_backend().sql_create_db(cls.table_name, cls.__pydantic_fields__)

    @classmethod
    def drop_model(cls: type[T]) -> None:
        Database.get_backend().sql_drop_table(cls.table_name)

    def save(self) -> None:
        pk_field_name: str = self.get_pk_field_name()
        pk: Any | None = getattr(self, pk_field_name, None)
        if pk_field_name and pk is not None:
            update_data = {
                field: getattr(self, field, None) for field in self._modified_fields
            }
            filters = {pk_field_name: pk}
            Database.get_backend().update_item(self.table_name, update_data, filters)
            self.clean_modified_fields()
            return
        model_data: dict[str, Any] = self.model_dump(exclude_computed_fields=True)
        res: tuple[Any] | None = Database.get_backend().insert_item(
            self.table_name, model_data
        )
        if res is not None:
            model = self.model_validate(
                {attr: value for attr, value in zip(model_data.keys(), res)}
            )
            for attribute, value in model.model_dump().items():
                setattr(self, attribute, value)
            self.clean_modified_fields()
        else:
            logger.warning("No result was returned after insert")

    def delete(self) -> None:
        pk_field_name: str = self.get_pk_field_name()
        pk: Any | None = getattr(self, pk_field_name, None)
        if pk_field_name and pk is not None:
            filters = {pk_field_name: pk}
        else:
            filters = {
                field: getattr(self, field, None)
                for field in self.__pydantic_fields_set__
            }
        Database.get_backend().delete_item(self.table_name, filters)
