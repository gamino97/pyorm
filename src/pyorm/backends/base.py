import abc
import types
from typing import Any, Union, get_args, get_origin

from pydantic.fields import FieldInfo

UnionType = getattr(types, "UnionType", Union)
NoneType = type(None)


class BaseBackend(abc.ABC):

    @abc.abstractmethod
    def get_connection(self) -> Any:
        pass

    @abc.abstractmethod
    def execute(self, sql: str, cursor: Any, params: dict | list | None = None) -> Any:
        """Execute `sql` statement in the database"""

    @abc.abstractmethod
    def get_many(
        self,
        table_name: str,
        params: dict,
        query_fields: list | None = None,
        _limit: int | None = None,
    ) -> list[Any]:
        """Get various items from the database"""

    def sql_select_build(
        self,
        table_name: str,
        filter_fields: dict,
        query_fields: list | None = None,
        _limit: int | None = None,
    ):
        query_fields_str = "*"
        if query_fields:
            query_fields_str = ", ".join(query_fields)
        filter_str = self._get_where_sql(filter_fields)
        limit_str = ""
        if _limit is not None:
            limit_str = f" LIMIT {_limit}"
        return f"SELECT {query_fields_str} FROM {table_name}{filter_str}{limit_str}"

    @abc.abstractmethod
    def sql_create_db(self, table_name: str, fields: dict[str, FieldInfo]):
        """Get SQL statement for creating a table in the database"""

    @abc.abstractmethod
    def sql_drop_table(self, table_name: str) -> None:
        """Execute statement for deleting some table from the database"""

    def get_field_type(self, field: FieldInfo) -> Any:
        annotation = field.annotation
        origin = get_origin(annotation)
        if origin is None:
            return field.annotation
        if self.is_union_type(origin):
            args = get_args(annotation)
            if len(args) > 2:
                raise ValueError("Cannot have a non-optional union as a column")
            if args[0] is None or args[0] is NoneType:
                annotation = args[1]
            else:
                annotation = args[0]
        return annotation

    @abc.abstractmethod
    def get_column_definition(self, name: str, field: FieldInfo) -> str:
        """Get column definitions for the database based on the field type"""

    @abc.abstractmethod
    def get_column_constraints(self, field: FieldInfo) -> str:
        """Get database constraints based on the field type"""

    def is_union_type(self, type: type[Any]) -> bool:
        return type is UnionType or type is Union

    @abc.abstractmethod
    def insert_item(self, table_name: str, params: dict) -> tuple | None:
        """Get SQL insert statement, and execute it in the database"""

    def sql_insert_row(self, table_name: str, column_names: list[str]) -> str:
        column_names_str = ", ".join(column_names)
        named_placeholders_list = (f":{placeholder}" for placeholder in column_names)
        named_placeholders = ", ".join(named_placeholders_list)
        sql: str = (
            f"INSERT INTO {table_name}({column_names_str}) VALUES({named_placeholders}) RETURNING {column_names_str}"  # noqa: E501
        )
        return sql

    @abc.abstractmethod
    def update_item(self, table_name: str, params: dict, filters: dict) -> list:
        """Get SQL update statement, and execute it in the database"""

    def sql_update_row(self, table_name, params: dict, filters: dict) -> str:
        column_name_list: str = ", ".join(
            f"{column} = :{column}" for column in params.keys()
        )
        where_sql: str = self._get_where_sql(filters)
        return f"UPDATE {table_name} SET {column_name_list}{where_sql}"

    def _get_where_sql(self, filters: dict) -> str:
        if not filters:
            return ""
        joined_filters_list: list[str] = []
        for field, value in filters.items():
            if value is None:
                joined_filters_list.append(f"{field} IS NULL")
            else:
                joined_filters_list.append(f"{field} = :{field}")
        joined_filters: str = " AND ".join(joined_filters_list)
        filter_str = f" WHERE {joined_filters}"
        return filter_str

    @abc.abstractmethod
    def delete_item(self, table_name: str, filters: dict) -> None:
        """Get SQL delete statement, and execute it in the database"""

    def sql_delete_row(self, table_name, filters: dict) -> str:
        where_sql = self._get_where_sql(filters)
        return f"DELETE FROM {table_name}{where_sql}"
