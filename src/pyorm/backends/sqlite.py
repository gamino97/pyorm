import decimal
import logging
import sqlite3
import types
from typing import Any, Union, get_args, get_origin

from pydantic.fields import FieldInfo

from pyorm.utils import is_field_primary_key

UnionType = getattr(types, "UnionType", Union)
NoneType = type(None)

logger = logging.getLogger("sqlite_backend")

type_affinities = {
    str: "TEXT",
    decimal.Decimal: "NUMERIC",
    int: "INTEGER",
    float: "REAL",
    bool: "INTEGER",
}


class SQLiteBackend:
    def __init__(self, database_path: str, *args, **kwargs):
        logger.debug("Initializing SQLiteBackend in %s", database_path)
        self.database_path = database_path
        self.connection = sqlite3.connect(self.database_path)
        self.cursor = self.connection.cursor()

    def execute(
        self, sql: str, cursor: sqlite3.Cursor, params: dict | list | None = None
    ):
        if params is None:
            params = []
        logger.debug("Executing %s and params %s", sql, params)
        return cursor.execute(sql, params)

    def get_many(self, table_name, params: dict, query_fields: list | None = None):
        sql = self.sql_select_build(table_name, params, query_fields)
        res = self.execute(sql, self.cursor, params)
        rows = res.fetchall()
        if query_fields:
            values = []
            for row in rows:
                values.append(
                    {
                        field_name: field_value
                        for field_name, field_value in zip(query_fields, row)
                    }
                )
            return values
        return rows

    def sql_select_build(
        self, table_name: str, filter_fields: dict, query_fields: list | None = None
    ):
        query_fields_str = "*"
        if query_fields_str is not None and query_fields:
            query_fields_str = ", ".join(query_fields)
        filter_str = ""
        if filter_fields:
            filters = " AND ".join(
                f"{field} = :{field}" for field in filter_fields.keys()
            )
            filter_str = f" WHERE {filters}"
        return f"SELECT {query_fields_str} FROM {table_name}{filter_str}"

    def sql_create_db(self, table_name: str, fields: dict[str, FieldInfo]):
        logger.debug(f"{table_name=} {fields=}")
        column_definitions = []
        for field_name, field in fields.items():
            column_definition = self.get_column_definition(field_name, field)
            logger.debug(column_definition)
            column_definitions.append(column_definition)
        column_definition_str = ", ".join(column_definitions)
        sql = f"CREATE TABLE {table_name}({column_definition_str})"
        logger.debug(sql)
        self.execute(sql, self.cursor)

    def sql_drop_table(self, table_name: str) -> None:
        logger.info("Dropping table %s", table_name)
        sql: str = f"DROP TABLE IF EXISTS {table_name}"
        logger.debug(sql)
        self.execute(sql, self.cursor)

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
        logger.debug("Type of field annotation %s is %s", field.annotation, annotation)
        return annotation

    def get_column_definition(self, name: str, field: FieldInfo) -> str:
        field_type = self.get_field_type(field)
        type_affinity: str = type_affinities.get(field_type, type_affinities[str])
        constraints = self.get_column_constraints(field)
        logger.debug(
            "Field %s, Field type: %s constraints: %s", name, field_type, constraints
        )
        return f"{name} {type_affinity.upper()}{constraints}"

    def get_column_constraints(self, field: FieldInfo) -> str:
        constraints = ""
        origin = get_origin(field.annotation)
        if is_field_primary_key(field):
            constraints = f"{constraints} PRIMARY KEY"
        elif origin is None or not self.is_union_type(origin):
            constraints = f"{constraints} NOT NULL"
        return constraints

    def is_union_type(self, type: type[Any]) -> bool:
        return type is UnionType or type is Union

    def insert_item(self, table_name: str, params: dict) -> tuple | None:
        sql = self.sql_insert_row(table_name, list(params.keys()))
        res = self.execute(sql, self.cursor, params)
        return res.fetchone()

    def sql_insert_row(self, table_name: str, column_names: list[str]) -> str:
        column_names_str = ", ".join(column_names)
        named_placeholders_list = (f":{placeholder}" for placeholder in column_names)
        named_placeholders = ", ".join(named_placeholders_list)
        sql: str = (
            f"INSERT INTO {table_name}({column_names_str}) VALUES({named_placeholders}) RETURNING {column_names_str}"
        )
        return sql

    def __del__(self, *args, **kwargs):
        logger.debug("Closing connection to SQLite '%s' database", self.database_path)
        self.cursor.close()
        self.connection.close()
