import decimal
import logging
import sqlite3
import types
from typing import Any, Union, get_origin

from pydantic.fields import FieldInfo

from pyorm.utils import is_field_primary_key

from .base import BaseBackend

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


class SQLiteBackend(BaseBackend):

    def get_connection(self):
        return self.connection

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

    def get_many(
        self,
        table_name: str,
        params: dict,
        query_fields: list | None = None,
        _limit: int | None = None,
    ) -> list[Any]:
        sql = self.sql_select_build(table_name, params, query_fields)
        with self.connection:
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

    def sql_create_db(self, table_name: str, fields: dict[str, FieldInfo]):
        logger.debug(f"{table_name=} {fields=}")
        column_definitions: list[str] = []
        for field_name, field in fields.items():
            column_definition = self.get_column_definition(field_name, field)
            column_definitions.append(column_definition)
        column_definition_str = ", ".join(column_definitions)
        sql = f"CREATE TABLE {table_name}({column_definition_str})"
        with self.connection:
            self.execute(sql, self.cursor)

    def sql_drop_table(self, table_name: str) -> None:
        logger.info("Dropping table %s", table_name)
        sql: str = f"DROP TABLE IF EXISTS {table_name}"
        with self.connection:
            self.execute(sql, self.cursor)

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

    def insert_item(self, table_name: str, params: dict) -> tuple | None:
        sql = self.sql_insert_row(table_name, list(params.keys()))
        with self.connection:
            res = self.execute(sql, self.cursor, self._clean_params(params))
            return res.fetchone()

    def _clean_params(self, params: dict) -> dict:
        new_params = params.copy()
        for key, v in params.items():
            if isinstance(v, decimal.Decimal):
                new_params[key] = str(v)
            if isinstance(v, bool):
                new_params[key] = 1 if v else 0
        return new_params

    def update_item(self, table_name: str, params: dict, filters: dict) -> list:
        sql: str = self.sql_update_row(table_name, params, filters)
        with self.connection:
            res = self.execute(sql, self.cursor, self._clean_params(params | filters))
            return res.fetchall()

    def delete_item(self, table_name: str, filters: dict) -> None:
        sql = self.sql_delete_row(table_name, filters)
        with self.connection:
            self.execute(sql, self.cursor, self._clean_params(filters))

    def __del__(self, *args, **kwargs):
        logger.debug("Closing connection to SQLite '%s' database", self.database_path)
        self.cursor.close()
        self.connection.close()
