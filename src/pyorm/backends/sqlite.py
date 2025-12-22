import logging
import sqlite3

logger = logging.getLogger("sqlite_backend")


class SQLiteBackend:

    def execute(self, sql: str, cursor: sqlite3.Cursor, params: dict | list):
        logger.debug("Executing %s and params %s", sql, params)
        return cursor.execute(sql, params)

    def get_many(self, table_name, params: dict, query_fields: list | None = None):
        con = sqlite3.connect("tutorial.db")
        cur = con.cursor()
        sql = self.sql_select_build(table_name, params, query_fields)
        res = self.execute(sql, cur, params)
        rows = res.fetchall()
        cur.close()
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
