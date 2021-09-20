
import sqlalchemy as sa
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql import text
from collections import namedtuple
import pandas as pd

success = info = print

class BaseConn:
    echo = False
    engine = None

    def __init__(self, cred) -> None:
        self.type = cred['type']
        self.cred = cred
        self.get_engine()
        success(f"connected to {cred['type']}")

    def get_engine(self, force=False) -> Engine:
        if force or not self.engine:
            self.engine = sa.create_engine(self._conn_str(), echo=self.echo)
        return self.engine
    
    def execute(self, sql):
        return self.get_engine().execute(sql)

    def stream(self, sql, fetch_size=10000, dtype="namedtuple"):
        result = self.get_engine().execute(sql)
        self._fields = [key.lower() for key in result.keys()]
        counter = 0

        if dtype == "namedtuple":
            Record = namedtuple("Row", self._fields)
            make_rec = lambda row: Record(*row)
        else:
            make_rec = lambda row: row

        while True:
            rows = result.fetchmany(fetch_size)
            if rows:
                for row in rows:
                    counter += 1
                    if dtype == "namedtuple":
                        yield make_rec(row)
                    elif dtype == "tuple":
                        yield tuple(row)
                    else:
                        yield row
            else:
                break

    def query(self, sql, dtype="namedtuple"):
        if dtype == "dataframe":
            columns = []
            rows = list(self.stream(sql, dtype="namedtuple"))
            if len(rows) > 0:
              columns = self._fields
            return pd.DataFrame(rows, columns=columns)
        if self.echo:
            info(sql)
        return list(self.stream(sql, dtype=dtype))

class SnowflakeConn(BaseConn):
    def _conn_str(self) -> str:
        self.database = self.cred['database']
        args = [
            "snowflake://",
            self.cred['user'],
            ":",
            self.cred['password'],
            "@",
            self.cred['account'],
            "/",
            self.cred['database'],
            "/",
            self.cred['schema'],
            "?warehouse=",
            self.cred['warehouse'],
            "&role=",
            self.cred['role'],
        ]
        return "".join(args)