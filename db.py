from typing import Self
import sqlite3


class DB:
    connection: sqlite3.Connection
    name: str = "db.sqlite3"
    _instance: Self

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance.connection = sqlite3.connect(cls._instance.name)
        return cls._instance

    def __init__(self, name: str = "db.sqlite3"):
        self.name = name

    def insert(self, *, table: str, data: dict):
        self.cursor.execute(f"""INSERT INTO {table} ({','.join(data.keys())})
          VALUES ({('?,' * len(data.values()))[:-1]})""", tuple(data.values()))

    @property
    def cursor(self):
        return self.connection.cursor()
