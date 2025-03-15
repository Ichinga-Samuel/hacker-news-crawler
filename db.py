import sqlite3
from pathlib import Path
from typing import Self


class DB:
    connection: sqlite3.Connection
    database: str = Path("db.sqlite3")
    _instance: Self

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance.connection = sqlite3.connect(cls._instance.database)
        return cls._instance

    def __init__(self, database: str = None):
        self.database = database or self.database

    @property
    def cursor(self):
        return self.connection.cursor()
