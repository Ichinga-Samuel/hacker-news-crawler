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

    def __init__(self, name: str = None):
        self.name = name or self.name

    @property
    def cursor(self):
        return self.connection.cursor()
