from functools import cached_property
from random import randint
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Literal

from db import DB


@dataclass
class Item:
    id: int = field(default_factory=lambda: randint(99_999, 1_000_000))
    deleted: bool = False
    by: str = "anon"
    time: float = datetime.now().timestamp()
    dead: bool = False
    type: Literal["job", "story", "comment", "poll", "pollopt"] = "story"
    title: str = "''"
    text: str = "''"
    score: int = 0
    url: str = "'https://news.ycombinator.com/'"
    descendants: int = 0
    parent: int = None
    table_name: str = ""
    kids: list[int] = field(default_factory=list)

    @cached_property
    def db(self):
        return DB()

    def create_table(self):
        return self.db.cursor.execute("""CREATE TABLE IF NOT EXISTS {table_name}(id integer primary key default {id},
                 title text default {title}, text text default {text}, descendants integer default {descendants},
                 parent integer default {parent}, score integer default {score}, url text default {url},
                 deleted boolean default {deleted}, 'by' text default {by},
                 time float default {time}, dead boolean default {dead}, type text default {type})
                """.format(**self.asdict()))

    async def insert(self, **kwargs):
        try:
            data = self.asdict()
            data.pop("table_name", None)
            data.pop("kids", None)
            data.update(kwargs)
            return self.db.cursor.execute(f"""INSERT INTO {self.table_name} ({','.join(data.keys())})
                VALUES ({('?,' * len(data.values()))[:-1]})""", tuple(data.values()))
        except Exception as exe:
            print(f"Error saving item: {exe}")

    def asdict(self):
        return asdict(self)

    async def show(self):
        res = self.db.cursor.execute("SELECT count(*) FROM {table_name}".format(table_name=self.table_name))
        res = res.fetchall()
        count = sum([i for r in res for i in r])
        print(f"Total items in {self.table_name}: {count}")

    def __str__(self):
        return f'{self.id}'


@dataclass
class Story(Item):
    table_name: str = "stories"


@dataclass
class Job(Item):
    type: str = "job"
    table_name: str = "job"
        

@dataclass
class Comment(Item):
    type: str = "comment"
    table_name: str = "comment"


@dataclass
class Poll(Item):
    type: str = "poll"
    table_name: str = "poll"


@dataclass
class PollOpt(Item):
    type: str = "pollopt"
    table_name: str = "pollopt"


@dataclass
class User:
    id: str = 'anon'
    created: float = datetime.now().timestamp()
    karma: int = 0
    about: str = "''"
    delay: int = 0
    table_name: str = "user"
    submitted: list[int] = field(default_factory=list)

    def __str__(self):
        return f'{self.id}'

    def create_table(self):
        return self.db.cursor.execute("""CREATE TABLE IF NOT EXISTS user(id text primary key default {id},
         created integer default {created}, karma integer default {karma}, about text default {about},
         delay integer default {delay})""".format(**self.asdict()))

    @cached_property
    def db(self):
        return DB()

    async def insert(self, **kwargs):
        try:
            data = self.asdict()
            data.pop("table_name", None)
            data.pop("submitted", None)
            data.update(kwargs)
            return self.db.cursor.execute(f"""INSERT INTO {self.table_name} ({','.join(data.keys())})
                VALUES ({('?,' * len(data.values()))[:-1]})""", tuple(data.values()))
        except Exception as exe:
            print(f"Error saving item: {exe}")

    async def show(self):
        res = self.db.cursor.execute("SELECT count(*) FROM {table_name}".format(table_name=self.table_name))
        res = res.fetchall()
        count = sum([i for r in res for i in r])
        print(f"Total items in {self.table_name}: {count}")

    def asdict(self):
        return asdict(self)


# import asyncio
# async def main():
#     s = Story()
#     s.create_table()
#     await s.insert()
#     await s.show()
#
# asyncio.run(main())
