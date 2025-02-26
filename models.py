from datetime import datetime
from dataclasses import dataclass, field
from enum import StrEnum

from db import DB


class Types(StrEnum):
    JOB = 'job'
    STORY = 'story'
    COMMENT = 'comment'
    
    
@dataclass
class Item:
    id: int
    type: Types
    deleted: bool = False
    by: str = 'anon'
    time: float = datetime.now().timestamp()
    dead: bool = False
    kids: list[int] = field(default_factory=list)
    table_name: str = ''
    cursor = DB().cursor

    def __str__(self):
        return f'{self.id}'


@dataclass
class Story(Item):
    descendants: int = 0
    title: str = ''
    url: str = 'https://news.ycombinator.com/'
    text: str = ''
    score: int = 0

    def create_table(self):
        return self.cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS story(id integer primary key, descendants integer default {self.descendants},
         title text default {self.title},
         url text default {self.url}, 'text' text default {self.text}, score integer default ?, deleted boolean default ?,
         by text default ?, time float default ?, dead boolean default ?, type text default ?)
         """, (self.descendants, self.title, self.url, self.text, self.score,
              self.deleted, self.by, self.time, self.dead, self.type))



@dataclass
class Job(Item):
    title: str = ''
    text: str = ''
    score: int = 0
    url: str = 'https://news.ycombinator.com/'

    def create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS job(id integer primary key, title text default ?, text text default ?,
         score integer default ?, url text default ?, deleted boolean default ?, by text default ?, time float default ?,
         dead boolean default ?, type text default ?)
        """, (self.title, self.text, self.score, self.url, self.deleted, self.by, self.time, self.dead, self.type))
        

@dataclass
class Comment(Item):
    parent: int = 0
    text: str = ''

    def create_table(self):
        return self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS comment(id integer primary key, parent integer default ?, text text default ?,
            deleted boolean default ?, by text default ?, time float default ?, dead boolean default ?, type text default ?)
            """, (self.parent, self.text, self.deleted, self.by, self.time, self.dead, self.type))


@dataclass
class User:
    id: str
    created: int
    karma: int = 0
    about: str = ''
    delay: int = 0
    submitted: list[int] = field(default_factory=list)
    cursor = DB().cursor
    
    def __str__(self):
        return f'{self.id}'

    def create_table(self):
        return self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS user(id text primary key, created integer default ?, karma integer default ?,
            about text default ?, delay integer default ?)
            """, (self.created, self.karma, self.about, self.delay))


st = Story(id=455344, type="story")
res = st.create_table()
db = DB()
db.insert(table='story', data={'title': "Test Story", "id": 455344, "score": 45, "type": "story"})
