from apistar import App, http, Route, wsgi
from apistar.backends import SQLAlchemy
from apistar.templating import Templates
from apistar.http import Response
from functools import partial
from operator import attrgetter, itemgetter
from os import environ
from random import randint
from sqlalchemy import Column, Integer, Unicode
from sqlalchemy.ext.declarative import declarative_base
import json


Base = declarative_base()


# orm table
class World(Base):
    __tablename__ = "world"
    id = Column(Integer, primary_key=True)
    randomnumber = Column(Integer)  # in MySQL this is randomNumber

    def serialize(self):
        """Return object data in easily serializeable format"""
        return {
            'id': self.id,
            'randomNumber': self.randomnumber,
        }


# orm table
class Fortune(Base):
    __tablename__ = "Fortune"
    id = Column(Integer, primary_key=True)
    message = Column(Unicode)

    def serialize(self):
        return {
            'id': self.id,
            'message': self.message,
        }


rp = partial(randint, 1, 10000)


def get_query_count(queries):
    # helper to deal with the querystring passed in
    if queries:
        try:
            query_count = int(queries)
            if query_count < 1:
                return 1
            if query_count > 500:
                return 500
            return query_count
        except ValueError:
            pass
    return 1


def json_view() -> wsgi.WSGIResponse:
    """Test Type 1: JSON Serialization"""
    content = json.dumps({'message': 'Hello, world!'}).encode('utf-8')
    return wsgi.WSGIResponse(
        '200 OK',
        [
            ('Content-Type', 'application/json'),
            ('Content-Length', str(len(content)))
        ],
        [content]
    )


def plaintext_view() -> wsgi. WSGIResponse:
    """Test Type 6: Plaintext """
    content = b'Hello, world!'
    return wsgi.WSGIResponse(
        '200 OK',
        [
            ('Content-Type', 'text/plain'),
            ('Content-Length', str(len(content)))
        ],
        [content]
    )


def get_random_world_single(db_backend: SQLAlchemy):
    """Test Type 2: Single Database Query"""
    session = db_backend.session_class()
    world = session.query(World).get(rp())
    return world.serialize()


def get_random_world_single_raw(db_backend: SQLAlchemy):
    """Test Type 2: Single Database Query"""
    db_connection = db_backend.engine.connect()
    result = db_connection.execute('SELECT id, "randomnumber" FROM "world" WHERE id = ' + str(rp())).fetchone()
    world = {'id': result[0], 'randomNumber': result[1]}
    db_connection.close()
    return world


def get_random_world(db_backend: SQLAlchemy, queries: http.QueryParam):
    """Test Type 3: Multiple database queries"""
    queries = get_query_count(queries)
    session = db_backend.session_class()
    get = session.query(World).get
    worlds = [get(rp()).serialize() for _ in range(queries)]
    return worlds


def get_random_world_raw(db_backend: SQLAlchemy, queries: http.QueryParam):
    """Test Type 3: Multiple database queries"""
    queries = get_query_count(queries)
    db_connection = db_backend.engine.connect()
    worlds = []
    for i in range(queries):
        result = db_connection.execute('SELECT id, "randomnumber" FROM "world" WHERE id = ' + str(rp())).fetchone()
        worlds.append({'id': result[0], 'randomNumber': result[1]})
    return worlds


def fortune_orm(db_backend: SQLAlchemy, templates: Templates):
    """Test 4: Fortunes"""
    session = db_backend.session_class()
    fortunes = session.query(Fortune).all()
    fortunes.append(Fortune(id=0, message="Additional fortune added at request time."))
    fortunes.sort(key=attrgetter('message'))
    fortune_template = templates.get_template('fortune.html')
    return fortune_template.render(fortunes=fortunes)


def fortune_raw(db_backend: SQLAlchemy, templates: Templates):
    """Test 4: Fortunes"""
    db_connection = db_backend.engine.connect()
    fortunes = [(f.id, f.message) for f in db_connection.execute('SELECT * FROM "Fortune"')]
    fortunes.append((0, u'Additional fortune added at request time.'))
    fortunes = sorted(fortunes, key=itemgetter(1))
    db_connection.close()
    fortune_template = templates.get_template('fortune-raw.html')
    return fortune_template.render(fortunes=fortunes)


def updates(db_backend: SQLAlchemy, queries: http.QueryParam):
    """Test 5: Database Updates"""
    queries = get_query_count(queries)

    ids = [rp() for _ in range(queries)]
    ids.sort()  # To avoid deadlock

    worlds = []
    session = db_backend.session_class()
    for id in ids:
        world = session.query(World).get(id)
        world.randomnumber = rp()
        worlds.append(world.serialize())
    session.commit()
    session.close()
    return worlds


def raw_updates(db_backend: SQLAlchemy, queries: http.QueryParam) -> Response:
    """Test 5: Database Updates"""
    queries = get_query_count(queries)

    db_connection = db_backend.engine.connect()

    worlds = []
    for i in range(queries):
        world = db_connection.execute('SELECT * FROM "world" WHERE id=%s', (rp(),)).fetchone()
        randomNumber = rp()
        worlds.append({'id': world['id'], 'randomNumber': randomNumber})
        db_connection.execute('UPDATE "world" SET "randomnumber"=%s WHERE id=%s', (randomNumber, world['id']))
    db_connection.close()
    return Response(worlds, headers={'Content-Type': 'application/json'})


DBHOST = environ.get('DBHOST', 'localhost')


settings = {
    "DATABASE": {
        "URL": 'postgresql://benchmarkdbuser:benchmarkdbpass@localhost:5432/hello_world',
        "METADATA": Base.metadata
    },
    "TEMPLATES": {
        "DIRS": ["templates"]
    }
}


routes = [
    Route('/json', 'GET', json_view),
    Route('/plaintext', 'GET', plaintext_view),
    Route('/db', 'GET', get_random_world_single),
    Route('/raw-db', 'GET', get_random_world_single_raw),
    Route('/queries', 'GET', get_random_world),
    Route('/raw-queries', 'GET', get_random_world_raw),
    Route('/fortune', 'GET', fortune_orm),
    Route('/raw-fortune', 'GET', fortune_raw),
    Route('/updates', 'GET', updates),
    Route('/raw-updates', 'GET', raw_updates),
]

app = App(routes=routes, settings=settings)
