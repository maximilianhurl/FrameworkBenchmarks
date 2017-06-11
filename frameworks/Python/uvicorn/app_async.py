from functools import partial
from random import randint
import asyncpg
import ujson as json


def get_query_count(query_string):
    # helper to deal with the querystring passed in
    queries = query_string.get('queries', None)
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


random_int = partial(randint, 1, 10000)


async def json_endpoint(pool, message):
    content = json.dumps({'message': 'Hello, world!'}).encode('utf-8')
    response = {
        'status': 200,
        'headers': [
            [b'content-type', b'application/json'],
        ],
        'content': content
    }
    message['reply_channel'].send(response)


async def plaintext_endpoint(pool, message):
    content = b'Hello, world!'
    response = {
        'status': 200,
        'headers': [
            [b'content-type', b'text/plain'],
        ],
        'content': content
    }
    message['reply_channel'].send(response)


async def handle_404(pool, message):
    content = b'Not found'
    response = {
        'status': 404,
        'headers': [
            [b'content-type', b'text/plain'],
        ],
        'content': content
    }
    message['reply_channel'].send(response)


async def db_endpoint(pool, message):
    """Test Type 2: Single database object"""
    async with pool.acquire() as connection:
        row = await connection.fetchrow('SELECT id, "randomnumber" FROM "world" WHERE id = ' + str(random_int()))
        world = {'id': row[0], 'randomNumber': row[1]}
        content = json.dumps(world).encode('utf-8')
        response = {
            'status': 200,
            'headers': [
                [b'content-type', b'application/json'],
            ],
            'content': content
        }
        message['reply_channel'].send(response)


async def queries_endpoint(pool, messsage):
    """Test Type 3: Multiple database queries"""
    queries = get_query_count(message.get('query_string', {}))
    db_connection = db_backend.engine.connect()
    worlds = []
    for i in range(queries):
        result = db_connection.execute('SELECT id, "randomnumber" FROM "world" WHERE id = ' + str(rp())).fetchone()
        worlds.append({'id': result[0], 'randomNumber': result[1]})
    return worlds


async def fortunes_endpoint(pool, messsage):
    """Test 4: Fortunes"""
    db_connection = db_backend.engine.connect()
    fortunes = [(f.id, f.message) for f in db_connection.execute('SELECT * FROM "Fortune"')]
    fortunes.append((0, u'Additional fortune added at request time.'))
    fortunes = sorted(fortunes, key=itemgetter(1))
    db_connection.close()
    fortune_template = templates.get_template('fortune-raw.html')
    return fortune_template.render(fortunes=fortunes)


async def updates_endpoint(pool, messsage):
    """Test 5: Database Updates"""
    queries = get_query_count(message.get('query_string', {}))

    db_connection = db_backend.engine.connect()

    worlds = []
    for i in range(queries):
        world = db_connection.execute('SELECT * FROM "world" WHERE id=%s', (rp(),)).fetchone()
        randomNumber = rp()
        worlds.append({'id': world['id'], 'randomNumber': randomNumber})
        db_connection.execute('UPDATE "world" SET "randomnumber"=%s WHERE id=%s', (randomNumber, world['id']))
    db_connection.close()
    return Response(worlds, headers={'Content-Type': 'application/json'})


routes = {
    '/json': json_endpoint,
    '/plaintext': plaintext_endpoint,
    '/db': db_endpoint,
    '/queries': queries_endpoint,
    '/fortune': fortunes_endpoint,
    '/updates': updates_endpoint,
}


async def main(message):
    pool = await asyncpg.create_pool(
        database='hello_world',
        user='benchmarkdbuser',
        password='benchmarkdbpass',
        host='localhost',
        port='5432',
    )
    path = message['content']['path']
    await routes.get(path, handle_404)(pool, message)
