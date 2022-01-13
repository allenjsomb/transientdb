import asyncio
import os
import sqlite3
import configparser
import aiofiles
from datetime import datetime as dt, timedelta as td
from sanic import Sanic
from sanic.signals import Event
from sanic.log import logger
from sanic.exceptions import SanicException
from sanic.response import text
from routes import database
from util.dbutil import dump_data, get_tables, load_data
from setproctitle import setproctitle

cfg = configparser.ConfigParser()
db = sqlite3.connect(':memory:')
db.row_factory = sqlite3.Row

app_name = 'TransientDB'

app = Sanic(app_name)
app.blueprint(database.bp)


@app.middleware('request')
async def auth(request):
    auth = cfg['auth'] if 'auth' in cfg.sections() else None
    if auth:
        if auth.get('token') != request.headers.get('x-auth-token'):
            raise SanicException('Unauthorized', status_code=401)


@app.middleware('request')
async def inject_ctx(request):
    request.ctx.app = app
    request.ctx.cfg = cfg
    request.ctx.db = db


@app.middleware('response')
async def before_responding(request, response):
    response.headers['x-xss-protection'] = '1; mode=block'
    response.headers['Server'] = app_name
    #response.headers['Strict-Transport-Security'] = 'max-age=31536000 ; includeSubDomains'
    response.headers['X-Frame-Options'] = 'deny'
    response.headers['X-Content-Type-Options'] = 'nosniff'


@app.signal(Event.SERVER_INIT_BEFORE)
async def init(app, loop):
    logger.info('Initializing System')
    for root, _, files in os.walk(cfg['server'].get('schemas_folder', '/transientdb/schemas')):
        for filename in files:
            logger.info(f'Executing content of {root}/{filename}')
            async with aiofiles.open(f'{root}/{filename}', mode='r') as f:
                content = await f.read()
                for sql in content.split(';'):
                    db.cursor().execute(sql)


@app.signal(Event.SERVER_INIT_AFTER)
async def start(app, loop):
    logger.info('Starting System')
    for root, _, files in os.walk(cfg['server'].get('data_folder', '/transientdb/data')):
        for filename in files:
            parts = os.path.splitext(filename)
            if len(parts) < 2 or parts[1] != '.csv':
                logger.info(f'Ignoring file {root}/{filename} ... will not load.')
                continue
            app.add_task(load_data(db, parts[0], f'{root}/{filename}'))
    
    app.add_task(maintenance(), name='__maintenance__')


@app.signal(Event.SERVER_SHUTDOWN_BEFORE)
async def shutdown(app, loop):
    logger.info('Shut down triggered - removing tasks.')
    try:
        await app.cancel_task('__maintenance__')
    except:
        pass


@app.route('/')
async def root(request):
    return text(app_name)


async def maintenance():
    logger.info('Maintenance routine started')
    current_dump = dict()
    dump = cfg['dump'] if 'dump' in cfg.sections() else {}
    interval = abs(int(dump.get('interval_minutes', 15)))
    logger.info(f'Dump interval is {interval} minutes')
    while True:
        tasks = set()
        for t in app.tasks:
            tasks.add(t.get_name())

        for table in get_tables(db):
            dump_time = current_dump.get(table)
            if not dump_time:
                current_dump[table] = dt.now() + td(minutes=interval)
                continue
            
            if dump_time < dt.now():
                if table not in tasks:
                    app.add_task(
                        dump_data(
                            db, 
                            table, 
                            cfg['server'].get('data_folder'), 
                            count=int(dump.get('record_count', 1000))
                        ), 
                        name=table
                    )
                    current_dump[table] = dt.now() + td(minutes=interval)

        app.purge_tasks()
        await asyncio.sleep(60)


def run(cfg_file='config.ini'):
    cfg.read(cfg_file)
    svr = cfg['server'] if 'server' in cfg.sections() else {}
    host = svr.get('listen', '127.0.0.1')
    port = svr.getint('port', 8000)
    tag = svr.get('tag', 'default')
    access_log = svr.getboolean('access_log', False)
    auto_reload = svr.getboolean('auto_reload', False)
    debug = svr.getboolean('debug', False)

    logger.info(f'Auth enabled={"auth" in cfg.sections()}')

    setproctitle(f'{app_name} [{tag}] {host}:{port}')
    app.run(host=host, port=port, access_log=access_log,
            auto_reload=auto_reload, debug=debug, workers=1)


if __name__ == '__main__':
    run()
