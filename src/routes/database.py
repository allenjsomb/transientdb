from sanic.blueprints import Blueprint
from sanic.log import logger
from sanic.exceptions import SanicException
from sanic.response import json
from util.dbutil import insert_dict, get_tables

bp = Blueprint('database')


@bp.get('/vacuum')
async def vacuum(request):
    lvl = request.ctx.db.isolation_level
    try:
        request.ctx.db.isolation_level = None
        request.ctx.db.execute('VACUUM')
    except Exception as e:
        logger.error(f'{request.method}: {request.path} -> {str(e)}')
        return json(str(e))
    finally:
        request.ctx.db.isolation_level = lvl

    return json('OK')


@bp.get('/tables')
async def tables(request):
    return json(get_tables(request.ctx.db))


@bp.post('/execute')
async def create(request):
    try:
        sql = request.body.decode('utf-8')
        logger.debug(sql)
        request.ctx.db.execute(sql)
    except Exception as e:
        logger.error(f'{request.method}: {request.path} -> {str(e)} {sql}')
        return json(str(e))

    return json('OK')


@bp.post('/query')
async def query(request):
    rtn = list()
    try:
        sql = request.body.decode('utf-8')
        logger.debug(sql)
        results = request.ctx.db.execute(sql)

        for row in results.fetchall():
            rtn.append(dict(row))

    except Exception as e:
        logger.error(f'{request.method}: {request.path} -> {str(e)} {sql}')
        return json(str(e))

    return json(rtn)


@bp.post('/index/<dbname:str>/<name:str>/<field:str>')
async def create_index(request, dbname: str, name: str, field: str):
    try:
        cur = request.ctx.db.cursor()
        cur.execute(f'CREATE INDEX IF NOT EXISTS {name} ON {dbname} ({field})')
        logger.info(f'Created index {name} on {dbname}({field})')
    except Exception as e:
        logger.error(f'{request.method}: {request.path} -> {str(e)}')
    finally:
        cur.close()

    return json('OK')


@bp.post('/<dbname:str>')
async def database(request, dbname: str):
    count = 0
    try:
        data = request.json
        count = insert_dict(request.ctx.db, dbname, data)
    except Exception as e:
        logger.error(f'{request.method}: {request.path} -> {str(e)}')

    return json(count)


@bp.get('/count/<dbname:str>')
async def count(request, dbname: str):
    try:
        result = request.ctx.db.execute(
            f'SELECT COUNT(*) as records FROM {dbname}')
        row = result.fetchone()
        return json({k: row[k] for k in row.keys()})
    except Exception as e:
        logger.error(f'{request.method}: {request.path} -> {str(e)}')

    raise SanicException(dbname, status_code=404)


@bp.get('/<dbname:str>')
async def get_data(request, dbname: str):
    sql = f'SELECT * FROM {dbname} LIMIT {request.args.get("offset")}, {request.args.get("limit")}'
    logger.info(sql)
    if False and request.args:
        sql += ' WHERE '
        sql += ' AND '.join([f'{k}=:{k}' for k in request.args.keys()])

    args = {k: request.args.get(k) for k in request.args.keys()}
    try:
        results = request.ctx.db.execute(sql, args)

        rtn = list()
        for row in results.fetchall():
            rtn.append(dict(row))
        return json(rtn)
    except Exception as e:
        logger.error(f'{request.method}: {request.path} -> {str(e)}')

    raise SanicException(dbname, status_code=404)
