import os
import aiofiles
from sanic.log import logger
from aiocsv import AsyncDictReader, AsyncDictWriter


def insert_dict(db, table, data):
    try:
        cur = db.cursor()
        if not isinstance(data, (list, dict)):
            return 0

        if isinstance(data, list) and len(data) < 1:
            return 0

        sql = f'INSERT OR REPLACE INTO {table} '

        row = None
        if isinstance(data, list):
            if not isinstance(data[0], dict):
                return 0
            row = data[0]
        else:
            row = data

        sql += '(' + ','.join(row.keys()) + ') '
        sql += 'VALUES (' + ','.join([f':{k}' for k in row.keys()]) + ')'
        return cur.executemany(sql, data if isinstance(data, list) else [data]).rowcount
    except Exception as e:
        logger.error(f'sql={sql} err={str(e)}')
    finally:
        cur.close()

    return 0


def get_tables(db):
    res = list()
    try:
        sql = 'SELECT name FROM sqlite_schema WHERE type ="table" AND name NOT LIKE "sqlite_%";'
        tables = db.execute(sql).fetchall()
        for table in tables:
            res.append(table['name'])
    except Exception as e:
        logger.error(str(e))

    return res


async def load_data(db, table, filename):
    try:
        count = 0
        logger.info(f'Loading data from {filename}')
        async with aiofiles.open(filename, mode='r') as f:
            async for row in AsyncDictReader(f):
                count += insert_dict(db, table, row)
        logger.info(f'Loading of {filename} completed with {count} records')
    except Exception as e:
        logger.error(str(e))


async def dump_data(db, table, dump_dir, count=1000):
    try:
        num_dumped = 0
        logger.info(f'Dumping data from {table}.')
        partial = f'{dump_dir}/{table}.partial'
        final = f'{dump_dir}/{table}.csv'

        sql = f'SELECT * FROM {table} LIMIT 1'
        header = db.execute(sql).fetchone()
        if not header:
            logger.info(f'Table {table} is empty - not dumping')
            return

        async with aiofiles.open(partial, mode="w", encoding="utf-8") as f:
            writer = AsyncDictWriter(f, header.keys())
            await writer.writeheader()

            offset = 0
            while True:
                buffer = list()
                sql = f'SELECT * FROM {table} LIMIT {offset},{count}'
                records = db.execute(sql).fetchall()
                if len(records) < 1:
                    break

                for record in records:
                    buffer.append(dict(record))

                await writer.writerows(buffer)

                num_dumped += len(buffer)
                offset += count

        os.rename(partial, final)
        logger.info(f'Completed dump of {table} / records={num_dumped}')
    except Exception as e:
        logger.error(str(e))
