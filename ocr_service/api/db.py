from asyncpg.pool import PoolConnectionProxy
# взаимодействие с бд
async def insert_request(db: PoolConnectionProxy, param : int):
    """Добавление новой строки"""
    query = f"INSERT INTO requests(zip_type) VALUES({param}) RETURNING id;"
    result = await db.fetchval(query)
    return result

async def check_id(db: PoolConnectionProxy, id: int):
    query = f'select * from requests where id={id}'
    result = await db.fetchval(query) 
    return bool(result)

async def get_progress(db, id):
    query = f'select text_detection from ocr_progress where fk_request={id};'
    response=await db.fetchval(query)
    if not bool(response):
        progress=0
    else:
        progress=float(response.split('%')[0])
    return progress