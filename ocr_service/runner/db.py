from asyncpg.pool import PoolConnectionProxy

# взаимодействие с бд
async def insert_total_frames(db: PoolConnectionProxy, id:int, total:int):
    """Добавление новой строки"""
    query = f"insert into runner_progress(fk_request, frames_total) VALUES({id},{total});"
    await db.fetchval(query)

async def get_param(db: PoolConnectionProxy, id:int):
    query = f'select zip_type from requests where id={id}'
    result = await db.fetch(query)
    return result
