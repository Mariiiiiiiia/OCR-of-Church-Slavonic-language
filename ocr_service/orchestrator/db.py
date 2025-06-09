from asyncpg.pool import PoolConnectionProxy
# взаимодействие с бд
async def insert_state(db: PoolConnectionProxy, id:int,msg_module:str, status: str, frame: str = None):
    """Добавление новой строки"""
    query = f"INSERT INTO state_machine(fk_request, msg_module, state, frame) VALUES({id},'{msg_module}', '{status}', '{frame}')"
    await db.fetchval(query)

async def get_progress(db, id):
    query = f'select text_detection from ocr_progress where fk_request={id}'
    response=await db.fetch(query)
    return response[0]['text_detection']



