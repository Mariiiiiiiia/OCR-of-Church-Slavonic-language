from config import Config, cfg
import asyncpg



async def get_connection():
        return await asyncpg.connect(
            user=cfg.postgres_user,
            password=cfg.postgres_password,
            database=cfg.postgres_db,
            host=cfg.postgres_host,
            port=cfg.postgres_port
        )
async def stop_connection(connection):
        await connection.close()

