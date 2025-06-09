import asyncpg as pg
from asyncpg.pool import PoolConnectionProxy
from config import Config, cfg
from minio import Minio


class Database:

    def __init__(self, cfg: Config, retry: int = 1):
        self.dsn = cfg.build_postgres_dsn
        self.retry = retry

    async def connect(self):

        pool = await pg.create_pool(dsn=self.dsn)
        if pool is None:
            for _ in range(self.retry):
                pool = await pg.create_pool(dsn=self.dsn, min_size=1)
                if pool is not None:
                    break
        if pool is None:
            raise Exception(f"can't connect to db in {self.retry} retries")
        print("Database pool connectionn opened")
        self.pool = pool

    async def disconnect(self):
        await self.pool.close()
  


db_instance = Database(cfg)

async def get_connection() -> PoolConnectionProxy:
    await db_instance.connect()
    connection=await db_instance.pool.acquire()
    return connection


s3 = Minio(
    cfg.s3_host,
    access_key=cfg.access_key,
    secret_key=cfg.secret_key,
    secure=False,
)

async def stop_connection(connection):
        await connection.close()
