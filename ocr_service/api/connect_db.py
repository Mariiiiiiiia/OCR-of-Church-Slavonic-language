import asyncpg as pg
from typing import AsyncGenerator
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
                pool = await pg.create_pool(dsn=self.dsn)
                if pool is not None:
                    break
        if pool is None:
            raise Exception(f"can't connect to db in {self.retry} retries")
        self.pool = pool

    async def disconnect(self):
        await self.pool.close()
  


db_instance = Database(cfg)

async def get_connection() -> AsyncGenerator[PoolConnectionProxy, None]:
    async with db_instance.pool.acquire() as connection:
        yield connection

async def stop_connection(connection):
        await connection.close()



s3 = Minio(
    cfg.s3_host,
    access_key=cfg.access_key,
    secret_key=cfg.secret_key,
    secure=False,
)
if not s3.bucket_exists("zip"):
    s3.make_bucket("zip", "eu-west-1", True)