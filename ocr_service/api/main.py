from fastapi import FastAPI, File, UploadFile
import uvicorn
from contextlib import asynccontextmanager
from connect_db import db_instance
from fastapi import Depends
import db
import time
from config import cfg
from connect_db import s3
from connect_db import get_connection
from fastapi import Form
from aiokafka import AIOKafkaProducer
from asyncpg.pool import PoolConnectionProxy
import json

@asynccontextmanager
async def lifespan(app: FastAPI):
    db = db_instance
    await db.connect()
    app.state.db = db
    yield
    await db.disconnect()

app = FastAPI(lifespan=lifespan)

@app.post("/upload")
async def upload_video(zip: UploadFile = File(...),
    param: int = Form(...),
    db_conn: PoolConnectionProxy = Depends(get_connection)):
        try:
            new_id: int = await db.insert_request(db_conn, param)
            print(f'create request with id: {new_id}')
            if not s3.bucket_exists(str(new_id)+"-pages"):
                s3.make_bucket(str(new_id)+"-pages", "eu-west-1", True)
            s3.put_object(
                "zip",
                f"{new_id}.zip",
                zip.file,
                length=zip.size,
            )
            print(f'put zip into s3 for request_id {new_id}')
            await message_to_orc(new_id, 'finished')
            return {"id": str(new_id)}
        except Exception as e:
            await message_to_orc(new_id, 'failed')
            return {"detail": str(e)}
        
async def file_exists(bucket_name: str, object_name: str):
    try:
        s3.stat_object(bucket_name, object_name)
        return True
    except Exception as e:
        if e.code == "NoSuchKey":
            return False
        raise

@app.get("/status/{request_id}")
async def get_result(request_id: int,
    db_conn: PoolConnectionProxy = Depends(get_connection)):
        #try:
            valid = await db.check_id(db_conn, request_id)
            if valid:
                progress = await db.get_progress(db_conn, request_id)
                check = await file_exists("zip", str(request_id)+"_results.zip")
                if not check:
                     await message_to_results(request_id)        
                elif (check and progress<100):
                     check = False
                     s3.remove_object("zip", str(request_id)+"_results.zip")
                     await message_to_results(request_id) 
                while not check:
                     check= await file_exists("zip", str(request_id)+"_results.zip")
                     time.sleep(1)
                zip_result = s3.presigned_get_object(bucket_name="zip", object_name=str(request_id)+"_results.zip") 
                return {
                        "progress": f"{progress}%",
                        "download_url": zip_result
                    }                
            else:
                 return{"message": "Не найден запрос с соответствующим ID"}
        #except Exception as e:
           #return {"detail": str(e)}
        
async def message_to_orc(id, state):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port),
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        message = {
            "id": id,
            "state": state
        } # Автоматическая сериализация в JSON
        await producer.send_and_wait("api", message)

    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

async def message_to_results(id):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port),
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        message = {
            "id": id,
        }
        await producer.send_and_wait("api-results", message)

    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


if __name__=='__main__':
    uvicorn.run(app, host='127.0.0.1', port=9999)