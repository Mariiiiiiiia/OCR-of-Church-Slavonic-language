import db
import numpy as np
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
import asyncio
import zipfile
from io import BytesIO
from config import cfg
from connect_db import s3
import os
from connect_db import get_connection
from aiomultiprocess import Process
from asyncpg.pool import PoolConnectionProxy
import cv2
import requests
from prepare_pages import skew_correction_and_split
import json



#получение сообщений из орхестратора  
async def consume():
    consumer = AIOKafkaConsumer(
        "runner",
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value
            await start_process(message['id'])
            
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

processes={}
async def start_process(id):
    await produce(id, 'started')
    print(f'starting process for request_id {id}')
    process=Process(target=get_zip, args=(id,),)
    processes[id]=process
    process.start()


async def get_zip(id):
        try:
            print(f'getting zip for request_id {id}')
            zip=s3.presigned_get_object(bucket_name='zip', object_name=f'{id}.zip') 
            response = requests.get(zip)
            zip_data = BytesIO(response.content)
            images_dict = {}
            with zipfile.ZipFile(zip_data, 'r') as zip_ref:
                # Собираем все файлы с подходящими расширениями
                image_files = [f for f in zip_ref.namelist() 
                    if not f.endswith('/')  # не папки
                    and f.lower().endswith(('.jpg', '.jpeg', '.png', 'tif'))]
                for img_path in image_files:
                    img_name = os.path.basename(img_path)
                    with zip_ref.open(img_path) as file:
                        img_data = file.read()
                    np_img = np.frombuffer(img_data, dtype=np.uint8)
                    img = cv2.imdecode(np_img, cv2.IMREAD_COLOR)
                    images_dict[img_name]=img
            db_conn: PoolConnectionProxy = await get_connection()
            param = await db.get_param(db_conn, id)
            print(f'starting preproccesing images for request_id {id}')
            await skew_correction_and_split(id, images_dict, param[0][0], db_conn)
            print(f'finished preproccesing images for request_id {id}')
            await produce(id, 'finished')
        except:
           await produce(id, 'failed')




#отправка в орхестратор
async def produce(id, state):
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
        }
        # Produce message
        await producer.send_and_wait('run', message)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()
        

if __name__=='__main__':
    asyncio.run(consume())