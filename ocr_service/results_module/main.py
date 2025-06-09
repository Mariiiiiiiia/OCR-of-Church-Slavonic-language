from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
import asyncio
import zipfile
from config import cfg
from connect_db import s3
import json
import io



#получение сообщений из орхестратора  
async def consume():
    consumer = AIOKafkaConsumer(
        "results",
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value
            print(f"got message for request_id {message['id']}") 
            await produce(message['id'], "started")
            await make_zip(message['id'])
            await produce(id, "finished")
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()
async def consume():
    consumer = AIOKafkaConsumer(
        "api-results",
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value
            print(f"got message for request_id {message['id']}") 
            await make_zip(message['id'])
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

async def make_zip(id):
    #try:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
                objects = s3.list_objects(f"{id}-pages", recursive=True)
                txt_files = [obj.object_name for obj in objects if obj.object_name.endswith('.txt')]
                for txt_file in txt_files:
                    file_data = s3.get_object(f"{id}-pages", txt_file)
                    # Добавляем в архив с сохранением имени файла
                    zip_file.writestr(txt_file, file_data.read())
                    file_data.close()
        print(f"got files for request_id {id}")
        zip_buffer.seek(0)
        zip_name = f"{id}_results.zip"
        s3.put_object(
            "zip",
            zip_name,
            zip_buffer,
            length=zip_buffer.getbuffer().nbytes,
            content_type='application/zip'
        )
        print(f"put file for request_id {id}")
    #except:
        #await produce(id, 'failed')




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
        await producer.send_and_wait('return_results', message)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()
        

if __name__=='__main__':
    asyncio.run(consume())