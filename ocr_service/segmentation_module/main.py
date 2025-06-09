import db
import requests
import json
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
import asyncio
from config import cfg
from connect_db import s3, get_connection
from aiomultiprocess import Process
from multiprocessing import  Queue
from PIL import Image
from io import BytesIO
from ultralytics import YOLO

async def consume():
    consumer = AIOKafkaConsumer(
        "runner-yolo",
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        
        # Consume messages
        async for msg in consumer:
             message = msg.value
             await start_process(message['id'], message['frame_id'])
            #await start_process(msg.value.decode())
            ##
            
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()




async def produce(id, frame_id, state):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port),
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        message = {
            "id": id,
            "frame_id": frame_id,
            "state": state
        }
        await producer.send_and_wait('inference', message)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

async def produce_ocr(id, frame_id):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port),
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        message = {
            "id": id,
            "frame_id": frame_id
        }
        await producer.send_and_wait('yolo-ocr', message)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


processes={}
queues={}
async def start_process(id, frame_id):
    try:
        process=processes.get(id)
        if process and process.is_alive():
            queues[id].put((frame_id))
        elif not process:
            print(f'starting process for request_id {id}')
            queue = Queue()
            queue.put((frame_id))
            queues[int(id)]=queue
            process=Process(target=get_predict, args=(id,queues[int(id)],),)
            processes[int(id)]=process
            process.start()
    except:
        await produce(id, frame_id, 'failed')


async def get_predict(id, queue):
    try:
        while True:
            segments_amount=0
            #извлечение id кадра из очереди
            frame_id=queue.get()
            #отправка сообщения о начале работы в оркестратор
            await produce(id, frame_id, 'started')
            #извлечение объекта из хранилища
            frame = s3.presigned_get_object(bucket_name=f"{id}-pages", object_name=frame_id+".tif") 
            response = requests.get(frame)
            img = Image.open(BytesIO(response.content)) #конвертируем фото в bytes
            model = YOLO("best (5).pt")
            #предсказание моделью
            results = model.predict(source=img, save=False)
            parts=[]
            #обработка результатов
            for i in range (0, len(results[0].boxes)):
                parts.append([round(x) for x in results[0].boxes[i].xyxy[0].tolist()] +  [int(results[0].boxes[i].cls)])
            segments_amount+=len(results[0].boxes)
            box_filename = frame_id + ".box"
            box_content = '\n'.join(' '.join(map(str, row)) for row in parts) + '\n'
            #помещение файла результатов в хранилище
            s3.put_object(
                str(id)+"-pages",
                box_filename,
                BytesIO(box_content.encode('utf-8')),  #конвертируем текст в bytes
                length=len(box_content.encode('utf-8'))
            )
            #отправка сообщения о завершении работы в оркестратор
            await produce(id, frame_id, 'finished')
            print(f'finished segmentation {frame_id} for request_id {id}')
            #увеличение количества обработанных кадров в СУБД
            await collect_amount(id, segments_amount)
            #отправка в следующий модуль
            await produce_ocr(id, frame_id)         
    except:
        print(f'failed id: {id}, frame_id: {frame_id}')
        #отправка сообщения об ошибке в оркестратор
        await produce(id, frame_id, 'failed')

async def collect_amount(id, segment_amount):
    try:
        db_conn=await get_connection()
        await db.write_amount(db_conn, int(id), segment_amount)
    except:
        print("can't put the amount into db")


if __name__=='__main__':
    asyncio.run(consume())