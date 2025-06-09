import db
import requests
import easyocr
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
import asyncio
from config import cfg
from connect_db import s3, get_connection
from aiomultiprocess import Process
from multiprocessing import  Queue
from PIL import Image
from io import BytesIO
import cv2 as cv
import numpy as np
import json

async def consume():
    consumer = AIOKafkaConsumer(
        "yolo-ocr",
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        
        # Consume messages
        async for msg in consumer:
            message=msg.value
            print(f"got message request_id {message['id']} frame_id {message['frame_id']}")
            await start_process(message['id'], message['frame_id'])
            
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
        message = {
            "id": id,
            "frame_id": frame_id,
            "state": state
        }
        # Produce message
        await producer.send_and_wait('ocr', message)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


processes={}
queues={}
async def start_process(id, frame_id):
    try:
        process=processes.get(int(id))
        if process and process.is_alive():
            queues[int(id)].put((frame_id))
        elif not process:
            queue = Queue()
            queue.put((frame_id))
            queues[int(id)]=queue
            print('starting process: ', id)
            process=Process(target=get_strings, args=(id,queues[int(id)],),)
            processes[int(id)]=process
            process.start()
    except:
        await produce(id, frame_id, 'failed')


async def get_strings(id, queue):
    count=0
    try:
        while True:
            #извлечение ID кадра из очереди
            frame_id=queue.get()
            #отправка сообщения о начале работы в оркестратор
            await produce(id, frame_id.split('.')[0], 'started')
            #извлечение кадра из хранилища
            frame = s3.presigned_get_object(bucket_name=f"{id}-pages", object_name=frame_id+".tif") 
            #извлечение координат из хранилища
            boxes = s3.presigned_get_object(bucket_name=f"{id}-pages", object_name=frame_id+".box") 
            frame = requests.get(frame)
            boxes = requests.get(boxes)
            img = Image.open(BytesIO(frame.content))
            img_cv = np.array(img) 
            boxes_content = boxes.content.decode('utf-8').split('\n')
            cropped_images = []
            classes = []
            #нарезка фото на фрагменты
            for box in boxes_content:
                if len(box.split(' '))==5:
                    x1, y1, x2, y2, cls = map(int, box.split(' '))
                    cropped = img_cv[y1:y2, x1:x2]
                    cropped_images.append(cropped)
                    classes.append(cls)
            #получение предсказания
            text, count = await predict_text(cropped_images, id, count)
            #сборка результата в зависимости от класса фрагемента
            txt_content = ''.join(
                text[i] + ('\n' if classes[i] == 0 else ' ')
                for i in range(min(len(classes), len(text)))
            )
            txt_filename = frame_id+'.txt'
            #помещение файла в хранилище
            s3.put_object(
                str(id)+"-pages",
                txt_filename,
                BytesIO(txt_content.encode('utf-8')),  # Конвертируем текст в bytes
                length=len(txt_content.encode('utf-8'))
            )
            #отправка сообщения о завершении работы в оркестратор
            await produce(id, frame_id.split('.')[0], 'finished')       
    except:
     print(f'failed id: {id}, frame_id: {frame_id}')
     #отправка сообщения об ошибке в оркестратор
     await produce(id, frame_id, 'failed')
reader = easyocr.Reader(['ru'],
            model_storage_directory='custom_EasyOCR/model',
            user_network_directory='custom_EasyOCR/user_network',
            recog_network='best_norm_ED'
        ) 
async def predict_text(cropped_images, id, count):
    text_predictions=[]
    for img in cropped_images:
        result = reader.readtext(img, allowlist="абвгдежѕзийїклмноѺѿѡпрстуфхцчшщъыьѣюꙗѧіѽѫѯѱѳѵѷэ҃̾ⷠ҇ⷡ҇ⷢ҇ⷣⷩ҇ⷪ҇ⷬ҇ⷭ҇ ")
        combined_text = ' '.join(map(lambda x: x[1], result[:]))
        text_predictions.append(combined_text)
        count+=1
        await update_percentages(id, count)
    return text_predictions, count

async def update_percentages(id, count):
    try:
        db_conn=await get_connection()
        await db.update_process(db_conn, int(id), count)
    except:
        print("can't put the amount into db")



if __name__=='__main__':
    asyncio.run(consume())