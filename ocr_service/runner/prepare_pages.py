import cv2
import numpy as np
from imutils import rotate
from connect_db import s3
import statistics
import json
from io import BytesIO
from aiokafka import AIOKafkaProducer
from config import cfg
from connect_db import get_connection
from asyncpg.pool import PoolConnectionProxy
import db

async def horizontal_projection(sobel_img):
    sum_cols = []
    rows, cols = sobel_img.shape[:2]
    for row in range(rows - 1):
        sum_cols.append(np.sum(sobel_img[row, :]))
    return sum_cols

async def rotate_image(img):
    gx = cv2.Sobel(img, cv2.CV_64F, 1, 0)
    gy = cv2.Sobel(img, cv2.CV_64F, 0, 1)
    sobel_img = np.sqrt(gx * gx + gy * gy)
    sobel_img_inv = 255 - sobel_img
    predicted_angle = 0
    highest_hp = 0
    for index, angle in enumerate(range(-5, 5)):
        hp = await horizontal_projection(rotate(sobel_img_inv, angle))
        median_hp = np.median(hp)
        if highest_hp < median_hp:
            predicted_angle = angle
            highest_hp = median_hp
    rows = img.shape[0]
    cols = img.shape[1]
    img_center = (cols / 2, rows / 2)
    matrix = cv2.getRotationMatrix2D(img_center, predicted_angle, 1)
    rotate_img = cv2.warpAffine(img, matrix, (cols, rows), borderMode=cv2.BORDER_CONSTANT,
                                borderValue=(255, 255, 255))
    return rotate_img, predicted_angle

async def find_center_line(img):
  _, binary = cv2.threshold(img, 128, 255, cv2.THRESH_BINARY)
  height, width = binary.shape
  center_x = width // 2
  search_width = 200  # Ширина области поиска
  max_black_count = 0
  best_column = []
  for x in range(center_x - search_width // 2, center_x + search_width // 2):
      black_count = np.sum(binary[:, x] == 0)  # Считаем количество черных пикселей в столбце
      if black_count > max_black_count:
          max_black_count = black_count
          best_column = [x]
      elif black_count==max_black_count:
        best_column.append(x)
  if len(best_column)!=0:
      x=statistics.mean(best_column)
      return x
  else:
      print('only one page on scan')
      return 0
      #cv2.line(img, (round(x), 0), (round(x), height), (0, 255, 0), 2)
  
async def split_double_page(img):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    split_index = await find_center_line(gray)
    img_with_line=img.copy()
    left_page = img[:, :split_index]
    right_page = img[:, split_index:]
    return left_page, right_page

async def image_preproccesing(img):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    alpha = 0.8
    beta = -50
    contrast = cv2.convertScaleAbs(gray, alpha=alpha, beta=beta)
    return contrast

async def put_into_s3(img_name, img, id):
    _, img_encoded = cv2.imencode('.tif', img)  # или '.jpg'
    img_bytes = img_encoded.tobytes()
    img_buffer = BytesIO(img_bytes)
    s3.put_object(
            str(id)+"-pages",
            img_name,
            img_buffer,
            length=len(img_bytes),
            content_type='image/png'  # или 'image/jpeg'
    )
    


async def skew_correction_and_split(id, images_dict, param, db_conn):
    count=0
    if param:
        for img_name, img in images_dict.items():
            left_page, right_page = await split_double_page(img)
            rotate_left_page, angle = await rotate_image(left_page)
            rotate_right_page, angle = await rotate_image(right_page)
            preproccesed_left_page = await image_preproccesing(rotate_left_page)
            preproccesed_right_page = await image_preproccesing(rotate_right_page)
            frame_id=img_name.split('.')[0]
            await put_into_s3(f"{frame_id}_left.tif", preproccesed_left_page, id)
            await put_into_s3(f"{frame_id}_right.tif", preproccesed_right_page, id)
            count+=2
            frame_message = {
            "id": id,
            "frame_id": f"{frame_id}_left"
            }
            await producer(frame_message)
            frame_message = {
            "id": id,
            "frame_id": f"{frame_id}_right"
            }
            await producer(frame_message)
    else: 
        for img_name, img in images_dict.items():
            rotate_page, angle = rotate_image(img)
            preproccesed_page = image_preproccesing(rotate_page)
            frame_id=img_name.split('.')[0]
            put_into_s3(f"{frame_id}.tif", preproccesed_page, id)
            count+=1
            frame_message = {
            "id": id,
            "frame_id": frame_id
            }
            await producer(frame_message)
    await db.insert_total_frames(db_conn, id, count)

def serializer(value):
    return json.dumps(value).encode(encoding="utf-8")

#отправка в модель yolo
async def producer(frame_message):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port),
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        print(f"sending {frame_message['frame_id']} frame to yolo for request_id {frame_message['id']}")
        await producer.send_and_wait("runner-yolo", frame_message)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()
