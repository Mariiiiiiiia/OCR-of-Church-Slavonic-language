import db
from config import cfg
from connect_db import get_connection
from aiokafka import AIOKafkaConsumer
import asyncio
from aiokafka import AIOKafkaProducer
from asyncpg.pool import PoolConnectionProxy
import json
        
async def listen_api():
    consumer = AIOKafkaConsumer(
        "api",
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value
            db_conn: PoolConnectionProxy = await get_connection()
            print(f"API {message['state']} on request {message['id']}")
            await db.insert_state(db_conn, message['id'], 'API', message['state'])
            await produce_runner(message['id'])
    finally:
        await consumer.stop()

async def produce_runner(id):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port),
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )
    await producer.start()
    try:
        print(f'starting RUNNER for request_id {id}')
        message = {
            "id": id,
        }
        await producer.send_and_wait("runner", message)
    finally:
        await producer.stop()

async def listen_runner():
    consumer = AIOKafkaConsumer(
        'run',
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            message = msg.value
            db_conn: PoolConnectionProxy = await get_connection()
            print(f"RUNNER {message['state']} on request {message['id']}")
            await db.insert_state(db_conn, message['id'], 'RUNNER', message['state'])
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()
        
async def listen_yolo():
    consumer = AIOKafkaConsumer(
        'inference',
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            message = msg.value
            db_conn: PoolConnectionProxy = await get_connection()
            print(f"SEGMENTATION_MODULE {message['state']} frame {message['frame_id']} on request {message['id']}")
            await db.insert_state(db_conn, message['id'], 'SEGMENTATION_MODULE', message['state'], message['frame_id'])
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()   

async def listen_ocr():
    consumer = AIOKafkaConsumer(
        'ocr',
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            message = msg.value
            db_conn: PoolConnectionProxy = await get_connection()
            print(f"OCR_MODULE {message['state']} frame {message['frame_id']} on request {message['id']}")
            await db.insert_state(db_conn, message['id'], 'OCR_MODULE', message['state'], message['frame_id'])
            if message['state']=='finished':
                progress_result = await db.get_progress(db_conn, message['id'])
                print(progress_result)
                progress = progress_result.split('%')[0]
                if float(progress)==100:
                    await produce_results(message['id'])
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()  
async def produce_results(id):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port),
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )
    await producer.start()
    try:
        print(f'starting RESULTS_MODULE for request_id {id}')
        message = {
            "id": id,
        }
        await producer.send_and_wait("results", message)
    finally:
        await producer.stop()
async def listen_results():
    consumer = AIOKafkaConsumer(
        "return_results",
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            message = msg.value
            db_conn: PoolConnectionProxy = await get_connection()
            print(f"RESULT_MODULE {message['state']} on request {message['id']}")
            await db.insert_state(db_conn, message['id'], 'RESULTS_MODULE', message['state'])
            if message['state']=='finished':
                await db.insert_state(db_conn, message['id'], 'ORCHESTRATOR', "finished process")

    finally:
        await consumer.stop()
async def main():
    await asyncio.gather(
        listen_runner(),
        listen_api(),
        listen_yolo(),
        listen_ocr(),
        listen_results()
    )

asyncio.run(main())