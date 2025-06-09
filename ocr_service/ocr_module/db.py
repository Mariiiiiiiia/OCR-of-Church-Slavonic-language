async def update_process(db, id, count):
    query = f'select segments_amount from yolo_progress where fk_request={id}'
    amount=await db.fetchval(query)
    query = f'select text_detection from ocr_progress where fk_request={id}'
    response=await db.fetchrow(query)
    if response==None:
        query = f"insert into ocr_progress(fk_request, text_detection) values({id}, '{str(round(count/amount*100, 3))}%');"
    else:
        query = f"UPDATE ocr_progress set text_detection='{str(round(count/amount*100, 3))}%' where fk_request={id};"
    await db.fetchval(query)


