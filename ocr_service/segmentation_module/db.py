async def write_amount(db, id, segment_amount):
    query = f'select frames_segmented, segments_amount from yolo_progress where fk_request={id}'
    response=await db.fetchrow(query)
    if response==None:
        query = f"insert into yolo_progress(fk_request, frames_segmented, segments_amount) values({id}, 1, {segment_amount});"
    else:
        frames, segments = response['frames_segmented'], response['segments_amount']
        query = f"UPDATE yolo_progress set frames_segmented={int(frames)+1}, segments_amount={segments+segment_amount}  where fk_request={id};"
    await db.fetchval(query)

