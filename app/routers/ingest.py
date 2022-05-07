import logging
import os
from datetime import datetime
from http import HTTPStatus
from typing import Dict

from fastapi import APIRouter

from app.routers.mysql_module import Storage

logger = logging.getLogger(__file__)
router = APIRouter(tags=['income'])


storage = Storage(os.getenv('STORAGE_HOST', 'localhost'))


@router.post('/ingest', status_code=HTTPStatus.OK)
async def order_call(crawl_time: datetime, pipeline_name: str, ingest_key: str, ingest_value: str) -> Dict[str, str]:
    logger.info(f'Incoming ingest: {crawl_time=}, {ingest_key=}, {ingest_value=}')

    ingest_time = datetime.utcnow()
    storage.ingest(crawl_time, ingest_time, pipeline_name, ingest_key, ingest_value)

    logger.info(f'Stored ingest({crawl_time=}, {ingest_time}, {pipeline_name=}, {ingest_key=}, {ingest_value})')

    return {'ingest_key': ingest_key, 'ingest_value': ingest_value}
