import pytest
from datetime import datetime
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


@pytest.mark.parametrize('test_value', ['banana', 'apple', 'pear'])
def test_post_order(test_value, mocker):
    storage = mocker.patch('app.routers.ingest.storage')
    time_now = datetime.utcnow()
    response = client.post(
        '/ingest',
        params={
            'crawl_time': time_now.isoformat(),
            'pipeline_name': 'pipeline name',
            'ingest_key': 'key',
            'ingest_value': test_value
        }
    )
    assert storage.ingest.call_count == 1
    assert response.status_code == 200
    assert response.json() == {'ingest_key': 'key', 'ingest_value': test_value}
