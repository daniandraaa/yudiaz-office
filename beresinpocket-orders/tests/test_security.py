import importlib.util
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

@pytest.fixture
def client(tmp_path):
    spec = importlib.util.spec_from_file_location('bip_security', Path(__file__).parents[1] / 'app.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return TestClient(module.create_app(tmp_path / 'orders.sqlite3'))

BASE = dict(name='Uji Sintetis', telegram='@uji_sintetis', plan='personal', users=1, consent=True)

@pytest.mark.parametrize('extra', [{'amount_idr': 1}, {'status': 'paid'}, {'payment_status': 'paid'}, {'activated': True}, {'access_token':'chosen'}, {'users':2}, {'consent':False}, {'name':'   '}])
def test_reject_tampering(client, extra):
    assert client.post('/beresinpocket/api/orders', json={**BASE, **extra}).status_code == 422

def test_access_isolation(client):
    a = client.post('/beresinpocket/api/orders', json=BASE).json()
    b = client.post('/beresinpocket/api/orders', json=BASE).json()
    path = '/beresinpocket/api/orders/' + a['id']
    assert client.get(path).status_code == 404
    assert client.get(path, headers={'Authorization':'Bearer '+b['access_token']}).status_code == 404
    assert client.get(path+'?access_token='+a['access_token']).status_code == 404
    own = client.get(path, headers={'Authorization':'Bearer '+a['access_token']})
    assert own.status_code == 200
    assert not {'name','telegram','needs','token_hash','access_token'} & own.json().keys()
    assert own.headers['cache-control'] == 'no-store'
    assert client.get('/beresinpocket/api/orders').status_code in (404,405)
    assert client.patch(path, json={'payment_status':'paid'}).status_code == 405
    assert client.get('/beresinpocket/../../beresinpocket-orders/app.py').status_code == 404

def test_invalid_organization_size(client):
    assert client.post('/beresinpocket/api/orders', json={**BASE,'plan':'organization','users':6}).status_code == 422

def test_no_cross_origin_order_creation(client):
    assert client.post('/beresinpocket/api/orders', json=BASE, headers={'Origin':'https://evil.example'}).status_code == 403
