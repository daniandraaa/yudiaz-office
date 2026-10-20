import importlib.util
from pathlib import Path


def test_capture_quote_in_sqlite(tmp_path):
    source = Path(__file__).parents[1] / 'app.py'
    assert source.exists(), 'Order backend has not been implemented'
    spec = importlib.util.spec_from_file_location('bip_orders', source)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    from fastapi.testclient import TestClient
    db = tmp_path / 'orders.sqlite3'
    client = TestClient(module.create_app(db))
    response = client.post('/beresinpocket/api/orders', json={
        'name': 'Uji Sintetis', 'telegram': '@uji_sintetis',
        'plan': 'personal', 'users': 1, 'needs': 'Draf proposal', 'consent': True})
    assert response.status_code == 201
    order = response.json()
    assert order['amount_idr'] is None
    assert order['status'] == 'awaiting_quote'
    assert order['payment_status'] == 'pending'
    assert order['payment_instructions_status'] == 'awaiting_admin_instructions'
    assert order['activated'] is False
    import sqlite3
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT count(*) FROM orders').fetchone()[0] == 1
    restarted = TestClient(module.create_app(db))
    status = restarted.get('/beresinpocket/api/orders/' + order['id'], headers={
        'Authorization': 'Bearer ' + order['access_token']})
    assert status.status_code == 200
    assert status.json()['payment_status'] == 'pending'
