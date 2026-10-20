from test_security import client, BASE

def test_qris_disabled_until_merchant_configured(client):
    result = client.post('/beresinpocket/api/orders', json=BASE).json()
    assert result['payment_method'] == 'qris_manual'
    assert result['payment_destination_enabled'] is False
    assert result['payment_status'] == 'pending'
    headers = {'Authorization': 'Bearer ' + result['access_token']}
    endpoint = '/beresinpocket/api/orders/' + result['id'] + '/submit-payment'
    assert client.post(endpoint, headers=headers, json={}).status_code == 409
    assert client.post(endpoint, json={}).status_code == 404
    assert client.get('/beresinpocket/api/orders/'+result['id'], headers=headers).json()['activated'] is False
