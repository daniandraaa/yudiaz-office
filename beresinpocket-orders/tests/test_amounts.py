import importlib.util
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

def load():
    path = Path(__file__).parents[1] / 'amounts.py'
    assert path.exists(), 'Atomic amount allocator not implemented'
    spec = importlib.util.spec_from_file_location('amounts', path)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m

def test_atomic_concurrent_unique_amounts(tmp_path):
    m = load(); db = tmp_path / 'amounts.sqlite3'
    m.initialize(db)
    with ThreadPoolExecutor(max_workers=12) as pool:
        amounts = list(pool.map(lambda i:m.reserve(db, str(i), 39000, now=1000), range(50)))
    assert len({r['total_idr'] for r in amounts}) == 50
    assert all(1 <= r['unique_code'] <= 999 and r['total_idr'] == 39000 + r['unique_code'] for r in amounts)
    assert all(r['expires_at'] == 87400 for r in amounts)

def test_expiration_and_exhaustion(tmp_path):
    m = load(); db = tmp_path / 'amounts.sqlite3'; m.initialize(db)
    for i in range(999): m.reserve(db, str(i), 89000, now=1000)
    import pytest
    with pytest.raises(ValueError): m.reserve(db, 'full', 89000, now=1001)
    r = m.reserve(db, 'expired-reuse', 89000, now=87401)
    assert r['total_idr'] > 89000
    with pytest.raises(ValueError): m.reserve(db, 'bad', -1)

def test_repeat_order_idempotent(tmp_path):
    m=load(); db=tmp_path/'a.sqlite3'; m.initialize(db)
    first=m.reserve(db,'same',39000,now=1000)
    assert m.reserve(db,'same',39000,now=1001)==first
    import pytest
    with pytest.raises(ValueError): m.reserve(db,'same',89000,now=1001)
