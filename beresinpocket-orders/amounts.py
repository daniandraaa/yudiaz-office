"""Admin-only approved-quote allocation. No browser-provided price is accepted."""
import sqlite3
import time


def initialize(db):
    with sqlite3.connect(db, timeout=30) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS payment_reservations (
            order_id TEXT PRIMARY KEY, base_idr INTEGER NOT NULL CHECK(base_idr > 0),
            unique_code INTEGER NOT NULL CHECK(unique_code BETWEEN 1 AND 999),
            total_idr INTEGER NOT NULL CHECK(total_idr = base_idr + unique_code),
            expires_at INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1)''')
        conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS active_payment_amount ON payment_reservations(total_idr) WHERE active=1')


def reserve(db, order_id, base_idr, now=None):
    if type(base_idr) is not int or base_idr <= 0 or base_idr > 100000000:
        raise ValueError('Nominal dasar admin tidak valid')
    now = int(time.time() if now is None else now)
    with sqlite3.connect(db, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('UPDATE payment_reservations SET active=0 WHERE active=1 AND expires_at<=?', (now,))
        row = conn.execute('SELECT * FROM payment_reservations WHERE order_id=?', (order_id,)).fetchone()
        if row:
            if row['base_idr'] != base_idr or not row['active']:
                raise ValueError('Penawaran berubah/kedaluwarsa: perlu tinjauan admin, bukan alokasi ulang otomatis')
            return dict(row)
        occupied = {r[0] for r in conn.execute('SELECT total_idr FROM payment_reservations WHERE active=1')}
        code = next((code for code in range(1, 1000) if base_idr + code not in occupied), None)
        if code is None:
            raise ValueError('Semua kode unik aktif terpakai. Coba lagi setelah reservasi berakhir.')
        conn.execute('INSERT INTO payment_reservations VALUES (?,?,?,?,?,1)', (order_id, base_idr, code, base_idr + code, now + 86400))
        return dict(conn.execute('SELECT * FROM payment_reservations WHERE order_id=?', (order_id,)).fetchone())


def lookup(db, order_id):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute('SELECT * FROM payment_reservations WHERE order_id=?', (order_id,)).fetchone()
        return dict(row) if row else None


if __name__ == '__main__':
    import argparse
    import json
    from pathlib import Path
    parser = argparse.ArgumentParser(description='Admin lokal: tetapkan nominal penawaran yang SUDAH disetujui, bukan verifikasi pembayaran.')
    parser.add_argument('order_id')
    parser.add_argument('approved_base_idr', type=int)
    parser.add_argument('--confirm-approved-quote', required=True, action='store_true')
    parser.add_argument('--db', default=str(Path.home()/'.local/share/beresinpocket/orders.sqlite3'))
    args = parser.parse_args()
    with sqlite3.connect(args.db) as conn:
        if not conn.execute('SELECT 1 FROM orders WHERE id=?', (args.order_id,)).fetchone():
            parser.error('Pesanan tidak ditemukan')
    initialize(args.db)
    print(json.dumps(reserve(args.db, args.order_id, args.approved_base_idr)))
