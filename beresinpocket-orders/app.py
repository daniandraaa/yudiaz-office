"""Isolated BeresInPocket quotation capture; no payment or activation authority."""
import hashlib
import os
from pathlib import Path
import secrets
import sqlite3
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, model_validator

ROOT = Path(__file__).resolve().parent
FRONTEND = ROOT.parent / 'frontend' / 'beresinpocket'


class OrderInput(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    telegram: str = Field(pattern=r'^@[A-Za-z0-9_]{5,32}$')
    plan: Literal['personal', 'organization']
    users: int = Field(ge=1, le=5, strict=True)
    needs: str = Field(default='', max_length=2000)
    consent: Literal[True]


def create_app(db_path):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('''CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY, token_hash TEXT NOT NULL, name TEXT NOT NULL,
            telegram TEXT NOT NULL, plan TEXT NOT NULL, users INTEGER NOT NULL,
            needs TEXT NOT NULL, created_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'awaiting_quote',
            payment_status TEXT NOT NULL DEFAULT 'pending', amount_idr INTEGER,
            CHECK (payment_status = 'pending'), CHECK (amount_idr IS NULL))''')
    os.chmod(db_path, 0o600)
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware('http')
    async def privacy_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        return response

    @app.get('/beresinpocket/api/health')
    def health():
        with sqlite3.connect(db_path) as conn:
            conn.execute('SELECT 1 FROM orders LIMIT 1')
        return {'status': 'ok', 'mode': 'admin_confirmed_quote', 'payment_enabled': False}

    def public(row):
        return {'id': row['id'], 'plan': row['plan'], 'users': row['users'],
                'created_at': row['created_at'], 'status': row['status'],
                'amount_idr': row['amount_idr'], 'currency': 'IDR',
                'payment_status': row['payment_status'],
                'payment_instructions_status': 'awaiting_admin_instructions',
                'activated': False}

    @app.post('/beresinpocket/api/orders', status_code=201)
    def create_order(data: OrderInput):
        order_id, token = secrets.token_hex(16), secrets.token_urlsafe(32)
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute('INSERT INTO orders (id,token_hash,name,telegram,plan,users,needs,created_at) VALUES (?,?,?,?,?,?,?,?)',
                         (order_id, hashlib.sha256(token.encode()).hexdigest(), data.name,
                          data.telegram, data.plan, data.users, data.needs,
                          datetime.now(timezone.utc).isoformat()))
            row = conn.execute('SELECT * FROM orders WHERE id=?', (order_id,)).fetchone()
        return {**public(row), 'access_token': token}

    @app.get('/beresinpocket/api/orders/{order_id}')
    def get_order(order_id: str, request: Request):
        authorization = request.headers.get('Authorization', '')
        token = authorization.removeprefix('Bearer ') if authorization.startswith('Bearer ') else ''
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute('SELECT * FROM orders WHERE id=?', (order_id,)).fetchone()
        if not row or not secrets.compare_digest(row['token_hash'], hashlib.sha256(token.encode()).hexdigest()):
            raise HTTPException(404, 'Pesanan tidak ditemukan atau akses tidak sesuai.')
        return public(row)

    app.mount('/beresinpocket', StaticFiles(directory=FRONTEND, html=True), name='landing')
    return app


app = create_app(os.environ.get('BIP_DB_PATH', str(Path.home() / '.local/share/beresinpocket/orders.sqlite3')))
