"""BeresInPocket Order & QRIS Payment Service."""
import hashlib
import os
from pathlib import Path
import secrets
import sqlite3
import time
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import FastAPI, APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, model_validator

ROOT = Path(__file__).resolve().parent
import sys
sys.path.insert(0, str(ROOT))
from amounts import initialize, lookup, reserve

FRONTEND = ROOT.parent / 'frontend' / 'beresinpocket'
ASSETS = FRONTEND / 'assets'


class OrderInput(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)

    @model_validator(mode='after')
    def personal_limit(self):
        if self.plan == 'personal' and self.users != 1:
            raise ValueError('Paket Personal dikunci tepat untuk 1 pengguna Telegram.')
        return self

    name: str = Field(min_length=1, max_length=100)
    telegram: str = Field(pattern=r'^@[A-Za-z0-9_]{4,32}$')
    plan: Literal['personal', 'organization']
    users: int = Field(ge=1, le=5, strict=True)
    needs: str = Field(default='', max_length=2000)
    consent: Literal[True]


class PaymentSubmitInput(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)
    sender_name: str = Field(default='', max_length=100)
    sender_bank: str = Field(default='', max_length=100)
    notes: str = Field(default='', max_length=500)


def create_app(db_path):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    
    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('''CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            token_hash TEXT NOT NULL,
            name TEXT NOT NULL,
            telegram TEXT NOT NULL,
            plan TEXT NOT NULL,
            users INTEGER NOT NULL,
            needs TEXT NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending_payment',
            payment_status TEXT NOT NULL DEFAULT 'pending',
            amount_idr INTEGER,
            unique_code INTEGER,
            total_idr INTEGER,
            payment_ref TEXT,
            submitted_at TEXT,
            verified_at TEXT
        )''')
    initialize(db_path)
    if db_path.exists():
        os.chmod(db_path, 0o600)

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware('http')
    async def privacy_headers(request: Request, call_next):
        origin = request.headers.get('origin')
        if request.method == 'POST' and origin and origin not in {
            'https://office.daniandraaa.my.id', 'https://beresinpocket.daniandraaa.my.id',
            'http://127.0.0.1:9889', 'http://testserver'
        }:
            return JSONResponse({'detail': 'Origin tidak diizinkan.'}, status_code=403)
        if request.method == 'POST':
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 32768:
                    return JSONResponse({'detail': 'Permintaan terlalu besar.'}, status_code=413)
            request._body = bytes(body)
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        return response

    def public(row):
        quote = lookup(db_path, row['id'])
        now = int(time.time())
        expired = bool(quote and quote['expires_at'] <= now)
        base = quote['base_idr'] if quote else (39000 if row['plan'] == 'personal' else 89000)
        ucode = quote['unique_code'] if quote else 0
        total = quote['total_idr'] if quote else (base + ucode)
        exp_at = quote['expires_at'] if quote else (now + 86400)
        
        return {
            'id': row['id'],
            'name': row['name'],
            'telegram': row['telegram'],
            'plan': row['plan'],
            'users': row['users'],
            'needs': row['needs'],
            'created_at': row['created_at'],
            'status': row['status'],
            'base_idr': base,
            'unique_code': format(ucode, '03d'),
            'total_idr': total,
            'currency': 'IDR',
            'expires_at': exp_at,
            'quote_expired': expired,
            'payment_status': row['payment_status'],
            'payment_method': 'QRIS',
            'merchant_name': 'BeresInPocket, Elektronik',
            'nmid': 'ID1026620533368',
            'qris_image_url': '/assets/qris_beresinpocket_card.png',
            'qris_full_url': '/assets/qris_beresinpocket_full.jpg',
            'payment_destination_enabled': True,
            'activated': (row['status'] == 'active'),
            'submitted_at': row['submitted_at'] if 'submitted_at' in row.keys() else None,
            'payment_ref': row['payment_ref'] if 'payment_ref' in row.keys() else None
        }

    # API Router
    router = APIRouter()

    @router.get('/health')
    def health():
        with sqlite3.connect(db_path) as conn:
            conn.execute('SELECT 1 FROM orders LIMIT 1')
        return {
            'status': 'ok',
            'mode': 'production_qris',
            'payment_enabled': True,
            'merchant': 'BeresInPocket, Elektronik',
            'nmid': 'ID1026620533368'
        }

    @router.post('/orders', status_code=201)
    def create_order(data: OrderInput):
        order_id = secrets.token_hex(16)
        token = secrets.token_urlsafe(32)
        base_idr = 39000 if data.plan == 'personal' else 89000
        
        # Reserve unique 3-digit code
        quote = reserve(db_path, order_id, base_idr)
        
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute('''INSERT INTO orders (
                id, token_hash, name, telegram, plan, users, needs, created_at,
                status, payment_status, amount_idr, unique_code, total_idr
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
                order_id,
                hashlib.sha256(token.encode()).hexdigest(),
                data.name,
                data.telegram,
                data.plan,
                data.users,
                data.needs,
                datetime.now(timezone.utc).isoformat(),
                'pending_payment',
                'pending',
                base_idr,
                quote['unique_code'],
                quote['total_idr']
            ))
            row = conn.execute('SELECT * FROM orders WHERE id=?', (order_id,)).fetchone()

        return {**public(row), 'access_token': token}

    @router.get('/orders/{order_id}')
    def get_order(order_id: str, request: Request):
        auth = request.headers.get('Authorization', '')
        token = auth.removeprefix('Bearer ') if auth.startswith('Bearer ') else ''
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute('SELECT * FROM orders WHERE id=?', (order_id,)).fetchone()
        
        if not row or not secrets.compare_digest(row['token_hash'], hashlib.sha256(token.encode()).hexdigest()):
            raise HTTPException(404, 'Pesanan tidak ditemukan atau otorisasi tidak valid.')
        
        return public(row)

    @router.post('/orders/{order_id}/submit-payment')
    def submit_payment(order_id: str, request: Request, payload: PaymentSubmitInput = PaymentSubmitInput()):
        auth = request.headers.get('Authorization', '')
        token = auth.removeprefix('Bearer ') if auth.startswith('Bearer ') else ''
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute('SELECT * FROM orders WHERE id=?', (order_id,)).fetchone()
            
            if not row or not secrets.compare_digest(row['token_hash'], hashlib.sha256(token.encode()).hexdigest()):
                raise HTTPException(404, 'Pesanan tidak ditemukan atau otorisasi tidak valid.')
            
            if row['payment_status'] == 'verified':
                return {'detail': 'Pembayaran sudah terverifikasi.', **public(row)}

            ref_parts = []
            if payload.sender_name:
                ref_parts.append(f"Pengirim: {payload.sender_name}")
            if payload.sender_bank:
                ref_parts.append(f"Bank/E-Wallet: {payload.sender_bank}")
            if payload.notes:
                ref_parts.append(f"Catatan: {payload.notes}")
            ref_str = " | ".join(ref_parts) if ref_parts else "Konfirmasi dari website via QRIS"
            now_iso = datetime.now(timezone.utc).isoformat()

            conn.execute('''UPDATE orders SET
                payment_status = 'submitted_for_verification',
                status = 'waiting_verification',
                payment_ref = ?,
                submitted_at = ?
                WHERE id = ?''', (ref_str, now_iso, order_id))
            
            row = conn.execute('SELECT * FROM orders WHERE id=?', (order_id,)).fetchone()

        return {
            'message': 'Konfirmasi pembayaran berhasil dikirim. Tim finance sedang mencocokkan mutasi nominal QRIS Anda.',
            **public(row)
        }

    # Mount API to both /api and /beresinpocket/api
    app.include_router(router, prefix='/api')
    app.include_router(router, prefix='/beresinpocket/api')

    # Mount static assets
    if ASSETS.exists():
        app.mount('/assets', StaticFiles(directory=ASSETS), name='assets')
    if FRONTEND.exists():
        app.mount('/beresinpocket', StaticFiles(directory=FRONTEND, html=True), name='landing')

    return app


app = create_app(os.environ.get('BIP_DB_PATH', str(Path.home() / '.local/share/beresinpocket/orders.sqlite3')))
