# BeresInPocket — isolated quote capture

## Running artifact
User service `beresinpocket-orders.service`, enabled, loopback **127.0.0.1:9889**.
Landing `/beresinpocket/`; health `/beresinpocket/api/health`; POST `/beresinpocket/api/orders`; protected GET `/beresinpocket/api/orders/{id}` with Bearer access token. Token stored hashed in SQLite, never in URL; frontend uses sessionStorage. No public listing/admin/payment-verification API.
Database `/home/daniilham/.local/share/beresinpocket/orders.sqlite3`, WAL, owner-only permissions, root ext4 `/dev/sda1` (not ephemeral temp storage). Persistent local storage is not a remote backup; backup/retention policy still needs an operator decision.

## Pricing and payment safety
BRAND_APPROVED.md approves identity only, not pricing. No reliable approved price document found. Historic 39000/89000 values NOT published as approved prices. Capture starts `awaiting_quote`, `payment_status=pending`, no activation.
QRIS manual selected, but image and merchant verification missing: destination and submission disabled. POST `.../{id}/submit-payment` returns 409 for owner and 404 otherwise. There is NO fabricated QR, gateway, automatic bank matching, proof upload, paid transition, or activation.
Future enabled flow must be pending_payment -> submitted_for_verification -> manually_verified -> separate activation; proof alone must never mark paid. That configurable verification/activation workflow is NOT yet implemented, intentionally fails closed while merchant missing.

## Admin-approved quote allocation (local only)
After confirming a quote with customer, run from this directory:
`../.venv/bin/python amounts.py ORDER_ID APPROVED_BASE_IDR --confirm-approved-quote`
This does NOT enable payment. It allocates code 001..999 by BEGIN IMMEDIATE and a unique partial index across active total amounts. Exact total = base + code. TTL 24h, no active code reuse, exhaustion fails closed. Repeated request is idempotent; changed/expired quote needs manual review. Expired reservations are retained for audit. Late payments require manual review; automatic bank matching/reconciliation is not present. Returned status exposes base, code, total, expiration to the authenticated owner.

## Parent integration — Caddy NOT changed by this work
Existing office route remains whole-host proxy to 9449; public static landing is updated but form deliberately stays disabled until API proxy exists.
For sibling administering Caddy: add a scoped handler BEFORE existing whole-host office proxy:
```
handle /beresinpocket/* {
    reverse_proxy 127.0.0.1:9889
}
```
Do not use handle_path: the prefix must be retained.
For the new authorized hostname:
```
beresinpocket.daniandraaa.my.id {
    @root path /
    redir @root /beresinpocket/ 302
    reverse_proxy 127.0.0.1:9889
}
```
DNS A record must resolve to VPS first. Validate candidate and reload under the proxy owner's workflow. Verify exact public POST/GET isolation and HTTPS after routing. Root redirects rather than exposing office/CEO routes. Shared office backend/service not modified.

## Tests
`../.venv/bin/python -m pytest tests -q`
Tests cover real SQLite capture/restart, price/status/activation tampering, personal=1 and organization<=5, Bearer isolation, no query token, no listing/patch, cross-origin deny, disabled QRIS, concurrent unique allocations, 999-code exhaustion, expiration, idempotency. Existing dependency emits Starlette httpx deprecation warning.
Browser script/screenshots under Hermes scratch, not public. Tested create/status at 390 and 1440px without overflow, broken images, or page errors; public form fails closed before proxy integration.

## Remaining production gates
Approved prices or individually confirmed quotes; own QRIS image and merchant identity; authenticated administrator payment-verification workflow; safe private proof handling if desired; rate limiting/abuse protection, backup/retention and deletion process. User service may be restarted independently: `systemctl --user restart beresinpocket-orders`.
