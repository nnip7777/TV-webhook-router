# webhook-router

Compact local/web deployable router for webhook signals → broker destinations.

## Current endpoints
- `GET /` — admin UI (protected by admin password when configured)
- `GET /settings` — settings / secrets / backups UI
- `GET /journal` — weekly execution journal UI with filters (reads current + archived .gz)
- `GET /healthz` — healthcheck
- `GET /api/state` — config + observed + current mapping
- `GET /api/broker-metrics` — cached broker/account metrics
- `POST /admin/save-mappings` — save one row or all mappings
- `POST /admin/set-test-mode` — broker test-mode toggle (currently wired for Bybit)
- `POST /admin/quick-order` — manual quick order from UI
- `POST /settings/save` — save env/settings/secrets
- `POST /settings/test-broker` — run connectivity test for a single broker
- `POST /settings/backup/create` — create backup
- `POST /settings/backup/restore` — restore backup
- `POST /settings/backup/delete` — delete backup
- `POST /webhook` — inbound signal endpoint

## Run locally
```bash
cd /path/to/webhook-router
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# requirements include httpx[http2] because Finam auth/account calls use HTTP/2
cp .env.example .env
python3 app/server.py
```

## Environment
All paths and secrets are configured through `.env`.
Do not commit real secrets.
Admin password is controlled by `WEBHOOK_ROUTER_ADMIN_PASSWORD` and can also be changed from `/settings`.
Backups are controlled by `WEBHOOK_ROUTER_BACKUP_DIR` and `WEBHOOK_ROUTER_BACKUP_KEEP_COUNT`.

## Build / versioning
This project uses a simple build manifest approach for patch-based deploys:
- `VERSION` — human-readable build/version id
- `BUILD.json` — generated manifest with sha256 hashes and file sizes
- `CHANGELOG.md` — short human-readable "what's new"

Generate/update the manifest after changes:
```bash
./scripts/generate_build_manifest.py
```

Create a versioned patch package after changes:
```bash
./scripts/create_patch.py
```

Patch packages live under:
- `patch/<version>/files/...`
- `patch/<version>/patch-manifest.json`

Apply on server:
```bash
python3 scripts/apply_patch.py /opt/webhook-router/patch/<version> /opt/webhook-router
```

Rollback from backup:
```bash
python3 scripts/rollback_patch.py /opt/webhook-router/patch/_applied_backups/<backup-dir> /opt/webhook-router
```

The server surfaces build info in:
- login page
- admin page
- settings page
- `/healthz`
- journal entry `server-start`

Recommended layout:
- `config/` — routing + instruments + observed state
- `logs/` — runtime logs/jsonl
- `secrets/` — broker secret/token/config files
- `external/` — optional legacy helper scripts like `smart_order_executor.py`

## Deploy notes
For VPS deploy, do not expose admin endpoints publicly without protection.
At minimum put the app behind:
- Nginx/Caddy reverse proxy
- HTTPS
- auth layer or IP allowlist / VPN / Tailscale

### Bybit via separate VPN/AWG interface
If the VPS IP is geo-blocked by Bybit, you can keep a separate Linux interface (for example `bybit-egress`) up permanently and route only Bybit traffic through it.

Recommended approach:
- configure the AWG interface with `Table = off` so it does not steal the server's default route
- keep the interface up as a separate tunnel
- set `BYBIT_BIND_INTERFACE=bybit-egress`

This lets webhook-router send only Bybit requests through that interface while Alor / Finam / Schwab continue using the normal server route.

Suggested process manager:
- systemd
- pm2
- supervisord

Example healthcheck:
```bash
curl http://127.0.0.1:8787/healthz
```
