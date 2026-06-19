# webhook-router

Compact local/web deployable router for webhook signals → broker destinations.

## Current endpoints
- `GET /` — admin UI (protected by local users / RBAC)
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
- `GET /settings/backup/download?name=...` — download backup as `.tar.gz`
- `GET /users` — users / roles UI
- `POST /users/create` — create a local user
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
Bootstrap admin password is controlled by `WEBHOOK_ROUTER_ADMIN_PASSWORD`.
On first login, the app auto-creates local `config/users.json` with an `admin` user and RBAC roles: `admin`, `manager`, `editor`, `viewer`.
Backups are controlled by `WEBHOOK_ROUTER_BACKUP_DIR` and `WEBHOOK_ROUTER_BACKUP_KEEP_COUNT`.
Backups now include local users/roles and can be exported as `.tar.gz`.

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

### Bybit or BingX via separate VPN/AWG interface
If the VPS IP is geo-blocked by Bybit or BingX, you can keep a separate Linux interface (for example `bybit-egress` or `bingx-egress`) up permanently and route only that broker's traffic through it.

Recommended approach:
- configure the AWG interface with `Table = off` so it does not steal the server's default route
- keep the interface up as a separate tunnel
- set `BYBIT_BIND_INTERFACE=bybit-egress` for Bybit
- set `BINGX_BIND_INTERFACE=bingx-egress` for BingX

This lets webhook-router send only Bybit or BingX requests through that interface while Alor / Finam / Schwab continue using the normal server route.

### BingX split tunnel helpers
Two helper scripts are included:

- macOS: `scripts/bingx_split_tunnel_macos.sh`
- Linux/VPS: `scripts/bingx_split_tunnel_linux.sh`

Examples:

```bash
./scripts/bingx_split_tunnel_macos.sh apply
./scripts/bingx_split_tunnel_macos.sh show
```

```bash
./scripts/bingx_split_tunnel_linux.sh apply --env-file /opt/webhook-router/.env
./scripts/bingx_split_tunnel_linux.sh show
```

If auto-detection cannot find a normal non-VPN uplink, pass them explicitly:

```bash
./scripts/bingx_split_tunnel_linux.sh apply --dev eth0 --via 203.0.113.1 --env-file /opt/webhook-router/.env
./scripts/bingx_split_tunnel_linux.sh show --dev eth0 --via 203.0.113.1
```

Notes:
- macOS does not support Linux `SO_BINDTODEVICE`, so local BingX bypass on macOS should be done with host routes.
- Linux can use both host routes and `BINGX_BIND_INTERFACE` together.

Suggested process manager:
- systemd
- pm2
- supervisord

Example healthcheck:
```bash
curl http://127.0.0.1:8787/healthz
```
