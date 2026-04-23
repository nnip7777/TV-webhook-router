# Patch workflow

Each versioned patch lives under:

- `patch/<version>/files/...`
- `patch/<version>/patch-manifest.json`

Recommended flow:

1. Update code/files
2. Update `VERSION`
3. Regenerate build manifest:
   `./scripts/generate_build_manifest.py`
4. Create patch package:
   `./scripts/create_patch.py`
5. Transfer `patch/<version>/` to server
6. Apply on server:
   `python3 scripts/apply_patch.py /opt/webhook-router/patch/<version> /opt/webhook-router`

Apply creates backups under:

- `/opt/webhook-router/patch/_applied_backups/<version>-<timestamp>/`

Rollback example:

- `python3 scripts/rollback_patch.py /opt/webhook-router/patch/_applied_backups/<backup-dir> /opt/webhook-router`
