#!/usr/bin/env python3
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
version = '2026.04.21-90'
patch_dir = PROJECT_ROOT / 'patch' / version
files_dir = patch_dir / 'files'
if patch_dir.exists():
    raise SystemExit(f'patch already exists: {version}')
files_dir.mkdir(parents=True, exist_ok=True)
include_files = [
    'VERSION',
    'BUILD.json',
    'CHANGELOG.md',
    'README.md',
    '.env.example',
    'requirements.txt',
    'webhook-router.service.example',
    'config/routing.json',
    'app/server.py',
    'app/settings.py',
    'app/execution.py',
    'app/bybit_adapter.py',
    'app/bingx_adapter.py',
    'app/finam_adapter.py',
    'app/schwab_adapter.py',
    'external/smart_order_executor.py',
    'scripts/generate_build_manifest.py',
    'scripts/bingx_split_tunnel_macos.sh',
    'scripts/bingx_split_tunnel_linux.sh',
]

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()

copied = {}
for rel in include_files:
    src = PROJECT_ROOT / rel
    if not src.exists() or not src.is_file():
        continue
    dst = files_dir / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    copied[rel] = {'sha256': sha256_file(src), 'bytes': src.stat().st_size}
manifest = {
    'version': version,
    'createdAt': datetime.now(timezone.utc).isoformat(),
    'projectRoot': str(PROJECT_ROOT),
    'fileCount': len(copied),
    'files': copied,
}
(patch_dir / 'patch-manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
print(patch_dir)
