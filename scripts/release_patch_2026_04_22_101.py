#!/usr/bin/env python3
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

root = Path(__file__).resolve().parent.parent
version = '2026.04.22-101'
include = [
    'VERSION','BUILD.json','CHANGELOG.md','README.md','.env.example','requirements.txt','webhook-router.service.example',
    'app/server.py','app/settings.py','app/execution.py','app/bybit_adapter.py','app/bingx_adapter.py','app/finam_adapter.py','app/schwab_adapter.py','external/smart_order_executor.py','scripts/generate_build_manifest.py','scripts/bingx_split_tunnel_macos.sh','scripts/bingx_split_tunnel_linux.sh'
]
(root / 'VERSION').write_text(version + '\n')
files = {}
for rel in include:
    p = root / rel
    if not p.exists() or not p.is_file():
        continue
    h = hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    files[rel] = {'sha256': h.hexdigest(), 'bytes': p.stat().st_size}
(root / 'BUILD.json').write_text(json.dumps({
    'version': version,
    'builtAt': datetime.now(timezone.utc).isoformat(),
    'fileCount': len(files),
    'files': files,
}, ensure_ascii=False, indent=2) + '\n')
patch_dir = root / 'patch' / version
if patch_dir.exists():
    shutil.rmtree(patch_dir)
files_dir = patch_dir / 'files'
files_dir.mkdir(parents=True, exist_ok=True)
files = {}
for rel in include:
    src = root / rel
    if not src.exists() or not src.is_file():
        continue
    dst = files_dir / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    h = hashlib.sha256()
    with src.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    files[rel] = {'sha256': h.hexdigest(), 'bytes': src.stat().st_size}
(patch_dir / 'patch-manifest.json').write_text(json.dumps({
    'version': version,
    'createdAt': datetime.now(timezone.utc).isoformat(),
    'projectRoot': str(root),
    'fileCount': len(files),
    'files': files,
}, ensure_ascii=False, indent=2) + '\n')
print(patch_dir)
print('has routing.json:', 'config/routing.json' in files)
print('build version:', json.loads((files_dir / 'BUILD.json').read_text())['version'])
