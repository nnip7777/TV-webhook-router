#!/usr/bin/env python3
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VERSION_PATH = PROJECT_ROOT / 'VERSION'
BUILD_PATH = PROJECT_ROOT / 'BUILD.json'

INCLUDE_FILES = [
    'VERSION',
    'CHANGELOG.md',
    'README.md',
    '.env.example',
    'requirements.txt',
    'webhook-router.service.example',
    'app/server.py',
    'app/settings.py',
    'app/analytics.py',
    'app/execution.py',
    'app/bybit_adapter.py',
    'app/bingx_adapter.py',
    'app/finam_adapter.py',
    'app/schwab_adapter.py',
    'external/smart_order_executor.py',
    'scripts/bingx_split_tunnel_macos.sh',
    'scripts/bingx_split_tunnel_linux.sh',
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    version = VERSION_PATH.read_text().strip() if VERSION_PATH.exists() else '0'
    files = {}
    for rel in INCLUDE_FILES:
        path = PROJECT_ROOT / rel
        if path.exists() and path.is_file():
            files[rel] = {
                'sha256': sha256_file(path),
                'bytes': path.stat().st_size,
            }
    build = {
        'version': version,
        'builtAt': datetime.now(timezone.utc).isoformat(),
        'fileCount': len(files),
        'files': files,
    }
    BUILD_PATH.write_text(json.dumps(build, ensure_ascii=False, indent=2) + '\n')
    print(BUILD_PATH)


if __name__ == '__main__':
    main()
