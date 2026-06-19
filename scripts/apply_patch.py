#!/usr/bin/env python3
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    if len(sys.argv) < 2:
        print('usage: apply_patch.py <patch_dir> [target_root]')
        return 2

    patch_dir = Path(sys.argv[1]).resolve()
    target_root = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else Path('/opt/webhook-router').resolve()
    files_dir = patch_dir / 'files'
    manifest_path = patch_dir / 'patch-manifest.json'
    if not files_dir.exists() or not manifest_path.exists():
        print('patch directory is invalid')
        return 2

    manifest = json.loads(manifest_path.read_text())
    version = manifest.get('version', 'unknown')
    backup_root = target_root / 'patch' / '_applied_backups' / f"{version}-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    backup_root.mkdir(parents=True, exist_ok=True)

    for rel, info in (manifest.get('files') or {}).items():
        src = files_dir / rel
        dst = target_root / rel
        if not src.exists():
            print(f'missing file in patch: {rel}')
            return 2
        actual = sha256_file(src)
        expected = str((info or {}).get('sha256') or '')
        if expected and actual != expected:
            print(f'hash mismatch in patch payload: {rel}')
            return 2
        if dst.exists() and dst.is_file():
            backup_dst = backup_root / rel
            backup_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(dst, backup_dst)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    current_link = target_root / 'patch' / 'CURRENT'
    current_link.parent.mkdir(parents=True, exist_ok=True)
    current_link.write_text(str(patch_dir) + '\n')
    print(f'applied {version} to {target_root}')
    print(f'backup saved to {backup_root}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
