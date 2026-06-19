#!/usr/bin/env python3
import shutil
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) < 2:
        print('usage: rollback_patch.py <backup_dir> [target_root]')
        return 2
    backup_dir = Path(sys.argv[1]).resolve()
    target_root = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else Path('/opt/webhook-router').resolve()
    if not backup_dir.exists() or not backup_dir.is_dir():
        print('backup dir not found')
        return 2

    for src in sorted([p for p in backup_dir.rglob('*') if p.is_file()]):
        rel = src.relative_to(backup_dir)
        dst = target_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        print(f'restored {rel}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
