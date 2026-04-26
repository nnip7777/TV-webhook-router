#!/usr/bin/env python3
from pathlib import Path
import json
from datetime import datetime, timezone
import subprocess

root = Path('/Users/nik/strategy/webhook-router')
version = '2026.04.26-121'
(root / 'VERSION').write_text(version + '\n')
build_path = root / 'BUILD.json'
build = json.loads(build_path.read_text())
build['version'] = version
build['builtAt'] = datetime.now(timezone.utc).isoformat()
build_path.write_text(json.dumps(build, ensure_ascii=False, indent=2) + '\n')
changelog_path = root / 'CHANGELOG.md'
existing = changelog_path.read_text()
entry = "## 2026.04.26-121\n- Fixed BingX target-direction execution to preserve routed fixed `qty` and `qtyKind` instead of overwriting them from the incoming webhook payload.\n- `openQtyKind` now remains separate, so target-direction flows can still open in quote mode when configured without corrupting the logged/requested final qty.\n\n"
changelog_path.write_text(entry + existing)
subprocess.run(['python3', 'scripts/create_patch.py'], cwd=root, check=True)
