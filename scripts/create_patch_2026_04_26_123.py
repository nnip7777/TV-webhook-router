#!/usr/bin/env python3
from pathlib import Path
import json
from datetime import datetime, timezone
import subprocess

root = Path('/Users/nik/strategy/webhook-router')
version = '2026.04.26-123'
(root / 'VERSION').write_text(version + '\n')
build_path = root / 'BUILD.json'
build = json.loads(build_path.read_text())
build['version'] = version
build['builtAt'] = datetime.now(timezone.utc).isoformat()
build_path.write_text(json.dumps(build, ensure_ascii=False, indent=2) + '\n')
changelog_path = root / 'CHANGELOG.md'
existing = changelog_path.read_text()
entry = "## 2026.04.26-123\n- Fixed BingX execution regression in non-target-direction quick orders where `openQtyKind` could be referenced before initialization.\n- Preserved the 122 semantics: quick-order fixed `qty` still comes from route, and request `qtyKind` still follows the routed destination.\n\n"
changelog_path.write_text(entry + existing)
subprocess.run(['python3', 'scripts/create_patch.py'], cwd=root, check=True)
