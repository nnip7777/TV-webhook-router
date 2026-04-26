#!/usr/bin/env python3
from pathlib import Path
import json
from datetime import datetime, timezone
import subprocess

root = Path('/Users/nik/strategy/webhook-router')
version = '2026.04.26-124'
(root / 'VERSION').write_text(version + '\n')
build_path = root / 'BUILD.json'
build = json.loads(build_path.read_text())
build['version'] = version
build['builtAt'] = datetime.now(timezone.utc).isoformat()
build_path.write_text(json.dumps(build, ensure_ascii=False, indent=2) + '\n')
changelog_path = root / 'CHANGELOG.md'
existing = changelog_path.read_text()
entry = "## 2026.04.26-124\n- Fixed a real Python scoping bug in BingX execution: inner reassignment of `open_qty_kind` inside `_run()` made earlier references raise `UnboundLocalError` on step-side quick orders.\n- Renamed the target-direction open leg variable to keep routed quick-order `qty/qtyKind` behavior from 123 intact while eliminating the scope collision.\n\n"
changelog_path.write_text(entry + existing)
subprocess.run(['python3', 'scripts/create_patch.py'], cwd=root, check=True)
