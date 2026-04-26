#!/usr/bin/env python3
from pathlib import Path
import json
from datetime import datetime, timezone
import subprocess

root = Path('/Users/nik/strategy/webhook-router')
version = '2026.04.26-122'
(root / 'VERSION').write_text(version + '\n')
build_path = root / 'BUILD.json'
build = json.loads(build_path.read_text())
build['version'] = version
build['builtAt'] = datetime.now(timezone.utc).isoformat()
build_path.write_text(json.dumps(build, ensure_ascii=False, indent=2) + '\n')
changelog_path = root / 'CHANGELOG.md'
existing = changelog_path.read_text()
entry = "## 2026.04.26-122\n- Fixed BingX quick-order target-direction semantics so request `qtyKind` stays `usdt` when the routed destination is configured that way.\n- Preserved `route.qty` as the source of truth for final request quantity while still keeping `openQtyKind` explicit for prepare/open stages.\n\n"
changelog_path.write_text(entry + existing)
subprocess.run(['python3', 'scripts/create_patch.py'], cwd=root, check=True)
