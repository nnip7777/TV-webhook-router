#!/usr/bin/env python3
from pathlib import Path
import subprocess

root = Path('/Users/nik/strategy/webhook-router')
(root / 'VERSION').write_text('2026.04.26-120\n')
subprocess.run(['python3', 'scripts/generate_build_manifest.py', '2026.04.26-120'], cwd=root, check=True)
subprocess.run(['python3', 'scripts/create_patch.py'], cwd=root, check=True)
