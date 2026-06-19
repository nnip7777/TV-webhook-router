#!/usr/bin/env bash
set -euo pipefail
pkill -f '/webhook-router/app/server.py' || true
cd "$(dirname "$0")/.."
mkdir -p logs
nohup python3 app/server.py > /tmp/webhook-router.log 2>&1 &
echo $! > /tmp/webhook-router.pid
echo restarted
