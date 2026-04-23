#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-apply}"
shift || true

DEV="${BINGX_INTERFACE:-}"
VIA="${BINGX_GATEWAY:-}"
ENV_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev)
      DEV="$2"
      shift 2
      ;;
    --via)
      VIA="$2"
      shift 2
      ;;
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This script is for Linux only" >&2
  exit 1
fi

if [[ -z "$DEV" ]]; then
  DEV="$(ip -4 route show default 2>/dev/null | awk '$1=="default" && $5 !~ /^(tun|tap|wg|tailscale|ppp)/ {print $5; exit}')"
fi

if [[ -z "$VIA" ]]; then
  VIA="$(ip -4 route show default 2>/dev/null | awk '$1=="default" && $5 !~ /^(tun|tap|wg|tailscale|ppp)/ {print $3; exit}')"
fi

if [[ -z "$DEV" ]]; then
  echo "Missing interface. Use --dev <iface> or BINGX_INTERFACE" >&2
  echo "Current default routes:" >&2
  ip -4 route show default >&2 || true
  exit 1
fi

IPS=()
while IFS= read -r line; do
  [[ -n "$line" ]] && IPS+=("$line")
done < <(python3 - <<'PY'
import socket
hosts = [
    'bingx.com',
    'open-api.bingx.com',
    'open-api.bingx.pro',
    'open-api-vst.bingx.com',
    'open-api-vst.bingx.pro',
]
seen = set()
for host in hosts:
    try:
        for item in socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP):
            ip = item[4][0]
            if ':' in ip or ip in seen:
                continue
            seen.add(ip)
            print(ip)
    except Exception:
        pass
PY
)

if [[ ${#IPS[@]} -eq 0 ]]; then
  echo "Could not resolve BingX hosts" >&2
  exit 1
fi

echo "mode=$MODE dev=$DEV via=${VIA:-<none>}"
printf 'ips:\n'
printf '  %s\n' "${IPS[@]}"

apply_one() {
  local ip="$1"
  if [[ -n "$VIA" ]]; then
    sudo ip route replace "$ip/32" via "$VIA" dev "$DEV"
  else
    sudo ip route replace "$ip/32" dev "$DEV"
  fi
}

clear_one() {
  local ip="$1"
  sudo ip route del "$ip/32" >/dev/null 2>&1 || true
}

case "$MODE" in
  apply)
    for ip in "${IPS[@]}"; do
      apply_one "$ip"
    done
    if [[ -n "$ENV_FILE" ]]; then
      sudo python3 - "$ENV_FILE" "$DEV" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
dev = sys.argv[2]
text = path.read_text() if path.exists() else ''
lines = text.splitlines()
out = []
found = False
for line in lines:
    if line.startswith('BINGX_BIND_INTERFACE='):
        out.append(f'BINGX_BIND_INTERFACE={dev}')
        found = True
    else:
        out.append(line)
if not found:
    out.append(f'BINGX_BIND_INTERFACE={dev}')
path.write_text('\n'.join(out).rstrip('\n') + '\n')
PY
      echo "Updated $ENV_FILE with BINGX_BIND_INTERFACE=$DEV"
    fi
    ;;
  clear|remove|delete)
    for ip in "${IPS[@]}"; do
      clear_one "$ip"
    done
    ;;
  show)
    ;;
  *)
    echo "Usage: $0 [apply|clear|show] --dev <iface> [--via <gateway>] [--env-file /opt/webhook-router/.env]" >&2
    exit 1
    ;;
esac

for ip in "${IPS[@]}"; do
  echo "--- $ip"
  ip route get "$ip" || true
done
