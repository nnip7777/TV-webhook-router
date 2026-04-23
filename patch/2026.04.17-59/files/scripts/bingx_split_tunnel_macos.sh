#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-apply}"
GATEWAY="${BINGX_GATEWAY:-}"
IFACE="${BINGX_INTERFACE:-}"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This script is for macOS only" >&2
  exit 1
fi

if [[ -z "$GATEWAY" ]]; then
  GATEWAY="$(route -n get default 2>/dev/null | awk '/gateway:/{print $2; exit}')"
fi

if [[ -z "$IFACE" ]]; then
  IFACE="$(route -n get default 2>/dev/null | awk '/interface:/{print $2; exit}')"
fi

if [[ -z "$GATEWAY" || -z "$IFACE" ]]; then
  echo "Could not detect default gateway/interface" >&2
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

echo "mode=$MODE gateway=$GATEWAY iface=$IFACE"
printf 'ips:\n'
printf '  %s\n' "${IPS[@]}"

case "$MODE" in
  apply)
    for ip in "${IPS[@]}"; do
      sudo route -n delete -host "$ip" >/dev/null 2>&1 || true
      sudo route -n add -host "$ip" "$GATEWAY"
    done
    ;;
  clear|remove|delete)
    for ip in "${IPS[@]}"; do
      sudo route -n delete -host "$ip" >/dev/null 2>&1 || true
    done
    ;;
  show)
    ;;
  *)
    echo "Usage: $0 [apply|clear|show]" >&2
    exit 1
    ;;
esac

for ip in "${IPS[@]}"; do
  echo "--- $ip"
  route -n get "$ip" | sed -n '1,12p'
done
