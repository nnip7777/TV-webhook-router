#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-apply}"
GATEWAY="${BINGX_GATEWAY:-}"
IFACE="${BINGX_INTERFACE:-}"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This script is for macOS only" >&2
  exit 1
fi

# Функция для получения маршрута к хосту
route_exists() {
  local ip="$1"
  route -n get "$ip" >/dev/null 2>&1
}

# Функция для определения физических интерфейсов (исключая VPN)
get_physical_interfaces() {
  # Возвращаем все интерфейсы, исключая туннельные (utun, amnezia, wg, tailscale)
  networksetup -listallhardwareports 2>/dev/null | \
    awk '/Device:|Hardware Port:/ {port=$0; getline; if ($1=="Device:") dev=$2; if (port ~ /Wi-Fi|Ethernet|Ethernet/) {if (dev) print dev}}' | \
    sort -u
}

# Функция для определения шлюза физического интерфейса
get_physical_gateway() {
  local iface="$1"
  # Ищем шлюз для конкретного интерфейса, исключая туннельные
  netstat -nr | grep -E "^default.*$iface$" | awk '{print $2}' | head -1
}

# Определяем физические интерфейсы
PHYSICAL_IFACES=$(get_physical_interfaces)

if [[ -z "$IFACE" ]]; then
  # Если интерфейс не указан, пробуем определить лучший
  # Приоритет: интерфейс с активным шлюзом (не туннельным)
  while IFS= read -r line; do
    [[ "$line" =~ ^default ]] || continue
    gw=$(echo "$line" | awk '{print $2}')
    iface=$(echo "$line" | awk '{print $NF}')
    # Проверяем, что это физический интерфейс (не tуннельный)
    if [[ ! "$iface" =~ ^(utun|amnezia|wg|tailscale|ppp) ]]; then
      # Проверяем, что интерфейс в списке физических
      for phys_iface in $PHYSICAL_IFACES; do
        if [[ "$iface" == "$phys_iface" ]]; then
          IFACE="$iface"
          GATEWAY="$gw"
          break 2
        fi
      done
    fi
  done < <(netstat -nr)
  
  # Если не нашли, пробуем по статусу active
  if [[ -z "$IFACE" ]]; then
    for iface in $PHYSICAL_IFACES; do
      if ifconfig "$iface" 2>/dev/null | grep -q "status: active"; then
        IFACE="$iface"
        GATEWAY=$(get_physical_gateway "$iface")
        break
      fi
    done
  fi
  
  # Если всё ещё не нашли, берём первый доступный
  if [[ -z "$IFACE" ]]; then
    IFACE=$(echo "$PHYSICAL_IFACES" | head -1)
    GATEWAY=$(get_physical_gateway "$IFACE")
  fi
fi

if [[ -z "$IFACE" ]]; then
  echo "WARNING: Could not auto-detect physical interface" >&2
  echo "Falling back to en1 (common for home Wi-Fi)" >&2
  IFACE="en1"
fi

if [[ -z "$GATEWAY" ]]; then
  echo "WARNING: Could not auto-detect gateway for $IFACE" >&2
  echo "Falling back to 192.168.0.1 (common home router)" >&2
  GATEWAY="192.168.0.1"
fi

echo "Detected: interface=$IFACE"
[[ -n "$GATEWAY" ]] && echo "Gateway: $GATEWAY" || echo "Gateway: (none, using interface-only)"

# Получаем IP адреса хостов BingX
IPS=()
while IFS= read -r line; do
  [[ -n "$line" ]] && IPS+=("$line")
done < <(python3 - <<'PY'
import socket
import sys

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
    except socket.gaierror as e:
        print(f"WARNING: Failed to resolve {host}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"WARNING: Error for {host}: {e}", file=sys.stderr)
PY
)

if [[ ${#IPS[@]} -eq 0 ]]; then
  echo "ERROR: Could not resolve any BingX hosts" >&2
  exit 1
fi

echo "Resolved ${#IPS[@]} IP addresses:"
printf '  %s\n' "${IPS[@]}"
echo ""

# Функция добавления маршрута через физический интерфейс
add_route() {
  local ip="$1"
  
  if route_exists "$ip"; then
    current_iface=$(route -n get "$ip" 2>/dev/null | awk '/interface:/{print $2; exit}')
    if [[ "$current_iface" == "$IFACE" ]]; then
      echo "[SKIP] Route for $ip already exists via $IFACE"
      return 0
    else
      echo "[UPDATE] Route for $ip exists via $current_iface, updating to $IFACE"
      delete_route "$ip"
    fi
  fi
  
  if [[ -n "$GATEWAY" ]]; then
    echo "[ADD] Adding route for $ip via $GATEWAY on $IFACE"
    if sudo route -n add -host "$ip" "$GATEWAY" -interface "$IFACE" >/dev/null 2>&1; then
      echo "  ✓ Success"
      return 0
    else
      echo "  ✗ Failed to add route"
      return 1
    fi
  else
    echo "[ADD] Adding interface-only route for $ip on $IFACE"
    if sudo route -n add -host "$ip" "$IFACE" >/dev/null 2>&1; then
      echo "  ✓ Success"
      return 0
    else
      echo "  ✗ Failed to add route"
      return 1
    fi
  fi
}

# Функция удаления маршрута
delete_route() {
  local ip="$1"
  
  if ! route_exists "$ip"; then
    echo "[SKIP] No route for $ip to delete"
    return 0
  fi
  
  echo "[DEL] Removing route for $ip"
  sudo route -n delete -host "$ip" >/dev/null 2>&1 || true
  echo "  ✓ Done"
}

# Функция показа маршрутов
show_routes() {
  local ip="$1"
  
  echo "--- Route for $ip ---"
  if route -n get "$ip" 2>/dev/null; then
    return 0
  else
    echo "  No specific route found (will use default)"
    return 1
  fi
}

case "$MODE" in
  apply)
    echo "=== Applying split tunnel routes ==="
    echo "BingX traffic will go via $IFACE"
    [[ -n "$GATEWAY" ]] && echo "Gateway: $GATEWAY" || echo "Gateway: interface-only"
    echo "All other traffic uses default route (VPN)"
    echo ""
    failed=0
    for ip in "${IPS[@]}"; do
      if ! add_route "$ip"; then
        ((failed++)) || true
      fi
    done
    echo ""
    if [[ $failed -gt 0 ]]; then
      echo "WARNING: $failed route(s) failed to add"
      exit 1
    fi
    echo "✓ All routes applied successfully"
    echo ""
    echo "Note: Routes may be reset by network changes. Run 'restore' after VPN reconnect."
    ;;
    
  clear|remove|delete)
    echo "=== Removing split tunnel routes ==="
    for ip in "${IPS[@]}"; do
      delete_route "$ip"
    done
    echo ""
    echo "✓ Routes cleared"
    echo "BingX traffic will now use default route (VPN)"
    ;;
    
  restore)
    echo "=== Restoring routes (after VPN reconnect) ==="
    echo "Clearing and re-applying routes via $IFACE..."
    for ip in "${IPS[@]}"; do
      delete_route "$ip"
    done
    echo ""
    failed=0
    for ip in "${IPS[@]}"; do
      if ! add_route "$ip"; then
        ((failed++)) || true
      fi
    done
    echo ""
    if [[ $failed -gt 0 ]]; then
      echo "WARNING: $failed route(s) failed to add"
      exit 1
    fi
    echo "✓ Routes restored successfully"
    ;;
    
  show)
    echo "=== Current routes for BingX IPs ==="
    echo "Expected interface: $IFACE"
    [[ -n "$GATEWAY" ]] && echo "Expected gateway: $GATEWAY" || echo "Expected gateway: interface-only"
    echo ""
    for ip in "${IPS[@]}"; do
      show_routes "$ip"
      echo ""
    done
    ;;
    
  *)
    echo "Usage: $0 [apply|clear|restore|show]" >&2
    echo "" >&2
    echo "Modes:" >&2
    echo "  apply   - Add routes for BingX IPs via physical interface (bypass VPN)" >&2
    echo "  clear   - Remove previously added routes" >&2
    echo "  restore - Re-apply routes (use after VPN reconnect)" >&2
    echo "  show    - Display current routing for BingX IPs" >&2
    echo "" >&2
    echo "Environment variables:" >&2
    echo "  BINGX_INTERFACE - Override detected interface (e.g., en0, en1)" >&2
    echo "  BINGX_GATEWAY   - Override detected gateway (e.g., 192.168.0.1)" >&2
    echo "" >&2
    echo "How it works:" >&2
    echo "  - BingX traffic: goes directly via $IFACE (bypasses VPN)" >&2
    echo "  - All other traffic: uses default route (through VPN)" >&2
    echo "" >&2
    echo "Tips:" >&2
    echo "  - After reconnecting VPN, run: $0 restore" >&2
    echo "  - To check current routing: $0 show" >&2
    exit 1
    ;;
esac

# Финальная проверка
echo ""
echo "=== Verification ==="
all_ok=true
for ip in "${IPS[@]}"; do
  if route_exists "$ip"; then
    route_info=$(route -n get "$ip" 2>/dev/null)
    route_iface=$(echo "$route_info" | awk '/interface:/{print $2; exit}')
    route_gw=$(echo "$route_info" | awk '/gateway:/{print $2; exit}')
    
    if [[ "$route_iface" == "$IFACE" ]]; then
      if [[ -n "$GATEWAY" && "$route_gw" == "$GATEWAY" ]]; then
        echo "✓ $ip -> $route_gw via $route_iface"
      elif [[ -z "$GATEWAY" ]]; then
        echo "✓ $ip -> interface-only via $route_iface"
      else
        echo "⚠ $ip -> $route_gw via $route_iface (expected $GATEWAY)"
        all_ok=false
      fi
    else
      echo "✗ $ip -> $route_gw via $route_iface (expected $IFACE)"
      all_ok=false
    fi
  else
    echo "✗ $ip -> no route"
    all_ok=false
  fi
done

if $all_ok; then
  echo ""
  echo "✓ All routes verified successfully!"
  echo "BingX traffic will bypass VPN and go directly via $IFACE"
fi