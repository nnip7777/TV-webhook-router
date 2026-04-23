#!/usr/bin/env python3
import os
from pathlib import Path
from typing import Dict, Optional

APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent
ENV_PATH = PROJECT_ROOT / '.env'


def parse_env_file(path: Path = ENV_PATH) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not path.exists():
        return data
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        parsed = value.strip()
        if len(parsed) >= 2 and ((parsed[0] == '"' and parsed[-1] == '"') or (parsed[0] == "'" and parsed[-1] == "'")):
            parsed = parsed[1:-1]
        parsed = parsed.replace('\\n', '\n')
        data[key.strip()] = parsed
    return data


def load_env_file(path: Path = ENV_PATH, override: bool = False) -> Dict[str, str]:
    data = parse_env_file(path)
    for key, value in data.items():
        if override or key not in os.environ:
            os.environ[key] = value
    return data


def save_env_file(values: Dict[str, str], path: Path = ENV_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for key, value in values.items():
        safe = str(value).replace('\r\n', '\n').replace('\r', '\n')
        safe = safe.replace('\\', '\\\\').replace('\n', '\\n')
        if any(ch.isspace() for ch in safe) or '#' in safe or '"' in safe or "'" in safe:
            safe = '"' + safe.replace('\\', '\\\\').replace('"', '\\"') + '"'
        lines.append(f'{key}={safe}')
    path.write_text('\n'.join(lines) + '\n')


load_env_file(override=True)


def reload_env() -> Dict[str, str]:
    return load_env_file(override=True)


def env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == '':
        return default
    return value


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ''):
        return default
    try:
        return int(value)
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    value = str(os.getenv(name, '')).strip().lower()
    if not value:
        return default
    return value in ('1', 'true', 'yes', 'on')


def env_path(name: str, default: str, root: Path = None) -> Path:
    raw = env_str(name, default) or default
    path = Path(raw).expanduser()
    base = root or PROJECT_ROOT
    if not path.is_absolute():
        path = (base / path)
    return path.resolve()


ROOT = env_path('WEBHOOK_ROUTER_ROOT', str(PROJECT_ROOT), PROJECT_ROOT)
CONFIG_PATH = env_path('WEBHOOK_ROUTER_CONFIG_PATH', 'config/routing.json', ROOT)
LOG_PATH = env_path('WEBHOOK_ROUTER_LOG_PATH', 'logs/webhooks.jsonl', ROOT)
JOURNAL_LOG_PATH = env_path('WEBHOOK_ROUTER_JOURNAL_LOG_PATH', 'logs/journal', ROOT)
OBSERVED_SIGNALS_PATH = env_path('WEBHOOK_ROUTER_OBSERVED_SIGNALS_PATH', 'config/observed_signals.json', ROOT)
INSTRUMENTS_PATH = env_path('WEBHOOK_ROUTER_INSTRUMENTS_PATH', 'config/instruments.json', ROOT)

SERVER_HOST = env_str('WEBHOOK_ROUTER_HOST', '127.0.0.1')
SERVER_PORT = env_int('WEBHOOK_ROUTER_PORT', 8787)
PUBLIC_BASE_URL = env_str('WEBHOOK_ROUTER_PUBLIC_BASE_URL', '')

SMART_EXECUTOR_PATH = env_path('WEBHOOK_ROUTER_SMART_EXECUTOR_PATH', 'external/smart_order_executor.py', ROOT)

ALOR_CONFIG_PATH = env_path('ALOR_CONFIG_PATH', 'broker/alor/config.json', ROOT)
ALOR_OAUTH_URL = env_str('ALOR_OAUTH_URL', 'https://oauth.alor.ru')
ALOR_API_BASE_URL = env_str('ALOR_API_BASE_URL', 'https://api.alor.ru')
ALOR_ALLOW_MARGIN = env_bool('ALOR_ALLOW_MARGIN', True)

FINAM_SECRET_PATH = env_path('FINAM_SECRET_PATH', 'broker/finam/token.secret', ROOT)
FINAM_ACCOUNT_ID = env_str('FINAM_ACCOUNT_ID', '')

SCHWAB_CONFIG_PATH = env_path('SCHWAB_CONFIG_PATH', 'broker/schwab/config.json', ROOT)

BYBIT_API_KEY = env_str('BYBIT_API_KEY', '') or ''
BYBIT_SECRET_KEY = env_str('BYBIT_SECRET_KEY', '') or ''
BYBIT_LIVE_BASE_URL = env_str('BYBIT_LIVE_BASE_URL', 'https://api.bybit.com')
BYBIT_TESTNET_BASE_URL = env_str('BYBIT_TESTNET_BASE_URL', 'https://api-testnet.bybit.com')
BYBIT_BIND_INTERFACE = env_str('BYBIT_BIND_INTERFACE', '') or ''

BINGX_API_KEY = env_str('BINGX_API_KEY', '') or ''
BINGX_SECRET_KEY = env_str('BINGX_SECRET_KEY', '') or ''
BINGX_LIVE_BASE_URL = env_str('BINGX_LIVE_BASE_URL', 'https://open-api.bingx.com')
BINGX_LIVE_FALLBACK_BASE_URL = env_str('BINGX_LIVE_FALLBACK_BASE_URL', 'https://open-api.bingx.pro')
BINGX_TESTNET_BASE_URL = env_str('BINGX_TESTNET_BASE_URL', 'https://open-api-vst.bingx.com')
BINGX_TESTNET_FALLBACK_BASE_URL = env_str('BINGX_TESTNET_FALLBACK_BASE_URL', 'https://open-api-vst.bingx.pro')
BINGX_BIND_INTERFACE = env_str('BINGX_BIND_INTERFACE', '') or ''
BINGX_SOURCE_KEY = env_str('BINGX_SOURCE_KEY', 'BX-AI-SKILL') or 'BX-AI-SKILL'
BINGX_RECV_WINDOW = env_int('BINGX_RECV_WINDOW', 5000)

ADMIN_PASSWORD = env_str('WEBHOOK_ROUTER_ADMIN_PASSWORD', '') or ''
BACKUP_DIR = env_path('WEBHOOK_ROUTER_BACKUP_DIR', 'backups', ROOT)
BACKUP_KEEP_COUNT = env_int('WEBHOOK_ROUTER_BACKUP_KEEP_COUNT', 20)
JOURNAL_KEEP_WEEKS = env_int('WEBHOOK_ROUTER_JOURNAL_KEEP_WEEKS', 12)
