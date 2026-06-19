#!/usr/bin/env python3
import asyncio
import copy
import gzip
import hashlib
import html
import json
import os
import queue
import re
import secrets
import threading
import time
import traceback
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs

from bingx_adapter import BingXBroker
from bybit_adapter import BybitBroker
from finam_adapter import auth_check_sync as finam_auth_check_sync
from schwab.client.base import BaseClient
from schwab_adapter import SchwabBroker
from bingx_adapter import _bingx_rows
from execution import execute_route_sync
from analytics import analytics_overview, init_analytics_db, record_execution_analytics
from settings import (
    ALOR_API_BASE_URL,
    ALOR_CONFIG_PATH,
    ALOR_OAUTH_URL,
    ANALYTICS_DB_PATH,
    BACKUP_DIR,
    BACKUP_KEEP_COUNT,
    CONFIG_PATH,
    ENV_PATH,
    FINAM_ACCOUNT_ID,
    INSTRUMENTS_PATH,
    JOURNAL_KEEP_WEEKS,
    JOURNAL_LOG_PATH,
    LOG_PATH,
    OBSERVED_SIGNALS_PATH,
    ROOT,
    SERVER_HOST,
    SERVER_PORT,
    parse_env_file,
    reload_env,
    save_env_file,
)
TEMPLATE_RE = re.compile(r'\{\{\s*([a-zA-Z0-9_\.]+)\s*\}\}')
METRICS_CACHE: Dict[str, Any] = {
    'data': {
        'alor': {'summary': {}, 'symbols': {}},
        'bybit': {'summary': {}, 'symbols': {}},
        'bingx': {'summary': {}, 'symbols': {}},
        'finam': {'summary': {}, 'symbols': {}},
        'schwab': {'summary': {}, 'symbols': {}},
    },
    'updated_at': 0.0,
    'refreshing': False,
    'new_tickers': [],
}
METRICS_SYNC_INTERVAL_SECONDS = 20
WEBHOOK_QUEUE: queue.Queue = queue.Queue()
WEBHOOK_WORKER_COUNT = 1
OBSERVED_SIGNALS_LOCK = threading.Lock()
LIVE_TICKER_RELOAD_LOCK = threading.Lock()
ADMIN_SESSIONS: Dict[str, float] = {}
ADMIN_SESSION_TTL_SECONDS = 60 * 60 * 24 * 14

USER_DB_PATH = ROOT / 'config' / 'users.json'
USER_SESSION_COOKIE = 'wr_session'
ROLE_ADMIN = 'admin'
DEFAULT_ROLES: Dict[str, Dict[str, Any]] = {
    'admin': {
        'label': 'Admin',
        'permissions': {
            'brokers': ['*'],
            'settingsSections': ['*'],
            'canAddTickers': True,
            'canAssign': True,
            'canManageUsers': True,
            'canManageBackups': True,
            'canDownloadBackups': True,
            'canEmailBackups': True,
            'canViewJournal': True,
            'canEditMappings': True,
            'canQuickOrder': True,
        },
    },
    'manager': {
        'label': 'Manager',
        'permissions': {
            'brokers': [],
            'settingsSections': [],
            'canAddTickers': True,
            'canAssign': True,
            'canManageUsers': False,
            'canManageBackups': False,
            'canDownloadBackups': False,
            'canEmailBackups': False,
            'canViewJournal': True,
            'canEditMappings': True,
            'canQuickOrder': True,
        },
    },
    'editor': {
        'label': 'Editor',
        'permissions': {
            'brokers': [],
            'settingsSections': [],
            'canAddTickers': True,
            'canAssign': False,
            'canManageUsers': False,
            'canManageBackups': False,
            'canDownloadBackups': False,
            'canEmailBackups': False,
            'canViewJournal': True,
            'canEditMappings': True,
            'canQuickOrder': False,
        },
    },
    'viewer': {
        'label': 'Viewer',
        'permissions': {
            'brokers': [],
            'settingsSections': [],
            'canAddTickers': False,
            'canAssign': False,
            'canManageUsers': False,
            'canManageBackups': False,
            'canDownloadBackups': False,
            'canEmailBackups': False,
            'canViewJournal': True,
            'canEditMappings': False,
            'canQuickOrder': False,
        },
    },
}
BOOLEAN_PERMISSION_KEYS = [
    'canAddTickers',
    'canAssign',
    'canManageUsers',
    'canManageBackups',
    'canDownloadBackups',
    'canEmailBackups',
    'canViewJournal',
    'canEditMappings',
    'canQuickOrder',
]
USER_SESSIONS: Dict[str, Dict[str, Any]] = {}
BROKER_TEST_CACHE: Dict[str, Dict[str, Any]] = {}
BROKER_ORDER_STATE_LOCK = threading.Lock()
BROKER_ORDER_STATE_CACHE: Dict[str, Dict[str, Dict[str, Any]]] = {}
BROKER_ORDER_STATE_TTL_SECONDS = 60 * 60 * 24
WORKING_ORDER_STATUSES = {'WORKING', 'QUEUED', 'PENDING_ACTIVATION', 'PENDING_ACKNOWLEDGEMENT', 'PENDING_RECALL', 'ACCEPTED', 'AWAITING_PARENT_ORDER', 'AWAITING_CONDITION'}
FINAL_ORDER_STATUSES = {'FILLED', 'EXECUTED', 'REJECTED', 'CANCELED', 'CANCELLED', 'EXPIRED', 'REPLACED'}
ERROR_ORDER_STATUSES = {'REJECTED', 'CANCELED', 'CANCELLED', 'EXPIRED'}
SETTINGS_FIELDS = [
    {'key': 'WEBHOOK_ROUTER_ADMIN_PASSWORD', 'label': 'Admin password', 'section': 'admin', 'type': 'password'},
    {'key': 'WEBHOOK_ROUTER_HOST', 'label': 'Host', 'section': 'admin', 'type': 'text'},
    {'key': 'WEBHOOK_ROUTER_PORT', 'label': 'Port', 'section': 'admin', 'type': 'text'},
    {'key': 'WEBHOOK_ROUTER_PUBLIC_BASE_URL', 'label': 'Public base URL', 'section': 'admin', 'type': 'text'},
    {'key': 'WEBHOOK_ROUTER_BACKUP_DIR', 'label': 'Backup dir', 'section': 'admin', 'type': 'path'},
    {'key': 'WEBHOOK_ROUTER_BACKUP_KEEP_COUNT', 'label': 'Backup keep count', 'section': 'admin', 'type': 'text'},
    {'key': 'WEBHOOK_ROUTER_JOURNAL_KEEP_WEEKS', 'label': 'Journal keep weeks', 'section': 'admin', 'type': 'text'},
    {'key': 'WEBHOOK_ROUTER_ROOT', 'label': 'Root', 'section': 'paths', 'type': 'path'},
    {'key': 'WEBHOOK_ROUTER_CONFIG_PATH', 'label': 'Routing config path', 'section': 'paths', 'type': 'path'},
    {'key': 'WEBHOOK_ROUTER_OBSERVED_SIGNALS_PATH', 'label': 'Observed signals path', 'section': 'paths', 'type': 'path'},
    {'key': 'WEBHOOK_ROUTER_INSTRUMENTS_PATH', 'label': 'Instruments path', 'section': 'paths', 'type': 'path'},
    {'key': 'WEBHOOK_ROUTER_LOG_PATH', 'label': 'Webhook log path', 'section': 'paths', 'type': 'path'},
    {'key': 'WEBHOOK_ROUTER_JOURNAL_LOG_PATH', 'label': 'Journal log path', 'section': 'paths', 'type': 'path'},
    {'key': 'WEBHOOK_ROUTER_ANALYTICS_DB_PATH', 'label': 'Analytics SQLite path', 'section': 'paths', 'type': 'path'},
    {'key': 'WEBHOOK_ROUTER_SMART_EXECUTOR_PATH', 'label': 'Legacy smart_order_executor path', 'section': 'paths', 'type': 'path'},
    {'key': 'BYBIT_API_KEY', 'label': 'Bybit API key', 'section': 'bybit', 'type': 'text'},
    {'key': 'BYBIT_SECRET_KEY', 'label': 'Bybit secret key', 'section': 'bybit', 'type': 'password'},
    {'key': 'BYBIT_LIVE_BASE_URL', 'label': 'Bybit live base URL', 'section': 'bybit', 'type': 'text'},
    {'key': 'BYBIT_TESTNET_BASE_URL', 'label': 'Bybit testnet base URL', 'section': 'bybit', 'type': 'text'},
    {'key': 'BYBIT_BIND_INTERFACE', 'label': 'Bybit bind interface (optional, e.g. bybit-egress)', 'section': 'bybit', 'type': 'text'},
    {'key': 'BINGX_API_KEY', 'label': 'BingX API key', 'section': 'bingx', 'type': 'text'},
    {'key': 'BINGX_SECRET_KEY', 'label': 'BingX secret key', 'section': 'bingx', 'type': 'password'},
    {'key': 'BINGX_LIVE_BASE_URL', 'label': 'BingX live base URL', 'section': 'bingx', 'type': 'text'},
    {'key': 'BINGX_LIVE_FALLBACK_BASE_URL', 'label': 'BingX live fallback base URL', 'section': 'bingx', 'type': 'text'},
    {'key': 'BINGX_TESTNET_BASE_URL', 'label': 'BingX testnet base URL', 'section': 'bingx', 'type': 'text'},
    {'key': 'BINGX_TESTNET_FALLBACK_BASE_URL', 'label': 'BingX testnet fallback base URL', 'section': 'bingx', 'type': 'text'},
    {'key': 'BINGX_BIND_INTERFACE', 'label': 'BingX bind interface (optional, e.g. bingx-egress)', 'section': 'bingx', 'type': 'text'},
    {'key': 'BINGX_SOURCE_KEY', 'label': 'BingX source key', 'section': 'bingx', 'type': 'text', 'default': 'BX-AI-SKILL'},
    {'key': 'BINGX_RECV_WINDOW', 'label': 'BingX recvWindow', 'section': 'bingx', 'type': 'text', 'default': '5000'},
    {'key': 'ALOR_CONFIG_PATH', 'label': 'Alor config JSON path', 'section': 'alor', 'type': 'path'},
    {'key': 'ALOR_OAUTH_URL', 'label': 'Alor OAuth URL', 'section': 'alor', 'type': 'text'},
    {'key': 'ALOR_API_BASE_URL', 'label': 'Alor API base URL', 'section': 'alor', 'type': 'text'},
    {'key': 'ALOR_ALLOW_MARGIN', 'label': 'Alor allowMargin', 'section': 'alor', 'type': 'bool', 'default': 'true'},
    {'key': 'FINAM_ACCOUNT_ID', 'label': 'Finam account id', 'section': 'finam', 'type': 'text'},
    {'key': 'FINAM_SECRET_PATH', 'label': 'Finam token file path', 'section': 'finam', 'type': 'path'},
    {'key': 'SCHWAB_CONFIG_PATH', 'label': 'Schwab config JSON path', 'section': 'schwab', 'type': 'path'},
]
SETTINGS_FIELD_MAP = {field['key']: field for field in SETTINGS_FIELDS}
ENV_KEYS_ORDER = [field['key'] for field in SETTINGS_FIELDS]
BUILD_INFO_PATH = ROOT / 'BUILD.json'
VERSION_PATH = ROOT / 'VERSION'


def _load_build_info() -> Dict[str, Any]:
    build: Dict[str, Any] = {}
    try:
        if BUILD_INFO_PATH.exists():
            build = json.loads(BUILD_INFO_PATH.read_text())
    except Exception:
        build = {}
    if not build.get('version'):
        try:
            if VERSION_PATH.exists():
                build['version'] = VERSION_PATH.read_text().strip()
        except Exception:
            pass
    return build


def _build_summary() -> Dict[str, str]:
    info = _load_build_info()
    version = str(info.get('version') or 'unknown')
    built_at = str(info.get('builtAt') or '')
    file_count = str(info.get('fileCount') or '')
    files = info.get('files') or {}
    server_hash = ''
    if isinstance(files, dict):
        server_hash = str(((files.get('app/server.py') or {}).get('sha256')) or '')[:12]
    return {
        'version': version,
        'builtAt': built_at,
        'serverHash': server_hash,
        'fileCount': file_count,
    }


def load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text())


def _env_values() -> Dict[str, str]:
    return parse_env_file(ENV_PATH)


def _ordered_env_values() -> Dict[str, str]:
    raw = _env_values()
    ordered: Dict[str, str] = {}
    for field in SETTINGS_FIELDS:
        key = field['key']
        if key in raw:
            ordered[key] = raw.get(key, '')
        elif 'default' in field:
            ordered[key] = str(field.get('default', ''))
    for key in sorted(raw.keys()):
        if key not in ordered:
            ordered[key] = raw[key]
    return ordered


def _write_text_secret(path_value: str, content: str) -> None:
    path = Path(path_value).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip('\n') + ('\n' if content else ''))


def _backup_dir() -> Path:
    values = _env_values()
    return Path(values.get('WEBHOOK_ROUTER_BACKUP_DIR') or str(BACKUP_DIR)).expanduser()


def _backup_keep_count() -> int:
    values = _env_values()
    raw = values.get('WEBHOOK_ROUTER_BACKUP_KEEP_COUNT', str(BACKUP_KEEP_COUNT))
    try:
        return max(1, int(raw))
    except Exception:
        return max(1, BACKUP_KEEP_COUNT)


def _backup_file_name(prefix: str) -> str:
    stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    return f"{prefix}-{stamp}"


def _prune_backups() -> None:
    backup_dir = _backup_dir()
    backup_dir.mkdir(parents=True, exist_ok=True)
    keep = _backup_keep_count()
    entries = sorted([p for p in backup_dir.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True)
    for old in entries[keep:]:
        for child in sorted(old.rglob('*'), reverse=True):
            if child.is_file() or child.is_symlink():
                child.unlink(missing_ok=True)
            elif child.is_dir():
                child.rmdir()
        old.rmdir()


def _create_backup(label: str = 'manual') -> str:
    env_values = _ordered_env_values()
    backup_dir = _backup_dir() / _backup_file_name(label)
    backup_dir.mkdir(parents=True, exist_ok=True)
    save_env_file(env_values, backup_dir / '.env')
    if env_values.get('ALOR_CONFIG_PATH'):
        _write_text_secret(str(backup_dir / 'alor-config.json'), _read_text_secret(env_values.get('ALOR_CONFIG_PATH', '')))
    if env_values.get('FINAM_SECRET_PATH'):
        _write_text_secret(str(backup_dir / 'finam-token.secret'), _read_text_secret(env_values.get('FINAM_SECRET_PATH', '')))
    if env_values.get('SCHWAB_CONFIG_PATH'):
        _write_text_secret(str(backup_dir / 'schwab-config.json'), _read_text_secret(env_values.get('SCHWAB_CONFIG_PATH', '')))
    if CONFIG_PATH.exists():
        (backup_dir / 'routing.json').write_text(CONFIG_PATH.read_text())
    if OBSERVED_SIGNALS_PATH.exists():
        (backup_dir / 'observed_signals.json').write_text(OBSERVED_SIGNALS_PATH.read_text())
    if INSTRUMENTS_PATH.exists():
        (backup_dir / 'instruments.json').write_text(INSTRUMENTS_PATH.read_text())
    if USER_DB_PATH.exists():
        (backup_dir / 'users.json').write_text(USER_DB_PATH.read_text())
    _prune_backups()
    return backup_dir.name


def _list_backups() -> List[Dict[str, Any]]:
    backup_dir = _backup_dir()
    backup_dir.mkdir(parents=True, exist_ok=True)
    items = []
    for p in sorted([x for x in backup_dir.iterdir() if x.is_dir()], key=lambda x: x.name, reverse=True):
        files = sorted([f.name for f in p.iterdir() if f.is_file()])
        items.append({'name': p.name, 'files': files})
    return items


def _restore_backup(name: str) -> None:
    target = _backup_dir() / name
    if not target.exists() or not target.is_dir():
        raise FileNotFoundError('backup_not_found')
    env_backup = target / '.env'
    if env_backup.exists():
        ENV_PATH.write_text(env_backup.read_text())
    restore_env = parse_env_file(ENV_PATH)
    if (target / 'routing.json').exists():
        CONFIG_PATH.write_text((target / 'routing.json').read_text())
    if (target / 'observed_signals.json').exists():
        OBSERVED_SIGNALS_PATH.write_text((target / 'observed_signals.json').read_text())
    if (target / 'instruments.json').exists():
        INSTRUMENTS_PATH.write_text((target / 'instruments.json').read_text())
    if (target / 'users.json').exists():
        USER_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        USER_DB_PATH.write_text((target / 'users.json').read_text())
    if restore_env.get('ALOR_CONFIG_PATH') and (target / 'alor-config.json').exists():
        _write_text_secret(restore_env.get('ALOR_CONFIG_PATH', ''), (target / 'alor-config.json').read_text())
    if restore_env.get('FINAM_SECRET_PATH') and (target / 'finam-token.secret').exists():
        _write_text_secret(restore_env.get('FINAM_SECRET_PATH', ''), (target / 'finam-token.secret').read_text())
    if restore_env.get('SCHWAB_CONFIG_PATH') and (target / 'schwab-config.json').exists():
        _write_text_secret(restore_env.get('SCHWAB_CONFIG_PATH', ''), (target / 'schwab-config.json').read_text())
    reload_env()


def _backup_tarball_path(name: str) -> Path:
    safe_name = re.sub(r'[^a-zA-Z0-9._-]+', '-', str(name or '').strip())
    if not safe_name:
        raise ValueError('invalid_backup_name')
    return _backup_dir() / f'{safe_name}.tar.gz'


def _export_backup_tarball(name: str) -> Path:
    target = _backup_dir() / name
    if not target.exists() or not target.is_dir():
        raise FileNotFoundError('backup_not_found')
    tar_path = _backup_tarball_path(name)
    import tarfile
    with tarfile.open(tar_path, 'w:gz') as tar:
        tar.add(target, arcname=name)
    return tar_path


def _delete_backup(name: str) -> None:
    target = _backup_dir() / name
    if not target.exists() or not target.is_dir():
        raise FileNotFoundError('backup_not_found')
    for child in sorted(target.rglob('*'), reverse=True):
        if child.is_file() or child.is_symlink():
            child.unlink(missing_ok=True)
        elif child.is_dir():
            child.rmdir()
    target.rmdir()


def _dedupe_keep_order(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        value = str(item or '').strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _broker_uses_category_venue(broker_name: str) -> bool:
    return str(broker_name or '').strip().lower() in ('bybit', 'bingx')


def _broker_venue_key(broker_name: str) -> str:
    return 'category' if _broker_uses_category_venue(broker_name) else 'exchange'


def _ensure_supported_broker_defaults(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config or {})
    brokers = config.setdefault('brokers', {})
    if 'bingx' not in brokers:
        brokers['bingx'] = {
            'label': 'BingX',
            'enabled': True,
            'status': 'ready-live',
            'defaultDestination': {
                'account': 'swap',
                'category': 'swap',
                'executionMode': 'maker',
                'qtyMode': 'pass-through',
                'positionSide': 'BOTH',
                'signalMode': 'step-side',
                'testnet': False,
            },
            'lookupVenues': ['swap'],
            'lookupSymbols': ['BTC-USDT', 'ETH-USDT', 'SOL-USDT'],
            'symbolMap': {
                'BTCUSDT': 'BTC-USDT',
                'BTCUSDT.P': 'BTC-USDT',
                'ETHUSDT': 'ETH-USDT',
                'ETHUSDT.P': 'ETH-USDT',
                'SOLUSDT': 'SOL-USDT',
                'SOLUSDT.P': 'SOL-USDT',
            },
        }
    return config


def _sync_broker_lookup_lists(config: Dict[str, Any], broker: str) -> Dict[str, Any]:
    broker = str(broker or '').strip().lower()
    broker_cfg = config.get('brokers', {}).get(broker)
    if not broker_cfg:
        return {'ok': False, 'details': f'unknown broker: {broker}'}

    symbols: List[str] = []
    venues: List[str] = []

    if broker == 'bybit':
        bybit = BybitBroker(testnet=False)
        for category in ('linear', 'inverse', 'spot', 'option'):
            data = bybit._request('GET', '/v5/market/instruments-info', params={'category': category, 'limit': 1000})
            for item in (((data.get('result') or {}).get('list')) or []):
                symbol = str(item.get('symbol') or '').strip()
                if symbol:
                    symbols.append(symbol)
                    venues.append(category)
    elif broker == 'bingx':
        bingx_cfg = (config.get('brokers', {}).get('bingx', {}) or {}).get('defaultDestination', {}) or {}
        bingx = BingXBroker(testnet=bool(bingx_cfg.get('testnet', False)))
        symbols_v3 = bingx.get_symbols_v3()
        contracts_v2 = bingx.get_contracts()
        limits = broker_cfg.setdefault('symbolLimits', {})
        symbol_rows = _bingx_rows(symbols_v3)
        contract_rows = _bingx_rows(contracts_v2)
        contract_index = {
            str(item.get('symbol') or '').strip(): item
            for item in contract_rows if isinstance(item, dict) and str(item.get('symbol') or '').strip()
        }
        raw_count = len(symbol_rows)
        filtered_out_closed = 0
        filtered_out_invalid = 0
        sample_symbols = []
        contains_gas = False
        source_rows = symbol_rows if symbol_rows else contract_rows
        for item in source_rows:
            if not isinstance(item, dict):
                filtered_out_invalid += 1
                continue
            symbol = str(item.get('symbol') or '').strip()
            if not symbol:
                filtered_out_invalid += 1
                continue
            if len(sample_symbols) < 30:
                sample_symbols.append(symbol)
            if symbol == 'GAS-USDT':
                contains_gas = True
            api_open = str(item.get('apiStateOpen', 'true')).lower() != 'false'
            broker_open_raw = item.get('brokerState', 'true')
            broker_open = str(broker_open_raw).lower() != 'false'
            effective_open = api_open if broker_name == 'bingx' else (api_open and broker_open)
            if not effective_open:
                filtered_out_closed += 1
            symbols.append(symbol)
            display_name = str(item.get('displayName') or '').strip()
            asset_name = str(item.get('asset') or '').strip()
            for alias in (display_name, asset_name):
                if alias:
                    symbols.append(alias)
            venues.append('swap')
            normalized_symbol = str(symbol).replace('-', '').replace('_', '').replace('.P', '').replace('/', '').upper()
            contract_item = contract_index.get(symbol) or contract_index.get(normalized_symbol) or item or {}
            limit_payload = {
                'minQty': contract_item.get('tradeMinQuantity') or contract_item.get('minQty') or contract_item.get('minOrderQty') or contract_item.get('minTradeAmount'),
                'minUsdt': contract_item.get('tradeMinUSDT') or contract_item.get('minNotional'),
                'qtyStep': contract_item.get('size') or contract_item.get('stepSize') or contract_item.get('quantityStep') or contract_item.get('qtyStep'),
                'priceStep': contract_item.get('tickSize') or contract_item.get('priceStep') or ((10 ** (-int(contract_item.get('pricePrecision')))) if str(contract_item.get('pricePrecision', '')).isdigit() else None),
                'quantityPrecision': contract_item.get('quantityPrecision'),
                'pricePrecision': contract_item.get('pricePrecision'),
                'raw': {
                    'symbol': item,
                    'contract': contract_item,
                },
            }
            for alias in (symbol, normalized_symbol, display_name, asset_name):
                alias_key = str(alias or '').strip()
                if alias_key:
                    limits[alias_key] = limit_payload
                    alias_normalized = alias_key.replace('-', '').replace('_', '').replace('.P', '').replace('/', '').upper()
                    if alias_normalized:
                        limits[alias_normalized] = limit_payload
        broker_cfg['_lastLookupDebug'] = {
            'broker': 'bingx',
            'endpoint': 'v3:/openApi/swap/v3/quote/symbols + v2:/openApi/swap/v2/quote/contracts',
            'httpStatus': {'v3': symbols_v3.get('_http_status'), 'v2': contracts_v2.get('_http_status')},
            'baseUrl': {'v3': symbols_v3.get('_base_url'), 'v2': contracts_v2.get('_base_url')},
            'code': {'v3': symbols_v3.get('code'), 'v2': contracts_v2.get('code')},
            'msg': {'v3': symbols_v3.get('msg'), 'v2': contracts_v2.get('msg')},
            'requestPath': {'v3': symbols_v3.get('_request_path'), 'v2': contracts_v2.get('_request_path')},
            'requestQuery': {'v3': symbols_v3.get('_request_query'), 'v2': contracts_v2.get('_request_query')},
            'requestMode': {'v3': symbols_v3.get('_request_mode'), 'v2': contracts_v2.get('_request_mode')},
            'responseSnippet': {'v3': symbols_v3.get('_response_snippet'), 'v2': contracts_v2.get('_response_snippet')},
            'rawCount': raw_count,
            'contractCount': len(contract_rows),
            'filteredOutClosed': filtered_out_closed,
            'filteredOutInvalid': filtered_out_invalid,
            'symbolsKept': len(symbols),
            'containsGasUsdt': contains_gas,
            'sampleSymbols': sample_symbols,
            'usedFallbackContracts': not bool(symbol_rows),
        }
    elif broker == 'finam':
        import httpx
        data = httpx.get('https://tradeapi.finam.ru/api/v1/securities/', params={'board': 'RTSX'}, timeout=30).json()
        for item in (data.get('data') or data.get('securities') or data or []):
            if not isinstance(item, dict):
                continue
            code = str(item.get('code') or item.get('ticker') or '').strip()
            board = str(item.get('board') or '').strip()
            if code:
                symbols.append(f'{code}@{board}' if board and '@' not in code else code)
            if board:
                venues.append(board)
    elif broker == 'alor':
        import httpx
        data = httpx.get(f"{ALOR_API_BASE_URL.rstrip('/')}/md/v2/Securities", params={'limit': 1000}, timeout=30).json()
        for item in (data or []):
            if not isinstance(item, dict):
                continue
            symbol = str(item.get('symbol') or item.get('ticker') or '').strip()
            exchange = str(item.get('exchange') or item.get('board') or '').strip()
            if symbol:
                symbols.append(symbol)
            if exchange:
                venues.append(exchange)
    elif broker == 'schwab':
        return {'ok': True, 'details': 'lookup sync not supported yet'}
    else:
        return {'ok': False, 'details': f'unknown broker: {broker}'}

    if broker == 'bingx' and not symbols:
        existing_symbols = list(broker_cfg.get('lookupSymbols', []))
        if existing_symbols:
            symbols = existing_symbols
    broker_cfg['lookupSymbols'] = _dedupe_keep_order(list(broker_cfg.get('lookupSymbols', [])) + symbols)
    broker_cfg['lookupVenues'] = _dedupe_keep_order(list(broker_cfg.get('lookupVenues', [])) + venues)
    active_symbols = []
    updated_routes = 0
    for route in (config.get('routes') or []):
        for dest in (route.get('destinations') or []):
            if str(dest.get('broker') or '').strip().lower() != broker:
                continue
            symbol = str(dest.get('symbol') or '').strip()
            if symbol:
                active_symbols.append(symbol)
            limits_entry = _lookup_symbol_limits(broker_cfg, symbol)
            if limits_entry:
                dest['limits'] = copy.deepcopy(limits_entry)
                updated_routes += 1
    active_symbols = _dedupe_keep_order(active_symbols)
    active_with_limits = []
    for symbol in active_symbols:
        limits_entry = _lookup_symbol_limits(broker_cfg, symbol)
        if limits_entry:
            active_with_limits.append(symbol)
    save_config(config)
    result_payload = {
        'ok': True,
        'details': f"lookup synced: symbols={len(symbols)} venues={len(venues)} totalSymbols={len(broker_cfg.get('lookupSymbols', []))} active={len(active_symbols)} activeWithLimits={len(active_with_limits)} updatedRoutes={updated_routes}",
        'symbolsCount': len(symbols),
        'venuesCount': len(venues),
        'totalSymbols': len(broker_cfg.get('lookupSymbols', [])),
        'activeSymbols': active_symbols,
        'activeWithLimits': active_with_limits,
        'updatedRoutes': updated_routes,
    }
    lookup_debug = broker_cfg.get('_lastLookupDebug')
    if isinstance(lookup_debug, dict):
        result_payload['debug'] = lookup_debug
        result_payload['details'] += f" | rawCount={lookup_debug.get('rawCount')} kept={lookup_debug.get('symbolsKept')} gas={lookup_debug.get('containsGasUsdt')}"
    return result_payload


def _broker_connection_test(broker: str) -> Dict[str, Any]:
    broker = str(broker or '').strip().lower()
    if broker == 'bybit':
        try:
            bybit = BybitBroker(testnet=False)
            api_info = bybit.get_api_info()
            ok = api_info.get('retCode') == 0
            text = str(api_info.get('retMsg') or api_info.get('retCode') or 'unknown')
            parts = []
            if api_info.get('_http_status') is not None:
                parts.append(f"http={api_info.get('_http_status')}")
            if api_info.get('retCode') is not None:
                parts.append(f"retCode={api_info.get('retCode')}")
            if api_info.get('retMsg') not in (None, ''):
                parts.append(f"retMsg={api_info.get('retMsg')}")
            if api_info.get('raw'):
                parts.append(f"raw={str(api_info.get('raw'))[:240]}")
            if api_info.get('_bind_interface'):
                parts.append(f"iface={api_info.get('_bind_interface')}")
            details = ' | '.join(parts) or text
            return {'ok': ok, 'text': text, 'details': details}
        except Exception as e:
            return {'ok': False, 'text': str(e), 'details': str(e)}
    if broker == 'bingx':
        try:
            config = load_config()
            bingx_cfg = (config.get('brokers', {}).get('bingx', {}) or {}).get('defaultDestination', {}) or {}
            bingx = BingXBroker(testnet=bool(bingx_cfg.get('testnet', False)))
            balance = bingx.get_balance()
            ok = balance.get('code') == 0
            text = 'ok' if ok else str(balance.get('msg') or balance.get('code') or 'error')
            balance_rows = balance.get('data') or []
            first_balance = balance_rows[0] if isinstance(balance_rows, list) and balance_rows else {}
            details_parts = []
            if balance.get('_http_status') is not None:
                details_parts.append(f"http={balance.get('_http_status')}")
            if balance.get('code') is not None:
                details_parts.append(f"code={balance.get('code')}")
            if balance.get('msg') not in (None, ''):
                details_parts.append(f"msg={balance.get('msg')}")
            if first_balance:
                details_parts.append(f"asset={first_balance.get('asset')}")
                details_parts.append(f"equity={first_balance.get('equity')}")
            if balance.get('_bind_interface'):
                details_parts.append(f"iface={balance.get('_bind_interface')}")
            return {'ok': ok, 'text': text, 'details': ' | '.join(part for part in details_parts if part) or text}
        except Exception as e:
            return {'ok': False, 'text': str(e), 'details': str(e)}
    if broker == 'finam':
        try:
            finam_result = finam_auth_check_sync()
            ok = bool(finam_result.get('ok'))
            text = 'ok' if ok else str(finam_result.get('error') or 'error')
            details_parts = [f"account={FINAM_ACCOUNT_ID}"]
            if ok:
                account = finam_result.get('account') or {}
                positions = account.get('positions', []) or []
                details_parts.append(f"positions={len(positions)}")
            else:
                details_parts.append(f"error={text}")
            return {'ok': ok, 'text': text, 'details': ' | '.join(details_parts)}
        except Exception as e:
            return {'ok': False, 'text': str(e), 'details': str(e)}
    if broker == 'schwab':
        try:
            schwab = SchwabBroker()
            schwab_result = schwab.auth_check()
            ok = bool(schwab_result.get('ok'))
            text = 'ok' if ok else str(schwab_result.get('error') or schwab_result.get('status_code') or 'error')
            details_parts = []
            if schwab_result.get('status_code') is not None:
                details_parts.append(f"status={schwab_result.get('status_code')}")
            if text:
                details_parts.append(f"result={text}")
            return {'ok': ok, 'text': text, 'details': ' | '.join(details_parts) or text}
        except Exception as e:
            return {'ok': False, 'text': str(e), 'details': str(e)}
    if broker == 'alor':
        try:
            if ALOR_CONFIG_PATH.exists():
                alor_cfg = json.loads(ALOR_CONFIG_PATH.read_text())
                access_token = asyncio.run(_alor_get_access_token(alor_cfg.get('refresh_token')))
                positions = asyncio.run(_alor_get_positions(alor_cfg.get('client_id'), access_token)) or []
                ok = isinstance(positions, list)
                text = f'ok ({len(positions)} positions)' if ok else 'positions_not_list'
                details = f"client_id={alor_cfg.get('client_id')} | positions={len(positions) if isinstance(positions, list) else 'n/a'}"
                return {'ok': ok, 'text': text, 'details': details}
            return {'ok': False, 'text': 'config not found', 'details': f'path={ALOR_CONFIG_PATH}'}
        except Exception as e:
            return {'ok': False, 'text': str(e), 'details': str(e)}
    return {'ok': False, 'text': 'unknown broker', 'details': f'unknown broker: {broker}'}


def _read_text_secret(path_value: str) -> str:
    path = Path(path_value).expanduser()
    if not path.exists():
        return ''
    return path.read_text()


def _read_json_secret(path_value: str) -> str:
    path = Path(path_value).expanduser()
    if not path.exists():
        return ''
    try:
        return json.dumps(json.loads(path.read_text()), ensure_ascii=False, indent=2)
    except Exception:
        return path.read_text()


def _is_protected_path(path: str) -> bool:
    return path.startswith('/admin') or path == '/' or path.startswith('/settings') or path.startswith('/api/')


def _parse_cookie(header_value: str) -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    for part in (header_value or '').split(';'):
        if '=' not in part:
            continue
        key, value = part.split('=', 1)
        cookies[key.strip()] = value.strip()
    return cookies


def _admin_password() -> str:
    return _env_values().get('WEBHOOK_ROUTER_ADMIN_PASSWORD', '')


def _password_hash(password: str, salt: str = '') -> str:
    salt_value = salt or secrets.token_hex(16)
    digest = hashlib.sha256((salt_value + '|' + str(password or '')).encode('utf-8')).hexdigest()
    return f'{salt_value}${digest}'


def _password_matches(password: str, stored_hash: str) -> bool:
    raw = str(stored_hash or '')
    if '$' not in raw:
        return secrets.compare_digest(str(password or ''), raw)
    salt, _sep, _digest = raw.partition('$')
    return secrets.compare_digest(_password_hash(password, salt), raw)


def _empty_permissions() -> Dict[str, Any]:
    return {
        'brokers': [],
        'settingsSections': [],
        'canAddTickers': False,
        'canAssign': False,
        'canManageUsers': False,
        'canManageBackups': False,
        'canDownloadBackups': False,
        'canEmailBackups': False,
        'canViewJournal': True,
        'canEditMappings': False,
        'canQuickOrder': False,
    }


def _load_user_store() -> Dict[str, Any]:
    data = load_json(USER_DB_PATH, {})
    roles = copy.deepcopy(DEFAULT_ROLES)
    custom_roles = data.get('roles') or {}
    for role_name, role_payload in custom_roles.items():
        base = copy.deepcopy(DEFAULT_ROLES.get(role_name) or {'label': role_name.title(), 'permissions': _empty_permissions()})
        base['label'] = str((role_payload or {}).get('label') or base.get('label') or role_name.title())
        perms = base.setdefault('permissions', _empty_permissions())
        perms.update((role_payload or {}).get('permissions') or {})
        roles[role_name] = base
    users = data.get('users') or []
    admin_password = _admin_password()
    if not users and admin_password:
        users = [{
            'username': 'admin',
            'passwordHash': _password_hash(admin_password),
            'role': 'admin',
            'permissions': {},
            'disabled': False,
            'createdAt': _utcnow_iso(),
        }]
        save_json(USER_DB_PATH, {'roles': roles, 'users': users})
    return {'roles': roles, 'users': users}


def _save_user_store(store: Dict[str, Any]) -> None:
    USER_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    save_json(USER_DB_PATH, store)


def _find_user(username: str) -> Dict[str, Any]:
    username = str(username or '').strip()
    store = _load_user_store()
    for user in store.get('users') or []:
        if str(user.get('username') or '').strip() == username:
            return copy.deepcopy(user)
    return {}


def _merge_permissions(role_name: str, custom: Dict[str, Any], roles: Dict[str, Any] = None) -> Dict[str, Any]:
    roles = roles or _load_user_store().get('roles') or {}
    role_permissions = copy.deepcopy(((roles.get(role_name) or {}).get('permissions')) or _empty_permissions())
    role_permissions.update(custom or {})
    role_permissions['brokers'] = _dedupe_keep_order(role_permissions.get('brokers') or [])
    role_permissions['settingsSections'] = _dedupe_keep_order(role_permissions.get('settingsSections') or [])
    return role_permissions


def _is_superuser(user: Dict[str, Any]) -> bool:
    return str((user or {}).get('role') or '') == ROLE_ADMIN


def _permission_allows_list(values: List[str], target: str) -> bool:
    normalized = [str(v or '').strip().lower() for v in (values or []) if str(v or '').strip()]
    if '*' in normalized:
        return True
    return str(target or '').strip().lower() in normalized


def _current_user(headers) -> Dict[str, Any]:
    token = _parse_cookie(headers.get('Cookie', '')).get(USER_SESSION_COOKIE, '')
    session = USER_SESSIONS.get(token) or {}
    if not token or not session:
        return {}
    if float(session.get('expiresAt') or 0) < time.time():
        USER_SESSIONS.pop(token, None)
        return {}
    user = _find_user(session.get('username') or '')
    if not user or user.get('disabled'):
        USER_SESSIONS.pop(token, None)
        return {}
    store = _load_user_store()
    user['effectivePermissions'] = _merge_permissions(str(user.get('role') or ''), user.get('permissions') or {}, store.get('roles') or {})
    return user


def _create_user_session(username: str) -> str:
    token = secrets.token_urlsafe(32)
    USER_SESSIONS[token] = {'username': username, 'expiresAt': time.time() + ADMIN_SESSION_TTL_SECONDS}
    return token


def _can_access_broker(user: Dict[str, Any], broker_name: str) -> bool:
    if _is_superuser(user):
        return True
    return _permission_allows_list((user.get('effectivePermissions') or {}).get('brokers') or [], broker_name)


def _can_access_section(user: Dict[str, Any], section: str) -> bool:
    if _is_superuser(user):
        return True
    return _permission_allows_list((user.get('effectivePermissions') or {}).get('settingsSections') or [], section)


def _has_permission(user: Dict[str, Any], permission_key: str) -> bool:
    if _is_superuser(user):
        return True
    return bool((user.get('effectivePermissions') or {}).get(permission_key))


def _available_brokers(config: Dict[str, Any]) -> List[str]:
    return sorted([name for name, cfg in (config.get('brokers') or {}).items() if cfg.get('enabled', False)])


def _settings_sections() -> List[str]:
    return sorted({str(field.get('section') or '') for field in SETTINGS_FIELDS if str(field.get('section') or '')})


def _display_path(root_value: str, raw_path: str) -> str:
    if not raw_path:
        return ''
    try:
        root = Path(root_value).expanduser().resolve()
        path = Path(raw_path).expanduser().resolve()
        return str(path.relative_to(root))
    except Exception:
        return str(raw_path)


def _resolve_path_from_root(root_value: str, value: str) -> str:
    value = str(value or '').strip()
    if not value:
        return ''
    path = Path(value).expanduser()
    if path.is_absolute():
        return str(path)
    return str((Path(root_value).expanduser() / path).resolve())


def _normalize_env_value(field: Dict[str, Any], raw_value: str, root_value: str) -> str:
    field_type = field.get('type', 'text')
    value = str(raw_value or '').strip()
    if field_type == 'path':
        return _resolve_path_from_root(root_value, value)
    if field_type == 'bool':
        if not value:
            value = str(field.get('default', 'false')).strip()
        lowered = value.lower()
        return 'true' if lowered in ('1', 'true', 'yes', 'on') else 'false'
    return value


def _render_env_input(field: Dict[str, Any], env_values: Dict[str, str], root_value: str) -> str:
    key = field['key']
    label = field.get('label', key)
    field_type = field.get('type', 'text')
    value = env_values.get(key, str(field.get('default', '')))
    if field_type == 'path':
        value = _display_path(root_value, value)
    elif field_type == 'bool':
        if str(value).strip() == '':
            value = str(field.get('default', 'false'))
        value = 'true' if str(value).strip().lower() in ('1', 'true', 'yes', 'on') else 'false'
    input_type = 'password' if field_type == 'password' else 'text'
    return f"<label class='settings-field'><span>{html.escape(label)}</span><input class='mini-input' type='{input_type}' name='env|{html.escape(key)}' value='{html.escape(value)}'></label>"


def _is_admin_authenticated(headers) -> bool:
    password = _admin_password()
    store = _load_user_store()
    if not password and not (store.get('users') or []):
        return True
    return bool(_current_user(headers))


def _create_admin_session() -> str:
    return _create_user_session('admin')


def _render_login_page(error: str = '') -> str:
    error_html = f"<div class='flash-status error'>{html.escape(error)}</div>" if error else "<div class='flash-status'></div>"
    return f"""
<!doctype html>
<html lang='ru'>
<head>
  <meta charset='utf-8'>
  <title>Webhook Router Login</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 16px; background: #111827; color: #f3f4f6; display:flex; min-height:100vh; align-items:center; justify-content:center; }}
    .panel {{ width: 360px; background: #1f2937; padding: 16px; border-radius: 12px; }}
    .mini-input {{ width:100%; box-sizing:border-box; margin: 8px 0 12px; padding: 8px 10px; border-radius: 8px; border: 1px solid #4b5563; background:#17191d; color:#eceff3; }}
    button {{ background: #2563eb; color: white; border: 0; border-radius: 8px; padding: 8px 12px; cursor: pointer; }}
    .flash-status.error {{ color:#fca5a5; font-size:12px; margin-top:8px; }}
  </style>
</head>
<body>
  <form class='panel' method='post' action='/login'>
    <h2 style='margin-top:0'>Admin login</h2>
    <div style='font-size:12px;color:#9ca3af'>Введите пароль админки</div>
    <input class='mini-input' type='text' name='username' placeholder='username' value='admin' autofocus>
    <input class='mini-input' type='password' name='password' placeholder='password'>
    <button type='submit'>Login</button>
    <div style='font-size:11px;color:#6b7280;margin-top:10px;'>build {html.escape(_build_summary().get('version','unknown'))} / server.py {html.escape(_build_summary().get('serverHash',''))}</div>
    {error_html}
  </form>
</body>
</html>
"""


def _render_settings_page(saved: bool = False, error: str = '', message: str = '', tests: Dict[str, Dict[str, Any]] = None, user: Dict[str, Any] = None) -> str:
    env_values = _ordered_env_values()
    tests = tests or {}
    backups = _list_backups()
    root_value = env_values.get('WEBHOOK_ROUTER_ROOT', str(ROOT))
    user = user or {}
    flash = ''
    if error:
        flash = f"<div class='flash-status error'>{html.escape(error)}</div>"
    elif message:
        flash = f"<div class='flash-status ok'>{html.escape(message)}</div>"
    elif saved:
        flash = "<div class='flash-status ok'>settings saved</div>"
    else:
        flash = "<div class='flash-status'></div>"

    finam_secret_text = _read_text_secret(env_values.get('FINAM_SECRET_PATH', '')) if env_values.get('FINAM_SECRET_PATH') else ''
    alor_json_text = _read_json_secret(env_values.get('ALOR_CONFIG_PATH', '')) if env_values.get('ALOR_CONFIG_PATH') else ''
    schwab_json_text = _read_json_secret(env_values.get('SCHWAB_CONFIG_PATH', '')) if env_values.get('SCHWAB_CONFIG_PATH') else ''

    def broker_status(name: str) -> str:
        info = tests.get(name) or BROKER_TEST_CACHE.get(name) or {}
        if not info:
            return "<span class='status-dot-mini status-dot-neutral'></span><span class='help'>не проверялся</span>"
        color_class = 'status-dot-green' if info.get('ok') else 'status-dot-red'
        extra = html.escape(str(info.get('text') or ''))
        return f"<span class='status-dot-mini {color_class}'></span><span class='help'>{extra}</span>"

    def broker_header(name: str) -> str:
        return f"<div class='broker-sync-head'><div class='broker-sync-title'>{html.escape(name.title())}</div><button type='submit' formaction='/settings/test-broker' formmethod='post' name='broker' value='{html.escape(name)}'>sync</button><div class='broker-sync-status'>{broker_status(name)}</div></div>"

    can_manage_backups = _has_permission(user, 'canManageBackups')
    can_download_backups = _has_permission(user, 'canDownloadBackups')
    backup_rows = ''.join(
        f"<div class='backup-row'><code>{html.escape(item['name'])}</code><span class='help'>{html.escape(', '.join(item.get('files', [])))}</span>"
        + (f"<form method='post' action='/settings/backup/restore' style='display:inline'><input type='hidden' name='name' value='{html.escape(item['name'])}'><button type='submit'>restore</button></form>" if can_manage_backups else '')
        + (f"<form method='post' action='/settings/backup/delete' style='display:inline'><input type='hidden' name='name' value='{html.escape(item['name'])}'><button type='submit'>delete</button></form>" if can_manage_backups else '')
        + (f"<a class='backup-link' href='/settings/backup/download?name={html.escape(item['name'])}'>download .tar.gz</a>" if can_download_backups else '')
        + "</div>"
        for item in backups
    ) or "<div class='help'>Пока нет backup'ов</div>"
    backup_actions = ''
    if can_manage_backups:
        backup_actions = """
          <form method='post' action='/settings/backup/create' style='margin-bottom:10px; display:flex; gap:10px; align-items:center;'>
            <input class='mini-input' type='text' name='label' value='manual' placeholder='label' style='max-width:180px;'>
            <button type='submit'>Create backup</button>
          </form>
        """
    backup_section = f"""
        <div class='section' style='margin-top:14px;'>
          <h3>Backups</h3>
          {backup_actions}
          {backup_rows}
          <div class='help'>Backup'и автоматически ограничиваются по keep count, чтобы не захламлять диск.</div>
        </div>
    """

    section_fields = lambda section: ''.join(_render_env_input(field, env_values, root_value) for field in SETTINGS_FIELDS if field.get('section') == section and _can_access_section(user, str(field.get('section') or '')))
    webhook_url = (env_values.get('WEBHOOK_ROUTER_PUBLIC_BASE_URL') or f'http://{env_values.get("WEBHOOK_ROUTER_HOST","127.0.0.1")}:{env_values.get("WEBHOOK_ROUTER_PORT","8787")}').rstrip('/') + '/webhook'
    webhook_examples = [
        ('1) адрес hook сервера', webhook_url),
        ('2) старый режим: buy', '{\n  "sourceTicker": "SPYUSDT.P",\n  "side": "buy",\n  "qty": 1\n}'),
        ('3) старый режим: sell', '{\n  "sourceTicker": "SPYUSDT.P",\n  "side": "sell",\n  "qty": 1\n}'),
        ('4) новый режим: 2long', '{\n  "sourceTicker": "SPYUSDT.P",\n  "side": "2long"\n}'),
        ('5) новый режим: 2short', '{\n  "sourceTicker": "SPYUSDT.P",\n  "side": "2short"\n}'),
    ]
    webhook_examples_html = ''.join(
        f"<label class='settings-field'><span>{html.escape(label)}</span><textarea readonly onclick='this.select()' style='min-height:{'64px' if idx == 0 else '112px'}'>{html.escape(value)}</textarea></label>"
        for idx, (label, value) in enumerate(webhook_examples)
    )

    return f"""
<!doctype html>
<html lang='ru'>
<head>
  <meta charset='utf-8'>
  <title>Webhook Router Settings</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 16px; background: #111827; color: #f3f4f6; }}
    .panel {{ background: #1f2937; padding: 14px; border-radius: 12px; max-width: 1100px; margin: 0 auto; }}
    .topbar {{ display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:12px; }}
    .navlinks {{ display:flex; gap:10px; align-items:center; }}
    .navlinks a {{ color:#93c5fd; text-decoration:none; }}
    .grid {{ display:grid; grid-template-columns: 1fr 1fr; gap:14px; }}
    .section {{ background:#111827; border:1px solid #374151; border-radius:10px; padding:12px; }}
    .section h3 {{ margin:0 0 10px; font-size:16px; }}
    .settings-field {{ display:flex; flex-direction:column; gap:6px; margin-bottom:10px; font-size:12px; color:#cbd5e1; }}
    .mini-input, textarea {{ width:100%; box-sizing:border-box; padding:8px 10px; border-radius:8px; border:1px solid #4b5563; background:#17191d; color:#eceff3; font-size:12px; }}
    textarea {{ min-height:160px; resize:vertical; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
    button {{ background: #2563eb; color: white; border: 0; border-radius: 8px; padding: 8px 12px; cursor: pointer; }}
    .flash-status {{ min-height:18px; margin:8px 0 14px; font-size:12px; }}
    .flash-status.error {{ color:#fca5a5; }}
    .flash-status.ok {{ color:#86efac; }}
    .help {{ color:#9ca3af; font-size:11px; margin-top:4px; }}
    .backup-row {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; padding:6px 0; border-top:1px solid #1f2937; }}
    .backup-row:first-child {{ border-top:0; }}
    .broker-sync-head {{ display:flex; align-items:center; gap:10px; margin-bottom:10px; flex-wrap:wrap; }}
    .broker-sync-title {{ font-size:16px; font-weight:600; }}
    .broker-sync-status {{ display:flex; align-items:center; gap:6px; }}
    .status-dot-mini {{ width:10px; height:10px; border-radius:999px; display:inline-block; border:1px solid rgba(255,255,255,0.18); }}
    .status-dot-green {{ background:#16a34a; }}
    .status-dot-red {{ background:#dc2626; }}
    .status-dot-neutral {{ background:#6b7280; }}
  </style>
</head>
<body>
  <div class='panel'>
    <div class='topbar'>
      <div>
        <h2 style='margin:0'>Settings</h2>
        <div class='help'>Все ключи, пути и секреты редактируются здесь и сохраняются в файлы.</div>
        <div class='help' style='margin-top:6px;'>Webhook URL для TradingView: <code>{html.escape((env_values.get('WEBHOOK_ROUTER_PUBLIC_BASE_URL') or f'http://{env_values.get("WEBHOOK_ROUTER_HOST","127.0.0.1")}:{env_values.get("WEBHOOK_ROUTER_PORT","8787")}').rstrip('/') + '/webhook')}</code></div>
        <div class='help'>build {html.escape(_build_summary().get('version','unknown'))} / server.py {html.escape(_build_summary().get('serverHash',''))}</div>
      </div>
      <div class='navlinks'><a href='/'>admin</a><a href='/effectiveness'>effectiveness</a><a href='/journal'>journal</a><a href='/logout'>logout</a></div>
    </div>
    {flash}
    <div class='section' style='margin-bottom:14px;'>
      <h3>TradingView webhook examples</h3>
      <div class='help' style='margin-bottom:10px;'>Скопируй нужный URL/JSON. Верхние примеры для старого режима buy/sell, нижние для нового режима 2long/2short.</div>
      {webhook_examples_html}
    </div>
    <form method='post' action='/settings/save'>
      <div class='grid'>
        <div class='section'>
          <h3>Admin / server</h3>
          {section_fields('admin')}
        </div>
        <div class='section'>
          <h3>Paths</h3>
          {section_fields('paths')}
        </div>
        <div class='section'>
          {broker_header('bybit')}
          {section_fields('bybit')}
        </div>
        <div class='section'>
          {broker_header('bingx')}
          {section_fields('bingx')}
        </div>
        <div class='section'>
          {broker_header('alor')}
          {section_fields('alor')}
          <label class='settings-field'><span>Alor config JSON</span><textarea name='secret|ALOR_CONFIG_JSON'>{html.escape(alor_json_text)}</textarea><div class='help'>Содержимое файла по пути ALOR_CONFIG_PATH</div></label>
        </div>
        <div class='section'>
          {broker_header('finam')}
          {section_fields('finam')}
          <label class='settings-field'><span>Finam token / secret file</span><textarea name='secret|FINAM_SECRET'>{html.escape(finam_secret_text)}</textarea><div class='help'>Содержимое файла по пути FINAM_SECRET_PATH</div></label>
        </div>
        <div class='section'>
          {broker_header('schwab')}
          {section_fields('schwab')}
          <label class='settings-field'><span>Schwab config JSON</span><textarea name='secret|SCHWAB_CONFIG_JSON'>{html.escape(schwab_json_text)}</textarea><div class='help'>Содержимое файла по пути SCHWAB_CONFIG_PATH</div></label>
        </div>
      </div>
      <div style='margin-top:14px; display:flex; gap:10px; align-items:center;'>
        <button type='submit'>Save settings</button>
        <a href='/' style='color:#93c5fd;text-decoration:none;'>← back</a>
      </div>
    </form>
    {backup_section}
  </div>
</body>
</html>
"""


def save_json(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def _journal_dir() -> Path:
    path = JOURNAL_LOG_PATH
    if path.suffix:
        return path.parent
    return path


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def _journal_week_key(ts: datetime = None) -> str:
    ts = ts or datetime.now(timezone.utc)
    year, week, _ = ts.isocalendar()
    return f"{year}-W{int(week):02d}"


def _display_local_time(value: Any) -> str:
    raw = str(value or '').strip()
    if not raw:
        return ''
    try:
        if raw.endswith('Z'):
            dt = datetime.fromisoformat(raw[:-1] + '+00:00')
        else:
            dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime('%d-%m-%Y %H:%M:%S %Z')
    except Exception:
        return raw


def _short_json(value: Any, limit: int = 400) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    except Exception:
        text = str(value)
    return text[:limit]


def _sanitize_journal_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(k): _sanitize_journal_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_journal_value(v) for v in value]
    return value


def _short_text(value: Any, limit: int = 400) -> str:
    text = str(value or '')
    return text[:limit]


def _journal_week_file(ts: datetime = None) -> Path:
    return _journal_dir() / f"journal-{_journal_week_key(ts)}.jsonl"


def _journal_keep_weeks() -> int:
    values = _env_values()
    raw = values.get('WEBHOOK_ROUTER_JOURNAL_KEEP_WEEKS', str(JOURNAL_KEEP_WEEKS))
    try:
        return max(1, int(raw))
    except Exception:
        return max(1, JOURNAL_KEEP_WEEKS)


def _compress_old_journals() -> None:
    current_name = _journal_week_file().name
    journal_dir = _journal_dir()
    journal_dir.mkdir(parents=True, exist_ok=True)
    for path in journal_dir.glob('journal-*.jsonl'):
        if path.name == current_name:
            continue
        gz_path = path.with_suffix(path.suffix + '.gz')
        if gz_path.exists():
            path.unlink(missing_ok=True)
            continue
        with path.open('rb') as src, gzip.open(gz_path, 'wb') as dst:
            dst.write(src.read())
        path.unlink(missing_ok=True)

    keep = _journal_keep_weeks()
    files = sorted(list(journal_dir.glob('journal-*.jsonl')) + list(journal_dir.glob('journal-*.jsonl.gz')), key=lambda p: p.name, reverse=True)
    for old in files[keep:]:
        old.unlink(missing_ok=True)


def append_journal(entry: Dict[str, Any]) -> None:
    journal_dir = _journal_dir()
    journal_dir.mkdir(parents=True, exist_ok=True)
    _compress_old_journals()
    path = _journal_week_file()
    sanitized = _sanitize_journal_value(entry)
    with path.open('a') as f:
        f.write(json.dumps(sanitized, ensure_ascii=False, default=str) + '\n')


def _route_destinations(materialized: Any) -> List[Dict[str, Any]]:
    if isinstance(materialized, dict):
        destinations = materialized.get('destinations', [])
        return [item for item in destinations if isinstance(item, dict)]
    if isinstance(materialized, list):
        return [item for item in materialized if isinstance(item, dict)]
    return []


def _result_error_text(result_obj: Any) -> str:
    if isinstance(result_obj, dict):
        direct_error = str(result_obj.get('error') or '')
        if direct_error:
            return direct_error
        ret_code = result_obj.get('retCode')
        if ret_code not in (None, 0, '0'):
            return str(result_obj.get('retMsg') or ret_code)
        code = result_obj.get('code')
        if code not in (None, 0, '0'):
            return str(result_obj.get('msg') or code)
        nested = result_obj.get('results')
        if isinstance(nested, dict) and nested is not result_obj:
            nested_error = _result_error_text(nested)
            if nested_error:
                return nested_error
        return ''
    if isinstance(result_obj, list):
        errors = []
        for item in result_obj:
            if not isinstance(item, dict):
                continue
            item_error = _result_error_text(item)
            if item_error:
                errors.append(str(item_error))
        return ' || '.join(item for item in errors if item)
    return ''


def _result_is_dry_run(result_obj: Any) -> bool:
    if isinstance(result_obj, dict):
        return bool(result_obj.get('dryRun'))
    if isinstance(result_obj, list):
        return any(isinstance(item, dict) and item.get('dryRun') for item in result_obj)
    return False


def _safe_append_journal(entry: Dict[str, Any]) -> None:
    try:
        append_journal(entry)
    except Exception:
        try:
            LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            fallback = {
                'time': _utcnow_iso(),
                'kind': 'journal-write-error',
                'status': 'error',
                'details': traceback.format_exc()[:2000],
                'entry': entry,
            }
            with LOG_PATH.open('a') as f:
                f.write(json.dumps(fallback, ensure_ascii=False, default=str) + '\n')
        except Exception:
            pass


def _quick_order_response_summary(result: Dict[str, Any]) -> str:
    try:
        return _short_json(result, 600)
    except Exception:
        return _short_text(result, 600)


def _render_effectiveness_page() -> str:
    return """<!doctype html><html lang='ru'><head><meta charset='utf-8'><title>Effectiveness</title><style>
body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;margin:16px;background:#111827;color:#f3f4f6}
a{color:#93c5fd;text-decoration:none}
.panel{background:#1f2937;padding:14px;border-radius:12px;max-width:1280px;margin:0 auto 14px auto}
.nav a{margin-right:12px}
.muted{color:#9ca3af;font-size:12px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}
.card{background:#111827;border:1px solid #374151;border-radius:10px;padding:12px}
.k{font-size:12px;color:#9ca3af;margin-bottom:4px}.v{font-size:22px;font-weight:700}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{border-bottom:1px solid #374151;padding:8px;text-align:left;vertical-align:top}
code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.ok{color:#86efac}.bad{color:#fca5a5}
</style></head><body>
<div class='panel'><div class='nav'><a href='/'>admin</a><a href='/settings'>settings</a><a href='/journal'>journal</a><a href='/logout'>logout</a></div><h2 style='margin-bottom:6px;'>Эффективность сделок</h2><div class='muted'>Страница читает только готовые данные из SQLite, без пересчёта истории на старте сервера и без rebuild аналитики при открытии. Все времена на этой странице показаны в MSK.</div></div>
<div class='panel'><div id='status' class='muted'>Загрузка…</div><div id='meta' class='grid' style='margin-top:12px;'></div></div>
<div class='panel'><h3 style='margin-top:0;'>Последний сигнал / исполнение</h3><div id='latest' class='grid'></div></div>
<div class='panel'><h3 style='margin-top:0;'>Последние fills</h3><div class='muted' style='margin-bottom:8px;'>`fill qty` показывает фактический размер исполнения. Для BingX swap он отображается кратко как `cts`, чтобы не дублировать длинное имя инструмента из колонки `symbol`. `request size` и `sizing basis` показывают, чем был задан размер сигнала, например `10 usdt` для open и `0.35 contracts` для close. Комиссия концептуально считается от notional (`qty × price`), но источником истины остаются фактические fee/income данные из API биржи.</div><div id='fills'></div></div>
<div class='panel'><h3 style='margin-top:0;'>Последние round-trips</h3><div id='roundtrips'></div></div>
<div class='panel'><h3 style='margin-top:0;'>Готовые дневные агрегаты</h3><div id='daily'></div></div>
<script>
function esc(v){return String(v ?? '').replace(/[&<>"']/g, s => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]));}
function fmtNum(v, digits=2){const n=Number(v); if(!Number.isFinite(n)) return esc(v ?? ''); return n.toLocaleString('ru-RU',{maximumFractionDigits:digits});}
function fmtTime(v){
  if(v === null || v === undefined || v === '') return '';
  const d = new Date(v);
  if(Number.isNaN(d.getTime())) return esc(v);
  const parts = new Intl.DateTimeFormat('ru-RU', {
    timeZone: 'Europe/Moscow',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(d);
  const map = Object.fromEntries(parts.map(p => [p.type, p.value]));
  return `${esc(map.day || '')}-${esc(map.month || '')}-${esc(map.year || '')} ${esc(map.hour || '')}:${esc(map.minute || '')}:${esc(map.second || '')} MSK`;
}
function renderTable(columns, rows){ if(!rows || !rows.length) return "<div class='muted'>Пока пусто</div>"; const head=columns.map(c=>`<th>${esc(c.label)}</th>`).join(''); const body=rows.map(r=>`<tr>${columns.map(c=>`<td>${c.render ? c.render(r[c.key], r) : esc(r[c.key] ?? '')}</td>`).join('')}</tr>`).join(''); return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`; }
async function load(){
  const status=document.getElementById('status');
  try{
    const res=await fetch('/api/effectiveness-overview',{headers:{'Accept':'application/json'}});
    const data=await res.json();
    if(!res.ok){throw new Error(data.error || ('HTTP '+res.status));}
    status.innerHTML=`<span class='ok'>OK</span> · db: <code>${esc(data.dbPath)}</code> · size: ${fmtNum((data.dbSizeBytes||0)/1024,1)} KB`;
    const counters=data.counters || {};
    const meta=[
      ['signals','Signals'],['executions','Executions'],['orders','Orders'],['fills','Fills'],['round_trips','Round-trips']
    ].map(([k,label])=>`<div class='card'><div class='k'>${esc(label)}</div><div class='v'>${fmtNum(counters[k]||0,0)}</div></div>`).join('');
    document.getElementById('meta').innerHTML=meta;
    const latestSignal=data.latestSignal || {};
    const latestExecution=data.latestExecution || {};
    document.getElementById('latest').innerHTML=`
      <div class='card'><div class='k'>Последний signal</div><div><code>${esc(latestSignal.signal_id || '')}</code></div><div>${fmtTime(latestSignal.received_at || '')}</div><div>${esc(latestSignal.source_ticker || '')} · ${esc(latestSignal.side || '')} · ${esc(latestSignal.qty_text || '')}</div></div>
      <div class='card'><div class='k'>Последний execution</div><div><code>${esc(latestExecution.execution_id || '')}</code></div><div>${fmtTime(latestExecution.received_at || '')}</div><div>${esc(latestExecution.broker || '')} · ${esc(latestExecution.symbol || '')} · ${esc(latestExecution.status || '')}</div><div class='muted'>${esc(latestExecution.error_text || '')}</div></div>`;
    document.getElementById('fills').innerHTML=renderTable([
      {key:'observed_at',label:'time',render:v=>fmtTime(v)}, {key:'broker',label:'broker'}, {key:'symbol',label:'symbol'}, {key:'phaseLabel',label:'phase'}, {key:'side',label:'side'},
      {key:'requestSize',label:'request size'}, {key:'sizingBasis',label:'sizing basis'},
      {key:'qty',label:'fill qty',render:(v,r)=>`${fmtNum(v,6)} ${esc(r.fillQtyUnit || '')}`},
      {key:'price',label:'price',render:(v,r)=>`${fmtNum(v,6)} ${esc(r.priceUnit || '')}`},
      {key:'notional',label:'notional',render:(v,r)=>`${fmtNum(v,4)} ${esc(r.notionalUnit || '')}`},
      {key:'commission',label:'fee',render:(v,r)=>`${fmtNum(v,6)} ${esc(r.feeUnit || '')}<div class='muted'>base: ${esc(r.feeBasis || '')}</div>`},
      {key:'signal_id',label:'signal_id',render:v=>`<code>${esc(v||'')}</code>`}
    ], data.latestFills || []);
    document.getElementById('roundtrips').innerHTML=renderTable([
      {key:'closed_at',label:'closed_at',render:v=>fmtTime(v)}, {key:'broker',label:'broker'}, {key:'symbol',label:'symbol'}, {key:'direction',label:'dir'},
      {key:'entry_qty',label:'qty',render:(v,r)=>`${fmtNum(v,6)} ${esc(r.qtyUnit || '')}`},
      {key:'gross_pnl',label:'gross',render:(v,r)=>`${fmtNum(v,4)} ${esc(r.pnlUnit || '')}`},
      {key:'commission_total',label:'fee',render:(v,r)=>`${fmtNum(v,4)} ${esc(r.pnlUnit || '')}`},
      {key:'net_pnl',label:'net',render:(v,r)=>`<span class='${Number(v)>=0?'ok':'bad'}'>${fmtNum(v,4)} ${esc(r.pnlUnit || '')}</span>`},
      {key:'opening_signal_id',label:'open_signal',render:v=>`<code>${esc(v||'')}</code>`}, {key:'closing_signal_id',label:'close_signal',render:v=>`<code>${esc(v||'')}</code>`}
    ], data.latestRoundTrips || []);
    document.getElementById('daily').innerHTML=renderTable([
      {key:'trade_day',label:'day'}, {key:'broker',label:'broker'}, {key:'symbol',label:'symbol'}, {key:'lot_bucket',label:'bucket'},
      {key:'trades_count',label:'trades',render:v=>fmtNum(v,0)}, {key:'gross_pnl_sum',label:'gross (quote)',render:v=>fmtNum(v,4)},
      {key:'commission_sum',label:'fee (quote)',render:v=>fmtNum(v,4)}, {key:'net_pnl_sum',label:'net (quote)',render:v=>fmtNum(v,4)}
    ], data.latestDailyStats || []);
  }catch(err){ status.innerHTML=`<span class='bad'>Ошибка</span> · ${esc(err.message || err)}`; }
}
load();
</script></body></html>"""


def _extract_bingx_risk_details(destination: Dict[str, Any]) -> str:
    if str(destination.get('broker') or '') != 'bingx':
        return ''
    request = destination.get('request') or {}
    result = destination.get('results') or {}
    parts = []
    stage = request.get('stage') or result.get('stage')
    stage_trace = request.get('stageTrace') or result.get('stageTrace') or []
    if stage:
        parts.append(f"stage={stage}")
    if stage_trace:
        parts.append(f"stageTrace={'>'.join([str(x) for x in stage_trace])}")

    if str(request.get('signalMode') or '').lower() == 'target-direction':
        target_direction = request.get('targetDirection')
        if target_direction:
            parts.append(f"targetDirection={target_direction}")
        netting_action = request.get('nettingAction')
        if netting_action:
            parts.append(f"nettingAction={netting_action}")
        close_still_open = request.get('targetDirectionCloseStillOpenQty')
        if close_still_open not in (None, ''):
            parts.append(f"closeStillOpenQty={close_still_open} contracts")
        open_qty_kind = request.get('targetOpenQtyKind')
        if open_qty_kind:
            parts.append(f"targetOpenQtyKind={open_qty_kind}")
        final_status = request.get('finalOrderStatus')
        if final_status:
            parts.append(f"finalOrderStatus={final_status}")
        close_attempts = request.get('targetDirectionCloseAttempts') or []
        if isinstance(close_attempts, list) and close_attempts:
            rendered = []
            for item in close_attempts[:8]:
                if not isinstance(item, dict):
                    continue
                rendered.append(
                    f"#{item.get('pass')}:pos={item.get('remainingPositionQty')} contracts,ord={item.get('closeOrderRemainingQty','')} contracts"
                )
            if rendered:
                parts.append('targetClosePasses=' + ','.join(rendered))
        open_attempts = request.get('targetOpenAttempts') or []
        if isinstance(open_attempts, list) and open_attempts:
            rendered_open = []
            for item in open_attempts[:6]:
                if not isinstance(item, dict):
                    continue
                rendered_open.append(
                    f"#{item.get('attempt')}:qty={item.get('placedQty')},status={item.get('finalStatus')},rem={item.get('remainingQty')}"
                )
            if rendered_open:
                parts.append('targetOpenPasses=' + ','.join(rendered_open))

    risk = request.get('riskControl') or {}
    if isinstance(risk, dict) and risk:
        for key in (
            'equity',
            'allowedLoss',
            'beforeQty',
            'incomingQty',
            'expectedFinalQty',
            'finalQty',
            'expectedNotional',
            'finalNotional',
            'preTradeLeverage',
            'currentMarginValue',
            'currentMarginPctOfEquity',
            'targetMargin',
            'targetMarginPctOfEquity',
            'addMargin',
            'addMarginPctOfEquity',
            'liquidationPrice',
        ):
            value = risk.get(key)
            if value not in (None, ''):
                parts.append(f"{key}={value}")
    return ' | '.join(parts)


def _append_multi_destination_journal(status: str, received_at: str, payload: Dict[str, Any], materialized: Any, details_builder) -> None:
    destinations = _route_destinations(materialized)
    if len(destinations) <= 1:
        destination = destinations[0] if destinations else {}
        append_journal({
            'time': received_at,
            'kind': 'webhook',
            'ticker': payload.get('sourceTicker'),
            'side': payload.get('side'),
            'qty': payload.get('qty'),
            'qtyUnit': _payload_qty_unit(payload),
            'brokers': [destination.get('broker')] if destination.get('broker') else [],
            'symbol': destination.get('symbol', ''),
            'venue': destination.get('category') or destination.get('exchange') or destination.get('account') or '',
            'status': status,
            'details': details_builder(destination),
        })
        return

    for destination in destinations:
        append_journal({
            'time': received_at,
            'kind': 'webhook-destination',
            'ticker': payload.get('sourceTicker'),
            'side': payload.get('side'),
            'qty': payload.get('qty'),
            'qtyUnit': _payload_qty_unit(payload),
            'brokers': [destination.get('broker')] if destination.get('broker') else [],
            'symbol': destination.get('symbol', ''),
            'venue': destination.get('category') or destination.get('exchange') or destination.get('account') or '',
            'status': status,
            'details': details_builder(destination),
        })


def _journal_files() -> List[Path]:
    journal_dir = _journal_dir()
    journal_dir.mkdir(parents=True, exist_ok=True)
    files = list(journal_dir.glob('journal-*.jsonl')) + list(journal_dir.glob('journal-*.jsonl.gz'))
    return sorted(files, key=lambda p: p.name, reverse=True)


def load_journal(limit: int = 300, week: str = '', broker: str = '', ticker: str = '', status: str = '', kind: str = '', sort: str = 'desc') -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    files = _journal_files()
    if sort == 'asc':
        files = list(reversed(files))
    for path in files:
        if week and week not in path.name:
            continue
        if len(items) >= limit:
            break
        try:
            if path.suffix == '.gz':
                raw_lines = gzip.open(path, 'rt').read().splitlines()
            else:
                raw_lines = path.read_text().splitlines()
            if sort != 'asc':
                raw_lines = list(reversed(raw_lines))
            for line in raw_lines:
                if len(items) >= limit:
                    break
                try:
                    record = json.loads(line)
                    record['_file'] = path.name
                    if broker and broker not in [str(x).lower() for x in (record.get('brokers') or [])]:
                        continue
                    if ticker and ticker.lower() not in str(record.get('ticker') or '').lower():
                        continue
                    if status and status.lower() not in str(record.get('status') or '').lower():
                        continue
                    if kind and kind.lower() not in str(record.get('kind') or '').lower():
                        continue
                    items.append(record)
                except Exception:
                    continue
        except Exception:
            continue
    return items


def _render_journal_page(week: str = '', broker: str = '', ticker: str = '', status: str = '', kind: str = '', page: int = 1, sort: str = 'desc') -> str:
    all_items = load_journal(2000, week=week, broker=broker, ticker=ticker, status=status, kind=kind, sort=sort)
    files = _journal_files()
    per_page = 100
    total = len(all_items)
    page = max(1, page)
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    items = all_items[start_idx:end_idx]
    rows = []
    for item in items:
        item_kind_raw = str(item.get('kind') or '')
        item_status_raw = str(item.get('status') or '')
        row_classes = ['journal-row']
        if item_kind_raw == 'server-start':
            row_classes.append('journal-row-server-start')
        if item_status_raw in ('error', 'execution_error', 'partial_error', 'invalid_json', 'missing_fields', 'route_not_found'):
            row_classes.append('journal-row-error')
        ts = html.escape(_display_local_time(item.get('time') or item.get('receivedAt') or ''))
        kind_label = html.escape(item_kind_raw)
        ticker_label = html.escape(str(item.get('ticker') or item.get('sourceTicker') or ''))
        side_label = html.escape(str(item.get('side') or ''))
        qty_value = str(item.get('qty') or '')
        qty_unit = str(item.get('qtyUnit') or '').strip()
        qty_label = html.escape(_qty_with_unit(qty_value, qty_unit))
        brokers_label = html.escape(', '.join(item.get('brokers') or []))
        status_label = html.escape(item_status_raw)
        version = str(item.get('version') or '')
        server_hash = str(item.get('serverHash') or '')
        details_raw = str(item.get('details') or item.get('changedKeys') or '')
        if item_kind_raw == 'server-start':
            extras = []
            if version:
                extras.append(f"version={version}")
            if server_hash:
                extras.append(f"serverHash={server_hash}")
            if extras:
                details_raw = (' | '.join(extras) + (' | ' + details_raw if details_raw else ''))
        details_raw = details_raw.replace(' | request=', '\nrequest=')
        details_raw = details_raw.replace(' | result=', '\nresult=')
        details_raw = details_raw.replace(' | error=', '\nerror=')
        details_raw = details_raw.replace(' | route=', '\nroute=')
        details_raw = details_raw.replace(' | exec=', '\nexec=')
        details_raw = details_raw.replace(' | body=', '\nbody=')
        details_raw = details_raw.replace(' | payload=', '\npayload=')
        details = html.escape(details_raw).replace('\n', '<br>')
        source_file = html.escape(str(item.get('_file') or ''))
        rows.append(
            f"<tr class='{' '.join(row_classes)}'><td>{ts}</td><td>{kind_label}</td><td>{ticker_label}</td><td>{side_label}</td><td>{qty_label}</td><td>{brokers_label}</td><td>{status_label}</td><td class='details-cell'>{details}</td><td>{source_file}</td></tr>"
        )
    body_rows = ''.join(rows) or "<tr><td colspan='9'>Журнал пока пуст</td></tr>"
    file_list = ''.join(f"<li>{html.escape(p.name)}</li>" for p in files) or '<li>пока нет файлов</li>'
    week_values = []
    for p in files:
        value = p.name.replace('journal-','').replace('.jsonl.gz','').replace('.jsonl','')
        if value not in week_values:
            week_values.append(value)
    week_options = ''.join(f"<option value='{html.escape(v)}' {'selected' if week == v else ''}>{html.escape(v)}</option>" for v in week_values)
    broker_options = ''.join(f"<option value='{name}' {'selected' if broker == name else ''}>{name}</option>" for name in ['alor','bybit','bingx','finam','schwab'])
    status_options = ''.join(f"<option value='{name}' {'selected' if status == name else ''}>{name}</option>" for name in ['accepted','placed','executed','execution_error','partial_error','route_not_found','invalid_json','missing_fields','ok','error'])
    kind_options = ''.join(f"<option value='{name}' {'selected' if kind == name else ''}>{name}</option>" for name in ['webhook','quick-order','settings-save','settings-sync','server-start'])
    prev_link = f"/journal?week={html.escape(week)}&broker={html.escape(broker)}&ticker={html.escape(ticker)}&status={html.escape(status)}&kind={html.escape(kind)}&sort={html.escape(sort)}&page={page-1}" if page > 1 else ''
    next_link = f"/journal?week={html.escape(week)}&broker={html.escape(broker)}&ticker={html.escape(ticker)}&status={html.escape(status)}&kind={html.escape(kind)}&sort={html.escape(sort)}&page={page+1}" if end_idx < total else ''
    download_link = f"/journal/download?week={html.escape(week or _journal_week_key())}"
    return f"""
<!doctype html>
<html lang='ru'>
<head>
  <meta charset='utf-8'>
  <title>Webhook Router Journal</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 16px; background: #111827; color: #f3f4f6; }}
    .panel {{ background: #1f2937; padding: 14px; border-radius: 12px; max-width: 1200px; margin: 0 auto; }}
    .nav a {{ color:#93c5fd; text-decoration:none; margin-right:12px; }}
    table {{ width:100%; border-collapse:collapse; margin-top:12px; }}
    th, td {{ border-bottom:1px solid #374151; padding:8px; text-align:left; font-size:12px; vertical-align:top; }}
    th {{ color:#cbd5e1; }}
    .muted {{ color:#9ca3af; font-size:11px; }}
    ul {{ margin:8px 0 0 18px; color:#9ca3af; font-size:12px; }}
    .journal-row {{ transition: background-color 0.12s ease; }}
    .journal-row-server-start td {{ background: rgba(168, 85, 247, 0.10); border-top: 1px solid rgba(216, 180, 254, 0.30); border-bottom: 1px solid rgba(216, 180, 254, 0.16); }}
    .journal-row-server-start td:first-child {{ box-shadow: inset 3px 0 0 rgba(216, 180, 254, 0.55); }}
    .journal-row-error td {{ background: rgba(239, 68, 68, 0.08); }}
    .journal-row-server-start.journal-row-error td {{ background: linear-gradient(90deg, rgba(59, 130, 246, 0.12), rgba(239, 68, 68, 0.08)); }}
  </style>
</head>
<body>
  <div class='panel'>
    <div class='nav'><a href='/'>admin</a><a href='/settings'>settings</a><a href='/journal'>journal</a><a href='/logout'>logout</a></div>
    <h2 style='margin-bottom:4px;'>Journal</h2>
    <div class='muted'>Журнал по неделям. Новая неделя → новый файл. Старые недели автоматически сжимаются в .gz и удаляются по лимиту недель хранения.</div>
    <form method='get' action='/journal' style='margin-top:10px; display:flex; gap:8px; flex-wrap:wrap; align-items:center;'>
      <select name='week'><option value=''>all weeks</option>{week_options}</select>
      <select name='broker'><option value=''>all brokers</option>{broker_options}</select>
      <input type='text' name='ticker' value='{html.escape(ticker)}' placeholder='ticker'>
      <select name='status'><option value=''>all statuses</option>{status_options}</select>
      <select name='kind'><option value=''>all kinds</option>{kind_options}</select>
      <select name='sort'><option value='desc' {'selected' if sort == 'desc' else ''}>newest</option><option value='asc' {'selected' if sort == 'asc' else ''}>oldest</option></select>
      <button type='submit'>Filter</button>
      <a href='/journal' style='color:#93c5fd;text-decoration:none;'>reset</a>
      <a href='{download_link}' style='color:#93c5fd;text-decoration:none;'>download week</a>
    </form>
    <div class='muted' style='margin-top:8px;'>Файлы:</div>
    <ul>{file_list}</ul>
    <div class='muted' style='margin-top:8px;'>Всего записей: {total}. Страница: {page}.</div>
    <table>
      <thead><tr><th>time</th><th>kind</th><th>ticker</th><th>side</th><th>qty</th><th>brokers</th><th>status</th><th>details</th><th>file</th></tr></thead>
      <tbody>{body_rows}</tbody>
    </table>
    <div style='margin-top:10px; display:flex; gap:12px;'>
      {f"<a href='{prev_link}' style='color:#93c5fd;text-decoration:none;'>← prev</a>" if prev_link else ''}
      {f"<a href='{next_link}' style='color:#93c5fd;text-decoration:none;'>next →</a>" if next_link else ''}
    </div>
  </div>
</body>
</html>
"""


def load_config():
    raw = load_json(CONFIG_PATH, {'version': 1, 'brokers': {}, 'defaultExecution': {}, 'routes': []})
    return _ensure_supported_broker_defaults(raw)


def save_config(config: Dict[str, Any]):
    save_json(CONFIG_PATH, _ensure_supported_broker_defaults(config))


def load_observed_signals():
    return load_json(OBSERVED_SIGNALS_PATH, {'tickers': {}})


def load_instruments():
    return load_json(INSTRUMENTS_PATH, {'version': 1, 'instruments': []})


def save_observed_signals(data: Dict[str, Any]):
    save_json(OBSERVED_SIGNALS_PATH, data)


def _upsert_observed_signal(observed: Dict[str, Any], payload: Dict[str, Any], increment_count: bool = True) -> bool:
    ticker = payload.get('sourceTicker')
    if not ticker:
        return False

    tickers = observed.setdefault('tickers', {})
    is_new = ticker not in tickers
    entry = tickers.setdefault(ticker, {
        'sourceTicker': ticker,
        'firstSeen': datetime.utcnow().isoformat() + 'Z',
        'lastSeen': None,
        'count': 0,
        'sides': [],
        'lastPayload': {},
    })

    side = payload.get('side')
    if side and side not in entry['sides']:
        entry['sides'].append(side)

    entry['lastSeen'] = datetime.utcnow().isoformat() + 'Z'
    if increment_count:
        entry['count'] += 1
    entry['lastPayload'] = payload
    return is_new


def register_observed_signal(payload: Dict[str, Any]):
    with OBSERVED_SIGNALS_LOCK:
        observed = load_observed_signals()
        changed = _upsert_observed_signal(observed, payload)
        if changed or payload.get('sourceTicker'):
            save_observed_signals(observed)


def _register_live_position_symbols(metrics: Dict[str, Any]) -> List[str]:
    with OBSERVED_SIGNALS_LOCK:
        observed = load_observed_signals()
        new_tickers: List[str] = []
        changed = False
        for broker_name, broker_payload in metrics.items():
            if broker_name.startswith('_'):
                continue
            for symbol, symbol_payload in (broker_payload.get('symbols') or {}).items():
                qty = symbol_payload.get('qty')
                try:
                    qty_num = float(qty)
                except Exception:
                    qty_num = 0.0
                payload = {
                    'sourceTicker': symbol,
                    'side': 'sell' if qty_num < 0 else 'buy',
                    'qty': abs(qty_num) if qty_num else 0,
                    'source': 'broker-sync',
                    'broker': broker_name,
                }
                is_new = _upsert_observed_signal(observed, payload, increment_count=False)
                changed = True
                if is_new:
                    new_tickers.append(symbol)
        if changed:
            save_observed_signals(observed)
        return new_tickers


def _normalize(value: Any):
    return str(value).strip().lower() if value is not None else None


def _lookup(context: Dict[str, Any], path: str):
    current = context
    for part in path.split('.'):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _matches_value(actual: Any, expected: Any) -> bool:
    if expected in (None, '*'):
        return True
    if isinstance(expected, list):
        return any(_matches_value(actual, item) for item in expected)
    if isinstance(expected, bool):
        return actual is expected
    if isinstance(expected, (int, float)):
        try:
            return float(actual) == float(expected)
        except Exception:
            return False
    return _normalize(actual) == _normalize(expected)


def route_matches(payload: Dict[str, Any], route: Dict[str, Any]) -> bool:
    match = route.get('match') or {
        'sourceTicker': route.get('sourceTicker'),
        'side': route.get('side', '*'),
    }
    for key, expected in match.items():
        if not _matches_value(payload.get(key), expected):
            return False
    return True


def match_route(payload: Dict[str, Any], config: Dict[str, Any]):
    for route in config.get('routes', []):
        if route.get('enabled', True) is False:
            continue
        if route_matches(payload, route):
            return route
    return None


def _render_template(value: Any, context: Dict[str, Any]):
    if isinstance(value, str):
        def replace(match):
            resolved = _lookup(context, match.group(1))
            return '' if resolved is None else str(resolved)
        return TEMPLATE_RE.sub(replace, value)
    if isinstance(value, list):
        return [_render_template(item, context) for item in value]
    if isinstance(value, dict):
        return {k: _render_template(v, context) for k, v in value.items()}
    return value


def _payload_implies_target_direction(payload: Dict[str, Any]) -> bool:
    side = str(payload.get('side') or '').strip().lower()
    return side in ('2long', '2short', 'long', 'short')


def _symbol_units(symbol: str) -> tuple[str, str]:
    raw = str(symbol or '').strip().upper()
    if '-' in raw:
        base, quote = raw.split('-', 1)
        return base or 'ASSET', quote or 'QUOTE'
    if raw.endswith('USDT') and len(raw) > 4:
        return raw[:-4] or 'ASSET', 'USDT'
    return raw or 'ASSET', 'QUOTE'


def _payload_qty_unit(payload: Dict[str, Any]) -> str:
    payload_target_mode = _payload_implies_target_direction(payload)
    default_unit = 'usdt' if payload_target_mode else 'contracts'
    return str(payload.get('qtyKind') or payload.get('sizeKind') or default_unit).strip().lower()


def _request_qty_unit(request: Dict[str, Any]) -> str:
    signal_mode = str(request.get('signalMode') or '').strip().lower()
    qty_kind = str(request.get('qtyKind') or '').strip().lower()
    open_qty_kind = str(request.get('openQtyKind') or '').strip().lower()
    if signal_mode == 'target-direction':
        return open_qty_kind or qty_kind or 'usdt'
    return qty_kind or 'contracts'


def _request_sizing_basis(request: Dict[str, Any]) -> str:
    signal_mode = str(request.get('signalMode') or '').strip().lower()
    if signal_mode == 'target-direction':
        open_unit = str(request.get('openQtyKind') or request.get('qtyKind') or 'usdt').strip().lower() or 'usdt'
        return f'target-direction(close=contracts, open={open_unit})'
    unit = _request_qty_unit(request)
    return unit or 'unknown'


def _qty_with_unit(value: Any, unit: str) -> str:
    text = str(value if value is not None else '').strip()
    unit_text = str(unit or '').strip()
    if text and unit_text:
        return f'{text} {unit_text}'
    return text or unit_text


def _journal_units_hint(request_like: Dict[str, Any], symbol: str = '') -> str:
    if not isinstance(request_like, dict):
        return ''
    parts = []
    request_size = _qty_with_unit(request_like.get('qty'), _request_qty_unit(request_like))
    if request_size:
        parts.append(f'requestSize={request_size}')
    sizing_basis = _request_sizing_basis(request_like)
    if sizing_basis:
        parts.append(f'sizingBasis={sizing_basis}')
    base_unit, quote_unit = _symbol_units(symbol or str(request_like.get('symbol') or ''))
    if base_unit:
        parts.append(f'fillQtyUnit={base_unit}')
    if quote_unit:
        parts.append(f'quoteUnit={quote_unit}')
    return ' | '.join(parts)


def _resolve_qty(payload: Dict[str, Any], destination: Dict[str, Any]) -> Any:
    mode = destination.get('qtyMode', 'pass-through')
    payload_qty = payload.get('qty')
    payload_target_mode = _payload_implies_target_direction(payload)
    payload_default_qty_kind = 'usdt' if payload_target_mode else 'contracts'
    payload_qty_kind = str(payload.get('qtyKind') or payload.get('sizeKind') or payload_default_qty_kind).strip().lower()
    destination_qty_kind = str(destination.get('qtyKind') or destination.get('sizeKind') or payload_qty_kind or payload_default_qty_kind).strip().lower()

    if mode == 'fixed':
        return destination.get('qty')
    if mode == 'multiplier':
        multiplier = destination.get('qtyMultiplier', 1)
        return float(payload_qty) * float(multiplier)
    if mode == 'pass-through':
        if destination_qty_kind != payload_qty_kind:
            destination['qtyResolutionError'] = f'qty kind mismatch: payload={payload_qty_kind}, destination={destination_qty_kind}'
        return payload_qty
    return payload_qty


def materialize_route(payload: Dict[str, Any], route: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    execution_defaults = config.get('defaultExecution', {})
    materialized_destinations: List[Dict[str, Any]] = []

    for destination in route.get('destinations', []):
        merged = {
            **execution_defaults,
            **destination,
        }
        context = {
            'payload': payload,
            **payload,
            'route': route,
            'destination': merged,
        }
        rendered = _render_template(merged, context)
        payload_target_mode = _payload_implies_target_direction(payload)
        if payload_target_mode and not str(rendered.get('signalMode') or '').strip():
            rendered['signalMode'] = 'target-direction'
        elif payload_target_mode and str(rendered.get('signalMode') or '').strip().lower() == 'step-side':
            rendered['signalMode'] = 'target-direction'
        rendered['qty'] = _resolve_qty(payload, rendered)
        default_qty_kind = 'usdt' if (payload_target_mode or str(rendered.get('signalMode') or payload.get('signalMode') or '').strip().lower() == 'target-direction') else 'contracts'
        rendered['qtyKind'] = str(rendered.get('qtyKind') or payload.get('qtyKind') or payload.get('sizeKind') or default_qty_kind).strip().lower()
        if (payload_target_mode or str(rendered.get('signalMode') or '').strip().lower() == 'target-direction') and not rendered.get('openQtyKind'):
            rendered['openQtyKind'] = 'usdt'
        if 'side' not in rendered:
            rendered['side'] = payload.get('side')
        materialized_destinations.append(rendered)

    return {
        'id': route.get('id'),
        'name': route.get('name'),
        'match': route.get('match') or {
            'sourceTicker': route.get('sourceTicker'),
            'side': route.get('side', '*'),
        },
        'destinations': materialized_destinations,
    }


def _safe_route_id(ticker: str) -> str:
    return 'ui-' + re.sub(r'[^a-zA-Z0-9]+', '-', ticker.strip()).strip('-').lower()


def _extract_tickers_from_route(route: Dict[str, Any]) -> List[str]:
    match = route.get('match') or {}
    source = match.get('sourceTicker', [])
    if isinstance(source, list):
        return [str(item) for item in source]
    if source:
        return [str(source)]
    return []


def _get_ui_route_for_ticker(config: Dict[str, Any], ticker: str) -> Dict[str, Any]:
    route_id = _safe_route_id(ticker)
    for route in config.get('routes', []):
        if route.get('id') == route_id and route.get('uiManaged'):
            return route
    return {}


def _route_has_active_destinations(route: Dict[str, Any]) -> bool:
    return bool((route or {}).get('destinations'))


def _parse_iso_ts(value: Any) -> float:
    raw = str(value or '').strip()
    if not raw:
        return 0.0
    try:
        if raw.endswith('Z'):
            dt = datetime.fromisoformat(raw[:-1] + '+00:00')
        else:
            dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _route_activity_ts(route: Dict[str, Any]) -> float:
    return max(
        _parse_iso_ts((route or {}).get('uiUpdatedAt')),
        _parse_iso_ts((route or {}).get('uiCreatedAt')),
    )


def _routing_details(route: Dict[str, Any]) -> str:
    if not route:
        return 'no active destinations'
    parts = []
    for destination in route.get('destinations', []) or []:
        broker = destination.get('broker') or '?'
        symbol = destination.get('symbol') or ''
        venue = destination.get(_broker_venue_key(broker))
        qty = destination.get('qty', '')
        piece = f"{broker}:{symbol}"
        if venue:
            piece += f"@{venue}"
        signal_mode = destination.get('signalMode')
        qty_kind = destination.get('qtyKind')
        if qty not in (None, ''):
            piece += f" qty={qty}"
        if signal_mode:
            piece += f" signalMode={signal_mode}"
        if qty_kind:
            piece += f" qtyKind={qty_kind}"
        parts.append(piece)
    return ' | '.join(parts) if parts else 'no active destinations'


def _append_routing_journal(ticker: str, before_route: Dict[str, Any], after_route: Dict[str, Any]) -> None:
    before_active = _route_has_active_destinations(before_route)
    after_active = _route_has_active_destinations(after_route)
    if not before_active and not after_active:
        action = 'removed'
    elif not before_route and after_active:
        action = 'created'
    elif before_active and after_active:
        action = 'updated'
    elif after_active:
        action = 'created'
    else:
        action = 'removed'
    append_journal({
        'time': _utcnow_iso(),
        'kind': 'routing-save',
        'ticker': ticker,
        'side': '',
        'qty': '',
        'brokers': [d.get('broker') for d in (after_route or {}).get('destinations', []) if d.get('broker')],
        'status': action,
        'details': _routing_details(after_route if after_active else before_route),
    })


def _ticker_sort_key(ticker: str, config: Dict[str, Any], observed: Dict[str, Any]):
    route = _get_ui_route_for_ticker(config, ticker)
    route_active = _route_has_active_destinations(route)
    observed_entry = (observed.get('tickers') or {}).get(ticker, {})
    last_seen_ts = _parse_iso_ts(observed_entry.get('lastSeen'))
    route_ts = _route_activity_ts(route)
    last_activity_ts = max(last_seen_ts, route_ts)
    has_any_activity = last_activity_ts > 0
    return (
        0 if route_active else 1,
        0 if has_any_activity else 1,
        -last_activity_ts,
        ticker.lower(),
    )


def _current_mapping(config: Dict[str, Any]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    mapping: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for route in config.get('routes', []):
        for ticker in _extract_tickers_from_route(route):
            ticker_bucket = mapping.setdefault(ticker, {})
            for destination in route.get('destinations', []):
                broker = destination.get('broker')
                if not broker:
                    continue
                venue_key = _broker_venue_key(broker)
                ticker_bucket[broker] = {
                    'symbol': destination.get('symbol', ''),
                    'venue': destination.get(venue_key, ''),
                    'qtyMode': destination.get('qtyMode', 'pass-through'),
                    'qty': destination.get('qty', ''),
                    'qtyMultiplier': destination.get('qtyMultiplier', ''),
                    'riskPct': destination.get('riskPct', ''),
                    'limits': copy.deepcopy(destination.get('limits', {})),
                }
    return mapping


def _current_destination(config: Dict[str, Any], ticker: str, broker_name: str) -> Dict[str, Any]:
    for route in config.get('routes', []):
        if ticker not in _extract_tickers_from_route(route):
            continue
        for destination in route.get('destinations', []):
            if destination.get('broker') == broker_name:
                return copy.deepcopy(destination)

    broker_cfg = config.get('brokers', {}).get(broker_name, {})
    destination = copy.deepcopy(broker_cfg.get('defaultDestination', {}))
    destination['broker'] = broker_name
    return destination


def _all_known_tickers(config: Dict[str, Any], observed: Dict[str, Any]) -> List[str]:
    tickers = set(observed.get('tickers', {}).keys())
    for route in config.get('routes', []):
        tickers.update(_extract_tickers_from_route(route))
    live_metrics = METRICS_CACHE.get('data') or {}
    for broker_payload in live_metrics.values():
        if isinstance(broker_payload, dict):
            tickers.update((broker_payload.get('symbols') or {}).keys())
    return sorted(tickers, key=lambda ticker: _ticker_sort_key(ticker, config, observed))


def _lookup_symbol_limits(broker_cfg: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    symbol = str(symbol or '').strip()
    if not symbol:
        return {}
    limits_map = broker_cfg.get('symbolLimits') or {}
    candidates = [
        symbol,
        symbol.replace('-', '').upper(),
        symbol.replace('_', '-').upper(),
        symbol.replace('_', '').upper(),
    ]
    for key, value in limits_map.items():
        key_text = str(key or '').strip()
        if not key_text:
            continue
        normalized = {
            key_text,
            key_text.replace('-', '').upper(),
            key_text.replace('_', '-').upper(),
            key_text.replace('_', '').upper(),
        }
        if any(candidate in normalized for candidate in candidates):
            return value or {}
    return {}


def _build_destination_for_broker(config: Dict[str, Any], broker_name: str, ticker: str, options: Dict[str, Any], instruments: Dict[str, Any] = None) -> Dict[str, Any]:
    broker_cfg = config.get('brokers', {}).get(broker_name, {})
    destination = copy.deepcopy(broker_cfg.get('defaultDestination', {}))
    destination['broker'] = broker_name
    symbol_map = broker_cfg.get('symbolMap', {})
    best_candidate = _best_catalog_candidate(ticker, broker_name, instruments or {'instruments': []})
    manual_symbol = str(options.get('symbol') or '').strip()
    if manual_symbol:
        destination['symbol'] = manual_symbol
    else:
        destination['symbol'] = best_candidate.get('symbol') or symbol_map.get(ticker, ticker)

    venue_key = _broker_venue_key(broker_name)
    venue_value = (options.get('venue') or '').strip()
    if venue_value:
        destination[venue_key] = venue_value
    elif best_candidate.get('venue'):
        destination[venue_key] = best_candidate.get('venue')

    symbol_limits = _lookup_symbol_limits(broker_cfg, destination.get('symbol'))
    if symbol_limits:
        destination['limits'] = copy.deepcopy(symbol_limits)

    risk_pct_raw = str(options.get('riskPct', '')).strip().replace('%', '')
    if broker_name == 'bingx' and risk_pct_raw:
        try:
            risk_pct_value = float(risk_pct_raw)
            if risk_pct_value > 0:
                destination['riskPct'] = risk_pct_value
                destination['marginType'] = 'ISOLATED'
        except Exception:
            pass

    qty_raw = str(options.get('qty', '')).strip()
    if qty_raw:
        try:
            qty_value = float(qty_raw)
            min_qty_raw = (destination.get('limits') or {}).get('minQty')
            if min_qty_raw not in (None, '') and qty_value < float(min_qty_raw):
                raise ValueError(f'min_qty:{min_qty_raw}')
            destination['qtyMode'] = 'fixed'
            destination['qty'] = int(qty_value) if qty_value.is_integer() else qty_value
            signal_mode = str(options.get('signalMode') or destination.get('signalMode') or '').strip().lower()
            destination['qtyKind'] = 'usdt' if signal_mode == 'target-direction' else 'contracts'
            if signal_mode == 'target-direction':
                destination['openQtyKind'] = destination['qtyKind']
            destination['sideMode'] = 'sign'
        except Exception:
            pass
    else:
        multiplier_raw = str(options.get('qtyMultiplier', '')).strip()
        if multiplier_raw:
            try:
                multiplier_value = float(multiplier_raw)
                destination['qtyMode'] = 'multiplier'
                destination['qtyMultiplier'] = multiplier_value
            except Exception:
                pass

    return destination


def _qty_badge_class(value: Any) -> str:
    raw = str(value or '').strip().upper()
    if raw == 'WORK':
        return 'qty-work'
    if raw in ('REJ', 'REJECT', 'REJECTED'):
        return 'qty-neg'
    try:
        num = float(value)
    except Exception:
        return 'qty-neutral'
    if num < 0:
        return 'qty-neg'
    if num > 0:
        return 'qty-pos'
    return 'qty-neutral'


def _find_instrument_for_ticker(ticker: str, instruments: Dict[str, Any]) -> Dict[str, Any]:
    needle = _normalize(ticker)
    for instrument in instruments.get('instruments', []):
        if _normalize(instrument.get('canonical')) == needle:
            return instrument
        for alias in instrument.get('aliases', []):
            if _normalize(alias) == needle:
                return instrument
        for alias in instrument.get('signalAliases', []):
            if _normalize(alias) == needle:
                return instrument
        for broker_data in instrument.get('brokers', {}).values():
            if _normalize(broker_data.get('symbol')) == needle:
                return instrument
    return {}


def _tokenize_lookup(value: Any) -> List[str]:
    return [part for part in re.split(r'[^A-Za-z0-9]+', str(value or '').upper()) if part]


def _instrument_lookup_names(instrument: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for key in ('canonical', 'underlying'):
        value = instrument.get(key)
        if value:
            names.append(str(value))
    for key in ('aliases', 'signalAliases'):
        for value in instrument.get(key, []) or []:
            if value:
                names.append(str(value))
    for broker_data in (instrument.get('brokers') or {}).values():
        symbol = broker_data.get('symbol')
        if symbol:
            names.append(str(symbol))
    return names


def _instrument_match_score(ticker: str, instrument: Dict[str, Any], broker_name: str) -> int:
    query = str(ticker or '').strip().upper()
    if not query:
        return 0

    broker_data = (instrument.get('brokers') or {}).get(broker_name) or {}
    if not broker_data.get('symbol'):
        return 0

    score = 0
    names = _instrument_lookup_names(instrument)
    names_upper = [str(name).strip().upper() for name in names if str(name).strip()]

    if query in names_upper:
        score += 1000
    elif any(query == name.replace('@RTSX', '') for name in names_upper):
        score += 900
    elif any(query in name for name in names_upper):
        score += 220

    query_tokens = set(_tokenize_lookup(query))
    name_tokens = set()
    for name in names_upper:
        name_tokens.update(_tokenize_lookup(name))
    score += 40 * len(query_tokens & name_tokens)

    underlying = str(instrument.get('underlying') or '').upper()
    if underlying and any(token in underlying for token in query_tokens):
        score += 120

    canonical = str(instrument.get('canonical') or '').upper()
    group = str(instrument.get('group') or '').lower()
    if query.startswith(('ES', 'NQ', 'CL', 'NG', 'GC', 'HG', 'ZW', 'ZS', 'CC', 'OJ', 'DX', 'RTY', 'YM', 'MES', 'MNQ')) and 'future' in group:
        score += 40
    if instrument.get('preferred'):
        score += 25
    if broker_data.get('proxy'):
        score -= 15
    if broker_data.get('fallbackOnly'):
        score -= 20
    if broker_data.get('manualReview'):
        score -= 10
    if query in canonical:
        score += 90

    return score


def _catalog_candidates_for_broker(ticker: str, broker_name: str, instruments: Dict[str, Any], limit: int = 8) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    seen = set()
    for instrument in instruments.get('instruments', []):
        broker_data = (instrument.get('brokers') or {}).get(broker_name) or {}
        symbol = str(broker_data.get('symbol') or '').strip()
        if not symbol:
            continue
        venue = str(broker_data.get('venue') or '').strip()
        key = (symbol, venue)
        if key in seen:
            continue
        seen.add(key)
        score = _instrument_match_score(ticker, instrument, broker_name)
        if score <= 0:
            continue
        candidates.append({
            'symbol': symbol,
            'venue': venue,
            'canonical': str(instrument.get('canonical') or ''),
            'score': score,
            'preferred': bool(instrument.get('preferred')),
            'proxy': bool(broker_data.get('proxy')),
            'fallbackOnly': bool(broker_data.get('fallbackOnly')),
            'manualReview': bool(broker_data.get('manualReview')),
            'exact': score >= 900,
        })
    candidates.sort(key=lambda item: (
        0 if item.get('exact') else 1,
        0 if item.get('manualReview') else 1,
        0 if not item.get('fallbackOnly') else 1,
        0 if not item.get('proxy') else 1,
        -int(item.get('score') or 0),
        item.get('canonical') or '',
    ))
    return candidates[:limit]


def _best_catalog_candidate(ticker: str, broker_name: str, instruments: Dict[str, Any]) -> Dict[str, Any]:
    candidates = _catalog_candidates_for_broker(ticker, broker_name, instruments, limit=1)
    return candidates[0] if candidates else {}


def _candidate_hint_text(candidate: Dict[str, Any]) -> str:
    tags = []
    if candidate.get('manualReview'):
        tags.append('manual-review')
    if candidate.get('proxy'):
        tags.append('proxy')
    if candidate.get('fallbackOnly'):
        tags.append('fallback')
    venue = candidate.get('venue') or '—'
    tail = f" · {', '.join(tags)}" if tags else ''
    return f"best: {candidate.get('symbol')} / {venue}{tail}"


def _manual_broker_info(broker_name: str, broker_cfg: Dict[str, Any]) -> Dict[str, str]:
    default_destination = broker_cfg.get('defaultDestination', {}) or {}
    if broker_name == 'alor':
        return {
            'tz': 'Europe/Moscow',
            'hours': '10:00-23:50',
            'markets': 'MOEX, FORTS',
            'target': f"target {(default_destination.get('exchange') or 'MOEX')}",
            'extra': 'live/dry-run + sync age',
        }
    if broker_name == 'finam':
        return {
            'tz': 'Europe/Moscow',
            'hours': '10:00-23:50',
            'markets': 'MOEX, RTSX',
            'target': f"target {(default_destination.get('exchange') or 'MOEX')}",
            'extra': 'live/dry-run + sync age',
        }
    if broker_name == 'schwab':
        return {
            'tz': 'America/New_York',
            'hours': '04:00-20:00',
            'markets': 'NYSE, NASDAQ, ARCA',
            'target': f"acct {(default_destination.get('account') or 'primary')} · NORMAL",
            'extra': 'premarket / regular / afterhours',
        }
    if broker_name == 'bingx':
        return {
            'tz': 'UTC',
            'hours': '00:00-24:00',
            'markets': 'perpetual swap',
            'target': f"target {(default_destination.get('category') or 'swap')}",
            'extra': '24/7 limit-only + sync age',
        }
    return {
        'tz': 'UTC',
        'hours': '00:00-24:00',
        'markets': 'spot, linear, inverse',
        'target': f"target {(default_destination.get('category') or 'linear')}",
        'extra': '24/7 live/dry-run + sync age',
    }


def _instrument_is_future(instrument: Dict[str, Any]) -> bool:
    group = str(instrument.get('group', '')).lower()
    canonical = str(instrument.get('canonical', '')).upper()
    return ('future' in group) or canonical.endswith(' FUT')


def _instrument_has_proxy(instrument: Dict[str, Any]) -> bool:
    for broker_data in instrument.get('brokers', {}).values():
        if broker_data.get('proxy') or broker_data.get('fallbackOnly'):
            return True
    return False


def _fmt_qty_text(value: Any) -> str:
    try:
        num = float(value)
    except Exception:
        return str(value or '')
    if num.is_integer():
        return str(int(num))
    text = f"{num:.8f}".rstrip('0').rstrip('.')
    return text or '0'


def _quick_order_base_qty(instrument: Dict[str, Any], broker_name: str, current: Dict[str, Any]) -> str:
    current_qty = current.get('qty')
    if current_qty not in (None, ''):
        try:
            current_abs = abs(float(current_qty))
            if current_abs > 0:
                return _fmt_qty_text(current_abs)
        except Exception:
            pass

    broker_data = (instrument or {}).get('brokers', {}).get(broker_name, {}) if instrument else {}
    for key in ('atomicQty', 'baseQty', 'orderQty', 'lotSize', 'minQty'):
        value = broker_data.get(key)
        if value not in (None, ''):
            return _fmt_qty_text(value)

    current_qty = current.get('qty')
    current_abs = None
    try:
        current_abs = abs(float(current_qty))
    except Exception:
        current_abs = None

    group = str((instrument or {}).get('group', '')).lower()
    symbol = str(current.get('symbol') or broker_data.get('symbol') or '').upper()

    if broker_name in ('alor', 'finam', 'schwab'):
        return '1'

    if broker_name in ('bybit', 'bingx'):
        if current_abs is not None and 0 < current_abs < 1:
            return _fmt_qty_text(current_abs)
        if 'crypto' in group or symbol.endswith('USDT') or symbol.endswith('-USDT'):
            return '0.01'
        return '1'

    if current_abs is not None and 0 < current_abs < 1:
        return _fmt_qty_text(current_abs)
    return '1'


def _route_atomic_qty(route: Dict[str, Any]) -> str:
    value = route.get('atomicQty')
    if value in (None, ''):
        return ''
    return _fmt_qty_text(value)


def _observed_signal_qty(observed_entry: Dict[str, Any]) -> str:
    payload = (observed_entry or {}).get('lastPayload') or {}
    if str(payload.get('source') or '').strip().lower() == 'broker-sync':
        return ''
    try:
        qty = abs(float(payload.get('qty')))
    except Exception:
        qty = 0.0
    if qty > 0:
        return _fmt_qty_text(qty)
    return ''


def _observed_broker_sync_qty(observed_entry: Dict[str, Any]) -> str:
    payload = (observed_entry or {}).get('lastPayload') or {}
    if str(payload.get('source') or '').strip().lower() != 'broker-sync':
        return ''
    try:
        qty = abs(float(payload.get('qty')))
    except Exception:
        qty = 0.0
    if qty > 0:
        return _fmt_qty_text(qty)
    return ''


def _current_broker_order_qty(instrument: Dict[str, Any], broker_name: str, current: Dict[str, Any], observed_entry: Dict[str, Any], ticker: str) -> str:
    current = current or {}
    value = current.get('qty')
    if value not in (None, ''):
        try:
            current_abs = abs(float(value))
            if current_abs > 0:
                return _fmt_qty_text(current_abs)
        except Exception:
            pass

    payload = (observed_entry or {}).get('lastPayload') or {}
    if str(payload.get('source') or '').strip().lower() == 'broker-sync':
        try:
            qty = abs(float(payload.get('qty')))
            if qty > 0:
                return _fmt_qty_text(qty)
        except Exception:
            pass

    return _quick_order_base_qty(instrument, broker_name, current)


def _current_order_qty(current: Dict[str, Any], default_qty: str) -> str:
    value = current.get('qty')
    if value in (None, ''):
        return default_qty
    try:
        return _fmt_qty_text(abs(float(value)))
    except Exception:
        return default_qty


def _live_symbol_qty_text(broker_name: str, *symbols: str) -> str:
    broker_payload = ((METRICS_CACHE.get('data') or {}).get(broker_name) or {}) if broker_name else {}
    symbol_map = broker_payload.get('symbols') or {}
    for symbol in symbols:
        if not symbol:
            continue
        symbol_payload = (symbol_map.get(symbol) or {})
        qty = symbol_payload.get('qty')
        if qty not in (None, ''):
            return _fmt_qty_text(qty)
    return ''


def _bingx_contract_lookup_options() -> List[str]:
    try:
        client = BingXBroker()
        rows = _bingx_rows(client.get_contracts())
    except Exception:
        return []
    options: List[str] = []
    seen = set()
    for item in rows:
        symbol = str(item.get('symbol') or '').strip()
        if symbol and symbol not in seen:
            seen.add(symbol)
            options.append(symbol)
    return options


def _broker_lookup_symbols(config: Dict[str, Any], broker_name: str, instruments: Dict[str, Any]) -> List[str]:
    broker_cfg = config.get('brokers', {}).get(broker_name, {})
    options = set(broker_cfg.get('lookupSymbols', []))
    symbol_map = broker_cfg.get('symbolMap', {})
    options.update(str(k) for k in symbol_map.keys())
    options.update(str(v) for v in symbol_map.values())
    for instrument in instruments.get('instruments', []):
        broker_data = instrument.get('brokers', {}).get(broker_name)
        if broker_data and broker_data.get('symbol'):
            options.add(str(broker_data.get('symbol')))
    for route in config.get('routes', []):
        for destination in route.get('destinations', []):
            if destination.get('broker') == broker_name and destination.get('symbol'):
                options.add(str(destination.get('symbol')))
    if broker_name == 'bingx':
        options.update(_bingx_contract_lookup_options())
    return sorted(options)


def _broker_lookup_venues(config: Dict[str, Any], broker_name: str, instruments: Dict[str, Any]) -> List[str]:
    broker_cfg = config.get('brokers', {}).get(broker_name, {})
    options = set(broker_cfg.get('lookupVenues', []))
    venue_key = _broker_venue_key(broker_name)
    default_destination = broker_cfg.get('defaultDestination', {})
    if default_destination.get(venue_key):
        options.add(str(default_destination.get(venue_key)))
    for instrument in instruments.get('instruments', []):
        broker_data = instrument.get('brokers', {}).get(broker_name)
        if broker_data and broker_data.get('venue'):
            options.add(str(broker_data.get('venue')))
    for route in config.get('routes', []):
        for destination in route.get('destinations', []):
            if destination.get('broker') == broker_name and destination.get(venue_key):
                options.add(str(destination.get(venue_key)))
    return sorted(options)


def _status_dot_class(status: str) -> str:
    status = (status or '').lower()
    if 'error' in status or '401' in status or 'fail' in status or 'offline' in status:
        return 'status-red'
    if 'dry' in status or 'paper' in status or 'test' in status:
        return 'status-amber'
    if 'ready' in status or 'live' in status or 'ok' in status:
        return 'status-green'
    return 'status-amber'


def _fmt_num(value: Any, decimals: int = 2) -> str:
    try:
        num = float(value)
    except Exception:
        return '—'
    return f"{num:,.{decimals}f}"


def _cleanup_broker_order_state(now_ts: float = None) -> None:
    now_ts = now_ts or time.time()
    with BROKER_ORDER_STATE_LOCK:
        for broker_name in list(BROKER_ORDER_STATE_CACHE.keys()):
            bucket = BROKER_ORDER_STATE_CACHE.get(broker_name) or {}
            for symbol in list(bucket.keys()):
                item = bucket.get(symbol) or {}
                if (now_ts - float(item.get('updatedAt') or 0.0)) > BROKER_ORDER_STATE_TTL_SECONDS:
                    bucket.pop(symbol, None)
            if not bucket:
                BROKER_ORDER_STATE_CACHE.pop(broker_name, None)


def _remember_broker_order_state(broker_name: str, symbol: str, status: str = '', order_id: Any = '', details: str = '') -> None:
    broker_name = str(broker_name or '').strip().lower()
    symbol = str(symbol or '').strip().upper()
    if not broker_name or not symbol:
        return
    status = str(status or '').strip().upper()
    if status in ('FILLED', 'EXECUTED'):
        with BROKER_ORDER_STATE_LOCK:
            (BROKER_ORDER_STATE_CACHE.get(broker_name) or {}).pop(symbol, None)
        return
    if not status and not details and not order_id:
        return
    with BROKER_ORDER_STATE_LOCK:
        bucket = BROKER_ORDER_STATE_CACHE.setdefault(broker_name, {})
        bucket[symbol] = {
            'status': status,
            'orderId': str(order_id or ''),
            'details': str(details or ''),
            'updatedAt': time.time(),
        }


def _clear_broker_order_state(broker_name: str, symbol: str) -> None:
    broker_name = str(broker_name or '').strip().lower()
    symbol = str(symbol or '').strip().upper()
    if not broker_name or not symbol:
        return
    with BROKER_ORDER_STATE_LOCK:
        bucket = BROKER_ORDER_STATE_CACHE.get(broker_name) or {}
        bucket.pop(symbol, None)
        if not bucket and broker_name in BROKER_ORDER_STATE_CACHE:
            BROKER_ORDER_STATE_CACHE.pop(broker_name, None)


def _cached_broker_order_states(broker_name: str) -> Dict[str, Dict[str, Any]]:
    _cleanup_broker_order_state()
    broker_name = str(broker_name or '').strip().lower()
    with BROKER_ORDER_STATE_LOCK:
        return copy.deepcopy(BROKER_ORDER_STATE_CACHE.get(broker_name) or {})


def _live_broker_metrics(config: Dict[str, Any]) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        'alor': {'summary': {}, 'symbols': {}},
        'bybit': {'summary': {}, 'symbols': {}},
        'bingx': {'summary': {}, 'symbols': {}},
        'finam': {'summary': {}, 'symbols': {}},
        'schwab': {'summary': {}, 'symbols': {}},
    }

    try:
        alor_cfg_path = ALOR_CONFIG_PATH
        if alor_cfg_path.exists():
            alor_cfg = json.loads(alor_cfg_path.read_text())
            access_token = asyncio.run(_alor_get_access_token(alor_cfg.get('refresh_token')))
            positions = asyncio.run(_alor_get_positions(alor_cfg.get('client_id'), access_token)) or []
            rub_cash = None
            for pos in positions:
                symbol = str(pos.get('symbol') or '')
                qty = pos.get('qty')
                if symbol == 'RUB':
                    rub_cash = qty
                    continue
                try:
                    qty_num = float(qty)
                    qty_live = int(qty_num) if qty_num.is_integer() else qty_num
                except Exception:
                    qty_live = qty
                metrics['alor']['symbols'][symbol] = {
                    'text': '',
                    'qty': qty_live,
                }
            metrics['alor']['summary'] = {
                'portfolio': '—',
                'cash': _fmt_num(rub_cash),
                'money': _fmt_num(rub_cash),
                'go': '—',
                'currency': 'RUB',
                'liveStatus': 'live-ok' if rub_cash is not None else 'offline',
            }
    except Exception:
        pass

    bybit_default = config.get('brokers', {}).get('bybit', {}).get('defaultDestination', {})
    bingx_default = config.get('brokers', {}).get('bingx', {}).get('defaultDestination', {})
    finam_default = config.get('brokers', {}).get('finam', {}).get('defaultDestination', {})
    schwab_default = config.get('brokers', {}).get('schwab', {}).get('defaultDestination', {})

    try:
        bybit = BybitBroker(testnet=bool(bybit_default.get('testnet', False)))
        wallet = bybit._request('GET', '/v5/account/wallet-balance', params={'accountType': 'UNIFIED'})
        pos = bybit._request('GET', '/v5/position/list', params={'category': 'linear', 'settleCoin': 'USDT'})
        wallet_row = (((wallet.get('result') or {}).get('list') or [{}])[0])
        total_equity = wallet_row.get('totalEquity')
        total_cash = wallet_row.get('totalWalletBalance') or wallet_row.get('totalAvailableBalance')
        total_go = wallet_row.get('totalInitialMargin') or wallet_row.get('totalInitialMarginByMp')
        if total_go in (None, ''):
            total_go_num = 0.0
            for p in ((pos.get('result') or {}).get('list') or []):
                try:
                    total_go_num += float(p.get('positionIMByMp') or p.get('positionIM') or 0)
                except Exception:
                    pass
            total_go = total_go_num
        metrics['bybit']['summary'] = {
            'portfolio': _fmt_num(total_equity),
            'cash': _fmt_num(total_cash),
            'go': _fmt_num(total_go),
            'currency': 'USDT',
            'mode': 'test' if bybit_default.get('testnet') else ('dry-run' if bybit_default.get('dryRun') else 'live'),
            'liveStatus': 'dry-run' if bybit_default.get('dryRun') else 'live-ok',
        }
        for p in ((pos.get('result') or {}).get('list') or []):
            symbol = str(p.get('symbol') or '')
            total_im = p.get('positionIMByMp') or p.get('positionIM') or p.get('positionMMByMp') or p.get('positionMM')
            size = p.get('size') or 0
            per_unit = None
            try:
                size_num = abs(float(size))
                if size_num > 0:
                    per_unit = float(total_im) / size_num
            except Exception:
                pass
            if symbol:
                try:
                    size_num = float(size)
                    qty_live = int(size_num) if size_num.is_integer() else size_num
                    if str(p.get('side') or '').lower() == 'sell':
                        qty_live = -abs(qty_live)
                except Exception:
                    qty_live = size
                metrics['bybit']['symbols'][symbol] = {
                    'totalGo': _fmt_num(total_im),
                    'perUnitGo': _fmt_num(per_unit),
                    'text': f"ГО {_fmt_num(total_im)} / {_fmt_num(per_unit)} за 1",
                    'qty': qty_live,
                }
    except Exception:
        pass

    try:
        bingx = BingXBroker(testnet=bool(bingx_default.get('testnet', False)))
        balance = bingx.get_balance()
        positions = bingx.get_positions()
        balance_data = balance.get('data') or []
        balance_row = balance_data[0] if isinstance(balance_data, list) and balance_data else {}
        asset_data = balance_row.get('balance') if isinstance(balance_row, dict) else None
        if isinstance(asset_data, dict):
            metric_source = asset_data
        elif isinstance(balance_data, dict):
            metric_source = balance_data
        else:
            metric_source = balance_row if isinstance(balance_row, dict) else {}
        portfolio_value = (
            metric_source.get('equity')
            or metric_source.get('balance')
            or metric_source.get('walletBalance')
            or metric_source.get('marginBalance')
        )
        cash_value = (
            metric_source.get('availableMargin')
            or metric_source.get('availableBalance')
            or metric_source.get('availableFunds')
            or metric_source.get('maxWithdrawAmount')
            or metric_source.get('balance')
        )
        go_value = (
            metric_source.get('usedMargin')
            or metric_source.get('occupiedMargin')
            or metric_source.get('positionMargin')
            or metric_source.get('freezedMargin')
            or metric_source.get('orderMargin')
        )
        metrics['bingx']['summary'] = {
            'portfolio': _fmt_num(portfolio_value),
            'cash': _fmt_num(cash_value),
            'go': _fmt_num(go_value),
            'currency': str(metric_source.get('asset') or balance_row.get('asset') or 'USDT'),
            'mode': 'test' if bingx_default.get('testnet') else ('dry-run' if bingx_default.get('dryRun') else 'live'),
            'liveStatus': 'dry-run' if bingx_default.get('dryRun') else 'live-ok',
        }
        for p in (positions.get('data') or []):
            symbol = str(p.get('symbol') or '')
            qty_raw = p.get('positionAmt') or 0
            total_im = p.get('initialMargin')
            per_unit = None
            try:
                qty_num_abs = abs(float(qty_raw))
                if qty_num_abs > 0:
                    per_unit = float(total_im) / qty_num_abs
            except Exception:
                pass
            if symbol:
                try:
                    qty_live = float(qty_raw)
                    qty_live = int(qty_live) if qty_live.is_integer() else qty_live
                    if str(p.get('positionSide') or '').upper() == 'SHORT':
                        qty_live = -abs(qty_live)
                except Exception:
                    qty_live = qty_raw
                metrics['bingx']['symbols'][symbol] = {
                    'totalGo': _fmt_num(total_im),
                    'perUnitGo': _fmt_num(per_unit),
                    'text': f"ГО {_fmt_num(total_im)} / {_fmt_num(per_unit)} за 1",
                    'qty': qty_live,
                }
    except Exception:
        pass

    try:
        finam = finam_auth_check_sync()
        account = finam.get('account') or {}
        forts = account.get('portfolio_forts') or {}
        portfolio_value = (account.get('equity') or {}).get('value')
        cash_value = (forts.get('available_cash') or {}).get('value')
        go_value = (forts.get('money_reserved') or {}).get('value')
        metrics['finam']['summary'] = {
            'portfolio': _fmt_num(portfolio_value),
            'cash': _fmt_num(cash_value),
            'go': _fmt_num(go_value),
            'currency': 'RUB',
            'liveStatus': 'dry-run' if finam_default.get('dryRun') else 'live-ok',
        }
        for p in account.get('positions', []) or []:
            symbol = str(p.get('symbol') or '')
            total_go = (p.get('maintenance_margin') or {}).get('value')
            qty = (p.get('quantity') or {}).get('value')
            per_unit = None
            try:
                qty_num = abs(float(qty))
                if qty_num > 0:
                    per_unit = float(total_go) / qty_num
            except Exception:
                pass
            if symbol:
                try:
                    qty_num = float(qty)
                    qty_live = int(qty_num) if qty_num.is_integer() else qty_num
                except Exception:
                    qty_live = qty
                metrics['finam']['symbols'][symbol] = {
                    'totalGo': _fmt_num(total_go),
                    'perUnitGo': _fmt_num(per_unit),
                    'text': f"ГО {_fmt_num(total_go)} / {_fmt_num(per_unit)} за 1",
                    'qty': qty_live,
                }
    except Exception:
        pass

    try:
        schwab = SchwabBroker()
        account_hash = schwab.resolve_account_hash('primary')
        resp = schwab.client.get_account(account_hash, fields=[BaseClient.Account.Fields.POSITIONS])
        data = resp.json().get('securitiesAccount', {}) if getattr(resp, 'status_code', None) == 200 else {}
        balances = data.get('currentBalances', {})
        metrics['schwab']['summary'] = {
            'portfolio': _fmt_num(balances.get('equity') or balances.get('liquidationValue')),
            'cash': _fmt_num(balances.get('cashBalance') or balances.get('availableFunds')),
            'go': _fmt_num(balances.get('maintenanceRequirement')),
            'currency': 'USD',
            'liveStatus': 'dry-run' if schwab_default.get('dryRun') else 'live-ok',
        }
        live_symbols = set()
        for p in data.get('positions', []) or []:
            instrument = p.get('instrument') or {}
            symbol = str(instrument.get('symbol') or '')
            try:
                long_qty = float(p.get('longQuantity') or 0)
                short_qty = float(p.get('shortQuantity') or 0)
                qty_live = long_qty - short_qty
                qty_live = int(qty_live) if float(qty_live).is_integer() else qty_live
            except Exception:
                qty_live = p.get('longQuantity') or p.get('shortQuantity') or 0
            market_value = p.get('marketValue')
            if symbol:
                live_symbols.add(symbol.upper())
                _clear_broker_order_state('schwab', symbol)
                metrics['schwab']['symbols'][symbol] = {
                    'text': f"MV {_fmt_num(market_value)}" if market_value not in (None, '') else '',
                    'qty': qty_live,
                }
        for symbol, order_state in _cached_broker_order_states('schwab').items():
            if symbol in live_symbols:
                continue
            order_status = str(order_state.get('status') or '').upper()
            order_id = str(order_state.get('orderId') or '')
            details = str(order_state.get('details') or '')
            text_parts = []
            if order_status:
                text_parts.append(f"order {order_status}")
            if order_id:
                text_parts.append(f"#{order_id}")
            if details and details not in ('WORK', order_status):
                text_parts.append(details)
            if order_status in WORKING_ORDER_STATUSES:
                metrics['schwab']['symbols'][symbol] = {
                    'text': ' · '.join(part for part in text_parts if part),
                    'qty': 'WORK',
                    'state': 'working',
                }
            elif order_status in ERROR_ORDER_STATUSES:
                metrics['schwab']['symbols'][symbol] = {
                    'text': ' · '.join(part for part in text_parts if part),
                    'qty': 'REJ',
                    'state': 'error',
                }
            elif order_status and order_status not in ('FILLED', 'EXECUTED'):
                metrics['schwab']['symbols'][symbol] = {
                    'text': ' · '.join(part for part in text_parts if part),
                    'qty': '',
                    'state': 'error' if order_status in FINAL_ORDER_STATUSES else 'neutral',
                }
    except Exception:
        pass

    return metrics


def _refresh_metrics_background(config: Dict[str, Any]) -> None:
    try:
        data = _live_broker_metrics(config)
        new_tickers = _register_live_position_symbols(data)
        METRICS_CACHE['data'] = data
        METRICS_CACHE['updated_at'] = time.time()
        METRICS_CACHE['new_tickers'] = new_tickers
    finally:
        METRICS_CACHE['refreshing'] = False


def _get_live_metrics_cached(config: Dict[str, Any], max_age_seconds: int = 15) -> Dict[str, Any]:
    now = time.time()
    is_stale = (now - float(METRICS_CACHE.get('updated_at') or 0.0)) > max_age_seconds
    if is_stale and not METRICS_CACHE.get('refreshing'):
        METRICS_CACHE['refreshing'] = True
        threading.Thread(target=_refresh_metrics_background, args=(copy.deepcopy(config),), daemon=True).start()
    payload = copy.deepcopy(METRICS_CACHE.get('data') or {})
    payload['_meta'] = {
        'updatedAt': METRICS_CACHE.get('updated_at') or 0.0,
        'newTickers': list(METRICS_CACHE.get('new_tickers') or []),
        'queueDepth': WEBHOOK_QUEUE.qsize(),
    }
    METRICS_CACHE['new_tickers'] = []
    return payload


def _metrics_sync_loop() -> None:
    while True:
        try:
            reload_env()
            _refresh_metrics_background(load_config())
        except Exception:
            METRICS_CACHE['refreshing'] = False
        time.sleep(METRICS_SYNC_INTERVAL_SECONDS)


async def _alor_get_access_token(refresh_token: str) -> str:
    import httpx
    base = ALOR_OAUTH_URL.rstrip('/')
    url = f"{base}/refresh?token={refresh_token}"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url)
        if resp.status_code == 200:
            return resp.json().get('AccessToken')
        raise RuntimeError(f'Alor auth failed: {resp.status_code}')


async def _alor_get_positions(client_id: str, access_token: str):
    import httpx
    base = ALOR_API_BASE_URL.rstrip('/')
    url = f"{base}/md/v2/clients/{client_id}/positions"
    headers = {'Authorization': f'Bearer {access_token}'}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code == 200:
            return resp.json()
        return []


def _merge_previous_broker_options(previous_route: Dict[str, Any], broker_options: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    merged = copy.deepcopy(broker_options or {})
    previous_destinations = {
        str(destination.get('broker') or '').strip(): destination
        for destination in (previous_route or {}).get('destinations', []) or []
        if str(destination.get('broker') or '').strip()
    }
    for broker_name, destination in previous_destinations.items():
        bucket = merged.setdefault(broker_name, {
            'enabled': False,
            'symbol': '',
            'venue': '',
            'qty': '',
            'qtyMultiplier': '',
            'riskPct': '',
            'limits': {},
        })
        if 'enabled' not in bucket:
            bucket['enabled'] = True
        bucket['symbol'] = str(bucket.get('symbol') or destination.get('symbol') or '').strip()
        venue_key = _broker_venue_key(broker_name)
        bucket['venue'] = str(bucket.get('venue') or destination.get(venue_key) or '').strip()
        bucket['qty'] = bucket.get('qty') or destination.get('qty', '')
        bucket['qtyMultiplier'] = bucket.get('qtyMultiplier') or destination.get('qtyMultiplier', '')
        bucket['riskPct'] = bucket.get('riskPct') or destination.get('riskPct', '')
        bucket['limits'] = bucket.get('limits') or copy.deepcopy(destination.get('limits', {}))
    return merged


def _build_ui_route(config: Dict[str, Any], ticker: str, broker_options: Dict[str, Dict[str, Any]], atomic_qty: str = '', previous_route: Dict[str, Any] = None) -> Dict[str, Any]:
    previous_route = previous_route or {}
    merged_options = _merge_previous_broker_options(previous_route, broker_options)
    enabled_brokers = [
        broker_name for broker_name, options in merged_options.items()
        if options.get('enabled') and config.get('brokers', {}).get(broker_name, {}).get('enabled', False)
    ]
    if not enabled_brokers:
        return {}

    instruments = load_instruments()
    destinations = [
        _build_destination_for_broker(config, broker_name, ticker, merged_options.get(broker_name, {}), instruments=instruments)
        for broker_name in enabled_brokers
    ]
    prev = previous_route or {}
    route = {
        'id': _safe_route_id(ticker),
        'name': f'UI mapping for {ticker}',
        'enabled': True,
        'uiManaged': True,
        'match': {
            'sourceTicker': [ticker],
            'side': '*'
        },
        'destinations': destinations,
        'uiCreatedAt': prev.get('uiCreatedAt') or _utcnow_iso(),
        'uiUpdatedAt': _utcnow_iso(),
    }
    atomic_qty = str(atomic_qty or '').strip()
    if atomic_qty:
        try:
            atomic_num = abs(float(atomic_qty))
            route['atomicQty'] = int(atomic_num) if atomic_num.is_integer() else atomic_num
        except Exception:
            pass
    return route


def save_mappings(config: Dict[str, Any], selections: Dict[str, Dict[str, Dict[str, Any]]], atomic_by_ticker: Dict[str, str]) -> Dict[str, Any]:
    preserved_routes = [route for route in config.get('routes', []) if not route.get('uiManaged')]
    ui_routes = []

    for ticker, broker_options in sorted(selections.items()):
        route = _build_ui_route(config, ticker, broker_options, atomic_by_ticker.get(ticker, ''))
        if route:
            ui_routes.append(route)

    config['routes'] = preserved_routes + ui_routes
    save_config(config)
    return config


def save_single_ticker_mapping(config: Dict[str, Any], ticker: str, broker_options: Dict[str, Dict[str, Any]], atomic_qty: str = '') -> Dict[str, Any]:
    routes = []
    replaced = False
    route_id = _safe_route_id(ticker)
    previous_route = _get_ui_route_for_ticker(config, ticker)
    merged_broker_options = _merge_previous_broker_options(previous_route, broker_options)
    new_route = _build_ui_route(config, ticker, merged_broker_options, atomic_qty, previous_route=previous_route)

    for route in config.get('routes', []):
        if route.get('id') == route_id and route.get('uiManaged'):
            replaced = True
            if new_route:
                routes.append(new_route)
            continue
        routes.append(route)

    if not replaced and new_route:
        routes.append(new_route)

    config['routes'] = routes
    save_config(config)
    previous_for_journal = previous_route
    if not previous_for_journal and replaced and new_route:
        previous_for_journal = {'id': route_id, 'destinations': []}
    _append_routing_journal(ticker, previous_for_journal, new_route)
    return config


def _render_admin_ui(config: Dict[str, Any], observed: Dict[str, Any], user: Dict[str, Any] = None) -> str:
    user = user or {}
    brokers = [(name, cfg) for name, cfg in config.get('brokers', {}).items() if cfg.get('enabled', False) and _can_access_broker(user, name)]
    mapping = _current_mapping(config)
    instruments = load_instruments()
    tickers = _all_known_tickers(config, observed)
    current_time = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
    can_add_tickers = _has_permission(user, 'canAddTickers')
    can_edit_mappings = _has_permission(user, 'canEditMappings')
    can_manage_users = _has_permission(user, 'canManageUsers')

    header_cells = ''.join(
        f"<th class='broker-head' data-broker-head='{html.escape(name)}'><div class='broker-title'>{html.escape(cfg.get('label', name))}<span class='status-dot {_status_dot_class(cfg.get('status', ''))}'></span>" +
        (f"<label class='test-toggle'><input type='checkbox' data-test-mode-toggle data-broker='{html.escape(name)}' {'checked' if cfg.get('defaultDestination', {}).get('testnet') else ''}> test</label>" if name == 'bybit' else "") +
        f"<button type='button' class='mini-head-btn' data-admin-broker-sync='{html.escape(name)}'>sync</button></div><div class='broker-summary-line'><span class='broker-summary broker-summary-left' data-broker-portfolio='{html.escape(name)}'>портфель —</span><span class='broker-summary broker-summary-right' data-broker-cash='{html.escape(name)}'>остаток —</span></div><div class='broker-summary-line'><span class='broker-summary broker-summary-right' data-broker-go='{html.escape(name)}'>ГО —</span></div></th>"
        for name, cfg in brokers
    )

    lookup_lists = []
    for broker_name, _broker_cfg in brokers:
        symbols = _broker_lookup_symbols(config, broker_name, instruments)
        venues = _broker_lookup_venues(config, broker_name, instruments)
        lookup_lists.append(
            f"<datalist id='lookup-symbol-{html.escape(broker_name)}'>" + ''.join(f"<option value='{html.escape(s)}'></option>" for s in symbols) + "</datalist>"
        )
        lookup_lists.append(
            f"<datalist id='lookup-venue-{html.escape(broker_name)}'>" + ''.join(f"<option value='{html.escape(v)}'></option>" for v in venues) + "</datalist>"
        )

    manual_ticker_placeholder = '__manual__'
    manual_broker_cells = []
    for broker_name, broker_cfg in brokers:
        info = _manual_broker_info(broker_name, broker_cfg)
        manual_broker_cells.append(
            f'<td class="broker-cell broker-disabled">'
            f"<div class='broker-info-card' data-broker-info='{html.escape(broker_name)}'>"
            f"<div class='broker-info-line'><span class='broker-info-label'>time</span><span class='broker-info-value' data-broker-info-time='{html.escape(broker_name)}'>—</span></div>"
            f"<div class='broker-info-line'><span class='broker-info-label'>status</span><span class='broker-info-value' data-broker-info-session='{html.escape(broker_name)}'>—</span></div>"
            f"<div class='broker-info-line'><span class='broker-info-label'>hours</span><span class='broker-info-muted'>{html.escape(info.get('hours') or '')}</span></div>"
            f"<div class='broker-info-line'><span class='broker-info-label'>markets</span><span class='broker-info-muted'>{html.escape(info.get('markets') or '')}</span></div>"
            f"<div class='broker-info-line'><span class='broker-info-label'>target</span><span class='broker-info-muted'>{html.escape(info.get('target') or '')}</span></div>"
            f"<div class='broker-info-line'><span class='broker-info-label'>mode</span><span class='broker-info-muted' data-broker-info-health='{html.escape(broker_name)}'>{html.escape(info.get('extra') or '')}</span></div>"
            '</div>'
            '</td>'
        )

    manual_save_style = " style='display:none'" if not can_edit_mappings else ''
    manual_row = (
        ('<tr class="mapping-row manual-row manual-row-disabled" data-manual-row="1" data-ticker="">' if can_add_tickers else '<tr class="mapping-row manual-row manual-row-disabled" data-manual-row="1" data-ticker="" style="display:none">')
        + f"<td class='save-cell'><button type='submit' class='row-save row-save-inline' data-row-save disabled{manual_save_style}>Save</button></td>"
        + f"<td class='ticker-cell'><div class='ticker-line'><label class='checkline-inline'><input type='checkbox' name='manualRowEnabled'></label><input autocomplete='off' class='mini-input symbol-input manual-ticker-input' type='text' name='manualTicker' value='' placeholder='new ticker'></div><div class='muted'>сначала добавь тикер в список, потом настрой брокеров справа</div></td>"
        + f"{''.join(manual_broker_cells)}"
        + '</tr>'
    )

    rows = [manual_row]
    for ticker in tickers:
        obs = observed.get('tickers', {}).get(ticker, {})
        instrument = _find_instrument_for_ticker(ticker, instruments)
        instrument_group = instrument.get('group', '') if instrument else ''
        row_mapping = mapping.get(ticker, {})
        broker_cells = []
        for broker_name, broker_cfg in brokers:
            current = row_mapping.get(broker_name, {})
            is_checked = 'checked' if current else ''
            risk_value = html.escape(str(current.get('riskPct', '')))
            best_candidate = _best_catalog_candidate(ticker, broker_name, instruments)
            symbol_raw = str(current.get('symbol') or best_candidate.get('symbol') or broker_cfg.get('symbolMap', {}).get(ticker, ticker)).strip()
            symbol_value = html.escape(symbol_raw)
            venue_default = broker_cfg.get('defaultDestination', {}).get(_broker_venue_key(broker_name), '')
            venue_value = html.escape(str(current.get('venue') or best_candidate.get('venue') or ('linear' if broker_name == 'bybit' else ('swap' if broker_name == 'bingx' else venue_default))))
            venue_placeholder = 'cat' if _broker_uses_category_venue(broker_name) else 'exch'
            margin_symbol = str(current.get('symbol') or broker_cfg.get('symbolMap', {}).get(ticker, ticker))
            broker_default_qty = _current_broker_order_qty(instrument, broker_name, current, obs, ticker)
            order_qty_value = _current_order_qty(current, broker_default_qty or '1')
            order_qty_class = _qty_badge_class(order_qty_value)
            position_qty_value = _live_symbol_qty_text(broker_name, symbol_raw, ticker, margin_symbol)
            position_qty_class = _qty_badge_class(position_qty_value)
            symbol_list_id = f"lookup-symbol-{html.escape(broker_name)}"
            venue_list_id = f"lookup-venue-{html.escape(broker_name)}"

            candidates = _catalog_candidates_for_broker(ticker, broker_name, instruments)
            top_candidate = candidates[0] if candidates else {}
            margin_hint = _candidate_hint_text(top_candidate) if top_candidate else ''
            limits = current.get('limits') or _lookup_symbol_limits(broker_cfg, symbol_raw)
            limit_parts = []
            if limits.get('minQty') not in (None, ''):
                limit_parts.append(f"minQty {limits.get('minQty')}")
            if limits.get('minUsdt') not in (None, ''):
                limit_parts.append(f"minUSDT {limits.get('minUsdt')}")
            if limits.get('qtyStep') not in (None, ''):
                limit_parts.append(f"qtyStep {limits.get('qtyStep')}")
            if limits.get('priceStep') not in (None, ''):
                limit_parts.append(f"pxStep {limits.get('priceStep')}")
            if limit_parts:
                margin_hint = ((margin_hint + ' · ') if margin_hint else '') + ', '.join(limit_parts)
            risk_line = f"<div class='quick-line'><span class='muted' style='min-width:34px;'>risk</span><input autocomplete='off' class='mini-input quick-qty' type='text' name='riskPct|{html.escape(ticker)}|{html.escape(broker_name)}' value='{risk_value}' placeholder='%'><span class='muted'>%</span></div>" if broker_name == 'bingx' else ''
            broker_cells.append(
                f'<td class="broker-cell" data-broker-cell="{html.escape(broker_name)}" data-broker-symbol="{html.escape(margin_symbol)}">'
                f"<div class='symbol-line'><label class='checkline-inline'><input type='checkbox' name='map|{html.escape(ticker)}|{html.escape(broker_name)}' {is_checked}></label>"
                f"<input list='{symbol_list_id}' autocomplete='off' class='mini-input symbol-input' type='text' name='symbol|{html.escape(ticker)}|{html.escape(broker_name)}' value='{symbol_value}' placeholder='symbol'>"
                f"<input list='{venue_list_id}' autocomplete='off' class='mini-input venue-inline' type='text' name='venue|{html.escape(ticker)}|{html.escape(broker_name)}' value='{venue_value}' placeholder='{venue_placeholder}'>"
                f"<input autocomplete='off' class='mini-input position-inline {position_qty_class}' type='text' value='{html.escape(position_qty_value)}' placeholder='pos' readonly tabindex='-1'></div>"
                f"<div class='quick-line'><button type='button' class='quick-btn buy-btn' data-quick-order data-side='buy' data-broker='{html.escape(broker_name)}'>buy</button><input autocomplete='off' class='mini-input quick-qty {order_qty_class}' data-base-qty value='{html.escape(order_qty_value or broker_default_qty or '1')}' type='text' name='qty|{html.escape(ticker)}|{html.escape(broker_name)}' value='{html.escape(order_qty_value or broker_default_qty or '1')}' placeholder='qty'><button type='button' class='quick-btn sell-btn' data-quick-order data-side='sell' data-broker='{html.escape(broker_name)}'>sell</button><button type='button' class='quick-btn book-btn' data-book-view data-broker='{html.escape(broker_name)}'>book</button><span class='cell-subhint'>{html.escape(margin_hint)}</span></div>"
                f"{risk_line}"
                f"<input type='hidden' name='qtyMultiplier|{html.escape(ticker)}|{html.escape(broker_name)}' value=''>"
                '</td>'
            )

        side_text = ', '.join(obs.get('sides', [])) if obs.get('sides') else ''
        last_seen = obs.get('lastSeen', '')
        pretty_last_seen = last_seen
        if last_seen and 'T' in last_seen:
            try:
                date_part, time_part = last_seen.replace('Z', '').split('T', 1)
                yyyy, mm, dd = date_part.split('-')
                hhmmss = time_part[:8]
                pretty_last_seen = f"{dd}-{mm}-{yyyy} {hhmmss}"
            except Exception:
                pretty_last_seen = last_seen
        meta_text = ' · '.join([part for part in [side_text, pretty_last_seen] if part])

        rows.append(
            '<tr class="mapping-row" data-ticker="' + html.escape(ticker) + '">'
            f"<td class='save-cell'><button type='submit' class='row-save row-save-inline' data-row-save disabled>Save</button></td>"
            f"<td class='ticker-cell'><input type='hidden' name='ticker' value='{html.escape(ticker)}'><div class='ticker-line'><code>{html.escape(ticker)}</code></div><div class='muted'>{html.escape(meta_text)}</div><div class='muted'>{html.escape(instrument_group)}</div></td>"            f"{''.join(broker_cells)}"
            '</tr>'
        )

    if not rows:
        rows.append(f"<tr><td colspan='{2 + len(brokers)}'>Пока нет известных тикеров. Первый webhook их заполнит.</td></tr>")

    return f"""
<!doctype html>
<html lang='ru'>
<head>
  <meta charset='utf-8'>
  <title>Webhook Router Admin</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 16px; background: #111827; color: #f3f4f6; }}
    h1 {{ margin: 0 0 8px 0; font-size: 20px; }}
    .muted {{ color: #9ca3af; font-size: 11px; }}
    .ticker-atomic-line {{ display:flex; align-items:center; gap:6px; margin:4px 0; }}
    .atomic-qty-input {{ width:72px; }}
    .toolbar {{ display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:12px; }}
    .toolbar-left {{ display:flex; flex-direction:column; gap:6px; }}
    .panel {{ background: #1f2937; padding: 12px; border-radius: 10px; margin-bottom: 12px; }}
    .flash-status {{ min-height: 16px; font-size: 11px; color: #9ca3af; }}
    .flash-status.error {{ color:#fca5a5; }}
    .flash-status.ok {{ color:#86efac; }}
    button {{ background: #2563eb; color: white; border: 0; border-radius: 8px; padding: 8px 12px; cursor: pointer; }}
    button:disabled {{ opacity: 0.45; cursor: default; }}
    table {{ border-collapse: collapse; width: 100%; background: #1f2937; table-layout: fixed; }}
    th, td {{ border: 1px solid #374151; padding: 6px; text-align: left; vertical-align: top; }}
    .details-cell {{ white-space: pre-wrap; word-break: break-word; }}
    th {{ background: #111827; position: sticky; top: 0; z-index: 1; }}
    tbody tr:nth-child(odd) td {{ background: #1f2528; }}
    tbody tr:nth-child(even) td {{ background: #242127; }}
    tbody tr:hover td {{ background: #2b3137; transition: background 120ms ease; }}
    code {{ color: #d7dde5; font-size: 12px; }}
    .save-cell {{ width: 72px; }}
    .ticker-cell {{ width: 220px; }}
    .ticker-line {{ display:flex; align-items:center; gap:8px; margin-bottom: 4px; }}
    .broker-head {{ font-size: 12px; }}
    .broker-title {{ display:flex; align-items:center; justify-content:center; gap:6px; }}
    .broker-summary-line {{ display:flex; justify-content:space-between; align-items:center; gap:8px; margin-top:2px; }}
    .broker-summary {{ font-size: 10px; color:#9ca3af; }}
    .test-toggle {{ font-size:10px; color:#9ca3af; display:flex; align-items:center; gap:4px; margin-left:6px; }}
    .mini-head-btn {{ padding:2px 6px; font-size:10px; line-height:1.1; border-radius:6px; background:#334155; color:#e2e8f0; }}
    .broker-summary-left {{ text-align:left; }}
    .broker-summary-right {{ text-align:right; }}
    .status-dot {{ display:inline-block; width:10px; height:10px; border-radius:999px; border:2px solid currentColor; background: transparent; }}
    .status-green {{ color:#22c55e; }}
    .status-red {{ color:#ef4444; }}
    .status-amber {{ color:#f59e0b; }}
    .broker-cell {{ width: 180px; }}
    .broker-disabled {{ opacity: 0.42; filter: grayscale(0.25); }}
    .broker-disabled .mini-input,
    .broker-disabled .quick-btn {{ pointer-events: none; }}
    .symbol-line {{ display:flex; align-items:center; gap:6px; }}
    .checkline-inline {{ display:flex; align-items:center; justify-content:center; width:18px; min-width:18px; }}
    .mini-input {{ width: 100%; box-sizing: border-box; margin: 2px 0; padding: 5px 6px; border-radius: 6px; border: 1px solid #5a5861; background: #17191d; color: #eceff3; font-size: 12px; }}
    .symbol-input {{ margin: 0; flex: 1 1 auto; min-width: 0; background: transparent; }}
    .venue-inline {{ margin: 0; width: 62px; min-width: 62px; text-align: center; flex: 0 0 62px; }}
    .position-inline {{ margin: 0; width: 58px; min-width: 58px; text-align: center; flex: 0 0 58px; background: #111827; }}
    .qty-inline {{ margin: 0; width: 58px; min-width: 58px; text-align: center; flex: 0 0 58px; }}
    .row-dirty {{ outline: 2px solid #6b7280; outline-offset: -2px; }}
    .row-save {{ width: 100%; padding: 6px 8px; font-size: 12px; }}
    .row-save-inline {{ width: auto; min-width: 56px; padding: 4px 8px; line-height: 1.1; flex: 0 0 auto; }}
    .cell-subhint {{ font-size: 11px; color:#aab4c0; margin-left: 6px; white-space: normal; overflow: hidden; text-overflow: ellipsis; min-width: 0; flex: 1 1 auto; line-height: 1.15; }}
    .quick-line {{ display:flex; align-items:center; gap:4px; margin-top:4px; min-width:0; }}
    .quick-btn {{ padding: 3px 8px; border-radius: 6px; font-size: 11px; line-height: 1.1; flex: 0 0 auto; }}
    .buy-btn {{ background: #14532d; color: #dcfce7; }}
    .sell-btn {{ background: #7f1d1d; color: #fee2e2; }}
    .book-btn {{ background: #1d4ed8; color: #dbeafe; }}
    .quick-qty {{ width: 56px; min-width: 56px; text-align: center; margin: 0; flex: 0 0 56px; }}
    .qty-neg {{ border-color: #dc2626; color: #fecaca; background: #2b1212; }}
    .qty-pos {{ border-color: #16a34a; color: #bbf7d0; background: #102315; }}
    .qty-work {{ border-color: #d97706; color: #fde68a; background: #2b2111; font-weight: 600; }}
    .qty-neutral {{ border-color: #4b5563; }}
    .broker-info-card {{ display:flex; flex-direction:column; gap:4px; min-height:92px; }}
    .broker-info-line {{ font-size:11px; color:#cbd5e1; line-height:1.2; }}
    .broker-info-label {{ color:#9ca3af; margin-right:4px; }}
    .broker-info-value {{ color:#e5e7eb; }}
    .broker-info-muted {{ color:#93a0af; }}
    .manual-row-disabled td {{ opacity: 0.8; }}
    .book-modal {{ position: fixed; inset: 0; background: rgba(3,7,18,0.72); display:none; align-items:center; justify-content:center; z-index: 50; }}
    .book-modal.open {{ display:flex; }}
    .book-modal-card {{ width:min(920px, calc(100vw - 32px)); max-height: calc(100vh - 32px); overflow:auto; background:#111827; border:1px solid #374151; border-radius:12px; padding:14px; box-shadow: 0 20px 60px rgba(0,0,0,0.45); }}
    .book-modal-head {{ display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:10px; }}
    .book-pre {{ white-space: pre-wrap; word-break: break-word; font-size:12px; color:#dbe4ee; background:#0b1220; border:1px solid #243041; border-radius:8px; padding:10px; }}
    .book-summary {{ display:grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap:8px; margin-bottom:12px; }}
    .book-stat {{ background:#0b1220; border:1px solid #243041; border-radius:8px; padding:8px; }}
    .book-stat-label {{ font-size:10px; color:#94a3b8; margin-bottom:4px; }}
    .book-stat-value {{ font-size:13px; color:#e5eef7; }}
    .book-grid {{ display:grid; grid-template-columns: 1fr 1fr; gap:12px; }}
    .book-side-title {{ font-size:12px; margin-bottom:6px; text-transform:uppercase; letter-spacing:0.04em; font-weight:600; }}
    .book-table {{ width:100%; border-collapse:collapse; background:#0b1220; border:1px solid #243041; border-radius:8px; overflow:hidden; }}
    .book-table th, .book-table td {{ padding:6px 8px; font-size:12px; border-bottom:1px solid #1f2a3a; }}
    .book-table tbody tr:nth-child(odd).book-ask td {{ background: rgba(127, 29, 29, 0.22); }}
    .book-table tbody tr:nth-child(even).book-ask td {{ background: rgba(69, 10, 10, 0.22); }}
    .book-table tbody tr:nth-child(odd).book-bid td {{ background: rgba(20, 83, 45, 0.22); }}
    .book-table tbody tr:nth-child(even).book-bid td {{ background: rgba(5, 46, 22, 0.22); }}
    .book-ask td {{ color:#fecaca; }}
    .book-bid td {{ color:#bbf7d0; }}
  </style>
  <script>
    function updateQtyColors() {{
      document.querySelectorAll('input[name^="qty|"], .position-inline').forEach(function(input) {{
        input.classList.remove('qty-neg', 'qty-pos', 'qty-neutral', 'qty-work');
        const raw = String(input.value || '').trim().toUpperCase();
        if (raw === 'WORK') {{
          input.classList.add('qty-work');
          return;
        }}
        if (raw === 'REJ' || raw === 'REJECT' || raw === 'REJECTED') {{
          input.classList.add('qty-neg');
          return;
        }}
        const v = parseFloat(input.value);
        if (isNaN(v)) input.classList.add('qty-neutral');
        else if (v < 0) input.classList.add('qty-neg');
        else if (v > 0) input.classList.add('qty-pos');
        else input.classList.add('qty-neutral');
      }});
    }}
    function rowSignature(row) {{
      const fields = Array.from(row.querySelectorAll('input[name]')).map(function(el) {{
        const value = el.type === 'checkbox' ? (el.checked ? '1' : '0') : el.value;
        return el.name + '=' + value;
      }});
      return fields.join('||');
    }}
    function refreshRowDirtyState(row) {{
      const btn = row.querySelector('[data-row-save]');
      const current = rowSignature(row);
      const initial = row.getAttribute('data-initial-signature') || '';
      let dirty = current !== initial;
      if (row && row.getAttribute('data-manual-row') === '1') {{
        const master = row.querySelector("input[name='manualRowEnabled']");
        const tickerInput = row.querySelector("input[name='manualTicker']");
        const enabled = !!(master && master.checked);
        const ticker = String((tickerInput && tickerInput.value) || '').trim();
        dirty = dirty && enabled && !!ticker;
      }}
      row.classList.toggle('row-dirty', dirty);
      if (btn) btn.disabled = !dirty;
    }}
    function refreshBrokerCellState(cell) {{
      if (!cell) return;
      const row = cell.closest('.mapping-row');
      if (row && row.getAttribute('data-manual-row') === '1') return;
      const checkbox = cell.querySelector('input[type="checkbox"][name^="map|"]');
      if (!checkbox) return;
      const enabled = checkbox.checked;
      cell.classList.toggle('broker-disabled', !enabled);
      cell.querySelectorAll('input, button').forEach(function(el) {{
        if (el === checkbox) return;
        if (el.type === 'hidden') return;
        el.disabled = !enabled;
      }});
    }}
    function refreshManualRowState(row) {{
      if (!row || row.getAttribute('data-manual-row') !== '1') return;
      const master = row.querySelector("input[name='manualRowEnabled']");
      const enabled = !!(master && master.checked);
      row.classList.toggle('manual-row-disabled', !enabled);
      const tickerInput = row.querySelector("input[name='manualTicker']");
      const atomicInput = row.querySelector("input[name^='atomicQty|']");
      if (tickerInput) tickerInput.disabled = !enabled;
      if (atomicInput) atomicInput.disabled = !enabled;
      row.querySelectorAll('.broker-cell').forEach(function(cell) {{
        cell.classList.add('broker-disabled');
        cell.querySelectorAll('input, button').forEach(function(el) {{
          if (el.type === 'hidden') return;
          el.disabled = true;
        }});
      }});
    }}
    function refreshAllRows() {{
      document.querySelectorAll('.mapping-row').forEach(function(row) {{
        if (row.getAttribute('data-manual-row') === '1') refreshManualRowState(row);
        row.querySelectorAll('.broker-cell').forEach(function(cell) {{
          refreshBrokerCellState(cell);
        }});
        refreshRowDirtyState(row);
      }});
    }}
    document.addEventListener('input', function(e) {{
      if (e.target && e.target.name && e.target.name.startsWith('qty|')) updateQtyColors();
      const row = e.target.closest('.mapping-row');
      if (row) refreshRowDirtyState(row);
    }});
    document.addEventListener('change', function(e) {{
      const row = e.target.closest('.mapping-row');
      const cell = e.target.closest('.broker-cell');
      if (row && row.getAttribute('data-manual-row') === '1') refreshManualRowState(row);
      if (cell) refreshBrokerCellState(cell);
      if (row) refreshRowDirtyState(row);
    }});
    function setFlashStatus(text, kind) {{
      const el = document.getElementById('flash-status');
      if (!el) return;
      el.textContent = text || '';
      el.className = 'flash-status' + (kind ? ' ' + kind : '');
    }}
    async function saveRow(row, btn) {{
      if (!row || !btn || btn.disabled) return;
      const isManualRow = row.getAttribute('data-manual-row') === '1';
      let ticker = row.getAttribute('data-ticker') || '';
      if (isManualRow) {{
        const manualTickerInput = row.querySelector("input[name='manualTicker']");
        ticker = String((manualTickerInput && manualTickerInput.value) || '').trim();
      }}
      if (!ticker) {{
        setFlashStatus('укажи ticker', 'error');
        return;
      }}
      const form = document.getElementById('mapping-form');
      if (!form) return;
      const originalText = btn.textContent;
      btn.disabled = true;
      btn.textContent = '...';
      setFlashStatus(`сохраняю ${{ticker}}`, '');
      try {{
        let res;
        if (isManualRow) {{
          const formData = new FormData();
          formData.append('ticker', ticker);
          const atomicInput = row.querySelector("input[name^='atomicQty|']");
          formData.append('atomicQty', String((atomicInput && atomicInput.value) || '').trim());
          res = await fetch('/admin/add-ticker', {{
            method: 'POST',
            body: new URLSearchParams(Array.from(formData.entries())),
          }});
        }} else {{
          const formData = new FormData();
          formData.append('ticker', ticker);
          formData.append('saveTicker', ticker);
          row.querySelectorAll('input[name]').forEach(function(el) {{
            if (el.disabled) return;
            const name = el.name;
            if (name === 'ticker' || name === 'manualTicker' || name === 'manualRowEnabled') return;
            if (el.type === 'checkbox') {{
              if (el.checked) formData.append(name, el.value || 'on');
              return;
            }}
            formData.append(name, el.value);
          }});
          res = await fetch(form.getAttribute('action') || '/admin/save-mappings', {{
            method: 'POST',
            body: new URLSearchParams(Array.from(formData.entries())),
          }});
        }}
        const data = await res.json();
        if (!res.ok || data.error) {{
          setFlashStatus(`ошибка save: ${{data.error || res.status}}`, 'error');
        }} else {{
          row.setAttribute('data-initial-signature', rowSignature(row));
          refreshRowDirtyState(row);
          setFlashStatus(`saved: ${{ticker}}`, 'ok');
          if (isManualRow) setTimeout(function() {{ window.location.reload(); }}, 250);
        }}
      }} catch (err) {{
        setFlashStatus(`ошибка save: ${{err}}`, 'error');
      }} finally {{
        btn.textContent = originalText;
        refreshRowDirtyState(row);
      }}
    }}
    document.addEventListener('click', async function(e) {{
      const syncBtn = e.target.closest('[data-admin-broker-sync]');
      if (syncBtn) {{
        const broker = syncBtn.getAttribute('data-admin-broker-sync') || '';
        const originalText = syncBtn.textContent;
        syncBtn.disabled = true;
        syncBtn.textContent = '...';
        try {{
          const body = new URLSearchParams();
          body.set('broker', broker);
          const res = await fetch('/settings/test-broker', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }},
            body: body.toString(),
          }});
          const text = await res.text();
          let payload = null;
          try {{ payload = JSON.parse(text); }} catch (_) {{}}
          if (!res.ok) setFlashStatus(`ошибка sync: ${{broker}}`, 'error');
          else {{
            const sync = (payload && payload.sync) || payload || {{}};
            const active = Array.isArray(sync.activeSymbols) ? sync.activeSymbols.length : (Array.isArray(payload && payload.activeSymbols) ? payload.activeSymbols.length : 0);
            const withLimits = Array.isArray(sync.activeWithLimits) ? sync.activeWithLimits.length : (Array.isArray(payload && payload.activeWithLimits) ? payload.activeWithLimits.length : 0);
            const updatedRoutes = Number(sync.updatedRoutes || (payload && payload.updatedRoutes) || 0);
            setFlashStatus(`sync: ${{broker}} active ${{active}}, limits ${{withLimits}}, updated ${{updatedRoutes}}`, 'ok');
          }}
          setTimeout(function() {{ refreshBrokerMetrics(); }}, 300);
        }} catch (err) {{
          setFlashStatus(`ошибка sync: ${{err}}`, 'error');
        }} finally {{
          syncBtn.disabled = false;
          syncBtn.textContent = originalText;
        }}
        return;
      }}
      const bookBtn = e.target.closest('[data-book-view]');
      if (bookBtn) {{
        const cell = bookBtn.closest('.broker-cell');
        const row = bookBtn.closest('.mapping-row');
        if (!cell || !row) return;
        const broker = bookBtn.getAttribute('data-broker') || '';
        const symbol = (cell.querySelector('.symbol-input') || {{value:''}}).value;
        const venue = (cell.querySelector('.venue-inline') || {{value:''}}).value;
        const modal = document.getElementById('book-modal');
        const pre = document.getElementById('book-modal-pre');
        const title = document.getElementById('book-modal-title');
        const bodyEl = document.getElementById('book-modal-body');
        if (!modal || !pre || !title || !bodyEl) return;
        title.textContent = `book: ${{broker}} ${{symbol}}`;
        bodyEl.innerHTML = '<div class="book-pre">loading...</div>';
        modal.classList.add('open');
        try {{
          const body = new URLSearchParams();
          body.set('broker', broker);
          body.set('symbol', symbol);
          body.set('venue', venue);
          const res = await fetch('/admin/book-view', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }},
            body: body.toString(),
          }});
          const data = await res.json();
          const view = data.view || {{}};
          const bids = Array.isArray(view.bids) ? view.bids : [];
          const asks = Array.isArray(view.asks) ? view.asks : [];
          const stats = view.stats || {{}};
          const statCard = (label, value) => `<div class="book-stat"><div class="book-stat-label">${{label}}</div><div class="book-stat-value">${{value || '—'}}</div></div>`;
          const rows = (items, cls) => items.map((item) => `<tr class="${{cls}}"><td>${{item.price ?? '—'}}</td><td>${{item.qty ?? '—'}}</td></tr>`).join('') || `<tr><td colspan="2">нет данных</td></tr>`;
          bodyEl.innerHTML = `
            <div class="book-summary">
              ${{statCard('symbol', stats.symbol)}}
              ${{statCard('bid', stats.bid)}}
              ${{statCard('ask', stats.ask)}}
              ${{statCard('spread', stats.spread)}}
              ${{statCard('minQty', stats.minQty)}}
              ${{statCard('minUSDT', stats.minUsdt)}}
            </div>
            <div class="book-grid">
              <div>
                <div class="book-side-title" style="color:#fca5a5;">asks</div>
                <table class="book-table"><thead><tr><th>price</th><th>qty</th></tr></thead><tbody>${{rows(asks, 'book-ask')}}</tbody></table>
              </div>
              <div>
                <div class="book-side-title" style="color:#86efac;">bids</div>
                <table class="book-table"><thead><tr><th>price</th><th>qty</th></tr></thead><tbody>${{rows(bids, 'book-bid')}}</tbody></table>
              </div>
            </div>
          `;
          pre.textContent = JSON.stringify(data, null, 2);
        }} catch (err) {{
          bodyEl.innerHTML = `<div class="book-pre">${{String(err)}}</div>`;
        }}
        return;
      }}
      const quickBtn = e.target.closest('[data-quick-order]');
      if (quickBtn) {{
        const cell = quickBtn.closest('.broker-cell');
        const row = quickBtn.closest('.mapping-row');
        if (!cell || !row) return;
        const ticker = row.getAttribute('data-ticker') || '';
        const broker = quickBtn.getAttribute('data-broker') || '';
        const side = quickBtn.getAttribute('data-side') || '';
        const symbol = (cell.querySelector('.symbol-input') || {{value:''}}).value;
        const venue = (cell.querySelector('.venue-inline') || {{value:''}}).value;
        const quickQtyInput = cell.querySelector('.quick-qty');
        const qty = (quickQtyInput || row.querySelector('.atomic-qty-input') || {{value:''}}).value;
        if (!qty) return;
        const originalText = quickBtn.textContent;
        quickBtn.disabled = true;
        quickBtn.textContent = '...';
        setFlashStatus(`отправка ${{side}} ${{qty}} ${{symbol}} -> ${{broker}}`, '');
        try {{
          const body = new URLSearchParams();
          body.set('ticker', ticker);
          body.set('broker', broker);
          body.set('side', side);
          body.set('qty', qty);
          body.set('symbol', symbol);
          body.set('venue', venue);
          const res = await fetch('/admin/quick-order', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }},
            body: body.toString(),
          }});
          const data = await res.json();
          const firstDest = (((data || {{}}).result || {{}}).destinations || [])[0] || {{}};
          const brokerResult = firstDest.results || {{}};
          const dryRun = !!(firstDest.dryRun || brokerResult.dryRun);
          const brokerState = String((data || {{}}).status || '');
          const brokerOk = brokerState === 'ok';
          const brokerAccepted = brokerState === 'accepted';
          const brokerErrorText = String((data || {{}}).details || brokerResult.error || brokerResult.details || brokerResult.retMsg || '');
          if (!res.ok || data.error) {{
            setFlashStatus(`ошибка quick-order: ${{(data.error || data.details || res.status)}}`, 'error');
          }} else if (dryRun) {{
            setFlashStatus(`dry-run: ${{side}} ${{qty}} ${{symbol}} -> ${{broker}}`, 'error');
          }} else if (brokerAccepted) {{
            setFlashStatus(`accepted: ${{side}} ${{qty}} ${{symbol}} -> ${{broker}} ${{brokerErrorText}}`, '');
          }} else if (!brokerOk) {{
            setFlashStatus(`ошибка quick-order: ${{brokerErrorText || 'broker reject'}}`, 'error');
          }} else {{
            setFlashStatus(`ok: ${{side}} ${{qty}} ${{symbol}} -> ${{broker}}`, 'ok');
          }}
        }} catch (err) {{
          setFlashStatus(`ошибка сети: ${{err}}`, 'error');
        }} finally {{
          quickBtn.disabled = false;
          quickBtn.textContent = originalText;
        }}
        return;
      }}
      const btn = e.target.closest('[data-row-save]');
      if (!btn) return;
      e.preventDefault();
      const row = btn.closest('.mapping-row');
      await saveRow(row, btn);
    }});
    function updateBrokerInfoCards() {{
      const brokerConfigs = {{
        alor: {{ timeZone: 'Europe/Moscow', kind: 'moex', open: '10:00', close: '23:50' }},
        finam: {{ timeZone: 'Europe/Moscow', kind: 'moex', open: '10:00', close: '23:50' }},
        bybit: {{ timeZone: 'UTC', kind: 'always-open', open: '00:00', close: '24:00' }},
        schwab: {{ timeZone: 'America/New_York', kind: 'us-equity', open: '09:30', close: '16:00' }},
      }};
      const pad = (n) => String(n).padStart(2, '0');
      const toParts = function(date, timeZone) {{
        const parts = new Intl.DateTimeFormat('en-GB', {{
          timeZone,
          hour12: false,
          weekday: 'short',
          hour: '2-digit',
          minute: '2-digit',
        }}).formatToParts(date);
        const map = {{}};
        parts.forEach(function(part) {{ map[part.type] = part.value; }});
        return map;
      }};
      const parseMinutes = function(text) {{
        const [hh, mm] = String(text || '00:00').split(':').map(Number);
        return (hh * 60) + mm;
      }};
      document.querySelectorAll('[data-broker-info]').forEach(function(card) {{
        const broker = card.getAttribute('data-broker-info');
        const cfg = brokerConfigs[broker];
        if (!cfg) return;
        const now = new Date();
        const parts = toParts(now, cfg.timeZone);
        const weekday = String(parts.weekday || '').toLowerCase();
        const hour = Number(parts.hour || 0);
        const minute = Number(parts.minute || 0);
        const totalMinutes = hour * 60 + minute;
        let sessionStatus = 'closed';
        if (cfg.kind === 'always-open') {{
          sessionStatus = 'open';
        }} else if (cfg.kind === 'us-equity') {{
          if (weekday === 'sat' || weekday === 'sun') sessionStatus = 'closed';
          else if (totalMinutes >= parseMinutes('04:00') && totalMinutes < parseMinutes('09:30')) sessionStatus = 'premarket';
          else if (totalMinutes >= parseMinutes('09:30') && totalMinutes < parseMinutes('16:00')) sessionStatus = 'open';
          else if (totalMinutes >= parseMinutes('16:00') && totalMinutes < parseMinutes('20:00')) sessionStatus = 'afterhours';
          else sessionStatus = 'closed';
        }} else {{
          if (weekday === 'sat' || weekday === 'sun') sessionStatus = 'closed';
          else if (totalMinutes >= parseMinutes(cfg.open) && totalMinutes < parseMinutes(cfg.close)) sessionStatus = 'open';
          else sessionStatus = 'closed';
        }}
        const timeEl = card.querySelector('[data-broker-info-time]');
        const sessionEl = card.querySelector('[data-broker-info-session]');
        if (timeEl) timeEl.textContent = `${{pad(hour)}}:${{pad(minute)}}`;
        if (sessionEl) sessionEl.textContent = sessionStatus;
      }});
    }}
    function tickHeaderClock() {{
      const el = document.getElementById('current-time');
      if (!el) return;
      const now = new Date();
      const pad = (n) => String(n).padStart(2, '0');
      el.textContent = `${{pad(now.getDate())}}-${{pad(now.getMonth()+1)}}-${{now.getFullYear()}} ${{pad(now.getHours())}}:${{pad(now.getMinutes())}}:${{pad(now.getSeconds())}}`;
      updateBrokerInfoCards();
    }}
    let liveTickerReloadScheduled = false;
    async function refreshBrokerMetrics() {{
      try {{
        const res = await fetch('/api/broker-metrics', {{cache:'no-store'}});
        if (!res.ok) return;
        const data = await res.json();
        const meta = data._meta || {{}};
        Object.entries(data || {{}}).forEach(function(entry) {{
          const broker = entry[0];
          if (String(broker).startsWith('_')) return;
          const payload = entry[1] || {{}};
          const summary = payload.summary || {{}};
          const head = document.querySelector(`[data-broker-head="${{broker}}"] .status-dot`);
          const portfolioEl = document.querySelector(`[data-broker-portfolio="${{broker}}"]`);
          const cashEl = document.querySelector(`[data-broker-cash="${{broker}}"]`);
          const goEl = document.querySelector(`[data-broker-go="${{broker}}"]`);
          let statusClass = 'status-red';
          const dryRunMode = String(summary.liveStatus || '').includes('dry') || String(summary.mode || '').includes('dry');
          const testMode = String(summary.mode || '').includes('test');
          const hasAnySummary = !!(summary.portfolio || summary.cash || summary.money || summary.go);
          if (dryRunMode || testMode) statusClass = 'status-amber';
          else if (String(summary.liveStatus || '').includes('live-ok') || hasAnySummary) statusClass = 'status-green';
          if (head) {{
            head.classList.remove('status-green', 'status-red', 'status-amber');
            head.classList.add(statusClass);
          }}

          if (broker === 'alor') {{
            if (portfolioEl) portfolioEl.textContent = `деньги ${{summary.money || summary.cash || '—'}} ${{summary.currency || ''}}`;
            if (cashEl) cashEl.textContent = '';
            if (goEl) goEl.textContent = summary.go && summary.go !== '—' ? `ГО ${{summary.go}}` : '';
          }} else {{
            if (portfolioEl) portfolioEl.textContent = `портфель ${{summary.portfolio || '—'}} ${{summary.currency || ''}}`;
            if (cashEl) cashEl.textContent = `остаток ${{summary.cash || '—'}}`;
            if (goEl) goEl.textContent = `ГО ${{summary.go || '—'}}`;
          }}
          const infoHealthEl = document.querySelector(`[data-broker-info-health="${{broker}}"]`);
          if (infoHealthEl) {{
            const ageSec = meta.updatedAt ? Math.max(0, Math.round((Date.now() / 1000) - Number(meta.updatedAt))) : null;
            const modeText = testMode ? 'test' : (dryRunMode ? 'dry-run' : 'live');
            infoHealthEl.textContent = ageSec === null ? `${{modeText}} · sync —` : `${{modeText}} · sync ${{ageSec}}s`;
          }}

          document.querySelectorAll(`[data-broker-cell="${{broker}}"]`).forEach(function(cell) {{
            const symbolInput = cell.querySelector('.symbol-input');
            const symbol = (symbolInput && symbolInput.value) || cell.getAttribute('data-broker-symbol') || '';
            const sym = ((payload.symbols || {{}})[symbol] || {{}});
            const hint = sym.text || '';
            const hintEl = cell.querySelector('.cell-subhint');
            if (hintEl) hintEl.textContent = hint;
            if (sym.qty !== undefined) {{
              const positionInput = cell.querySelector('.position-inline');
              if (positionInput) {{
                positionInput.value = String(sym.qty);
                positionInput.classList.remove('qty-neg', 'qty-pos', 'qty-neutral', 'qty-work');
                const rawQty = String(sym.qty || '').trim().toUpperCase();
                if (rawQty === 'WORK' || String(sym.state || '') === 'working') positionInput.classList.add('qty-work');
                else if (rawQty === 'REJ' || rawQty === 'REJECT' || rawQty === 'REJECTED' || String(sym.state || '') === 'error') positionInput.classList.add('qty-neg');
                else {{
                  const v = parseFloat(positionInput.value);
                  if (isNaN(v)) positionInput.classList.add('qty-neutral');
                  else if (v < 0) positionInput.classList.add('qty-neg');
                  else if (v > 0) positionInput.classList.add('qty-pos');
                  else positionInput.classList.add('qty-neutral');
                }}
              }}
              const quickInput = cell.querySelector('.quick-qty');
              const atomicInput = cell.closest('.mapping-row')?.querySelector('.atomic-qty-input');
              if (quickInput && document.activeElement !== quickInput) {{
                const currentQuickQty = String(quickInput.value || '').trim();
                const baseQty = currentQuickQty || (atomicInput && atomicInput.value) || quickInput.getAttribute('data-base-qty') || '1';
                quickInput.value = baseQty;
              }}
            }}
          }});
        }});
        const unseenLiveTicker = (meta.newTickers || []).find(function(ticker) {{
          return !document.querySelector(`.mapping-row[data-ticker="${{ticker}}"]`);
        }});
        if (unseenLiveTicker && !liveTickerReloadScheduled) {{
          liveTickerReloadScheduled = true;
          setTimeout(function() {{ window.location.reload(); }}, 300);
        }}
      }} catch (e) {{
      }}
    }}
    document.addEventListener('change', async function(e) {{
      const testToggle = e.target.closest('[data-test-mode-toggle]');
      if (testToggle) {{
        const broker = testToggle.getAttribute('data-broker') || '';
        try {{
          const body = new URLSearchParams();
          body.set('broker', broker);
          body.set('enabled', testToggle.checked ? '1' : '0');
          const res = await fetch('/admin/set-test-mode', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }},
            body: body.toString(),
          }});
          const data = await res.json();
          if (!res.ok || data.error) setFlashStatus(`ошибка test-mode: ${{data.error || res.status}}`, 'error');
          else setFlashStatus(`test-mode ${{broker}}: ${{testToggle.checked ? 'on' : 'off'}}`, 'ok');
          refreshBrokerMetrics();
        }} catch (err) {{
          setFlashStatus(`ошибка test-mode: ${{err}}`, 'error');
        }}
        return;
      }}
    }});
    document.addEventListener('DOMContentLoaded', function() {{
      updateQtyColors();
      document.querySelectorAll('.mapping-row').forEach(function(row) {{
        row.setAttribute('data-initial-signature', rowSignature(row));
      }});
      refreshAllRows();
      tickHeaderClock();
      refreshBrokerMetrics();
      setInterval(tickHeaderClock, 1000);
      setInterval(refreshBrokerMetrics, 15000);
    }});
  </script>
</head>
<body>
  <form method='post' action='/admin/save-mappings' id='mapping-form' autocomplete='off' onsubmit='return false;'>
    <input type='hidden' name='saveTicker' id='saveTicker' value=''>
    <div class='panel'>
      <div class='toolbar'>
        <div class='toolbar-left'>
          <div>
            <h1>Webhook Router Admin</h1>
            <div class='muted'>Компактный режим: строка = тикер, колонка = брокер, внутри on + symbol + qty.</div>
            <div class='muted'>Текущее время: <span id='current-time'>{current_time}</span></div>
            <div class='muted'>build {html.escape(_build_summary().get('version','unknown'))} / server.py {html.escape(_build_summary().get('serverHash',''))}</div>
            <div id='flash-status' class='flash-status'></div>
          </div>
        </div>
        <div style='display:flex; gap:10px; align-items:center;'>
          <a href='/settings' style='color:#93c5fd;text-decoration:none;'>Settings</a>
          <a href='/effectiveness' style='color:#93c5fd;text-decoration:none;'>Effectiveness</a>
          <a href='/journal' style='color:#93c5fd;text-decoration:none;'>Journal</a>
          {"<a href='/users' style='color:#93c5fd;text-decoration:none;'>Users</a>" if can_manage_users else ""}
          <button type='button' disabled>Save all позже</button>
          <a href='/logout' style='color:#fca5a5;text-decoration:none;'>Logout</a>
        </div>
      </div>
      {''.join(lookup_lists)}
      <table>
        <thead>
          <tr>
            <th class='save-cell'>Save</th>
            <th class='ticker-cell'>Ticker</th>
            {header_cells}
          </tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>
    </div>
  </form>
  <div class='book-modal' id='book-modal'><div class='book-modal-card'><div class='book-modal-head'><strong id='book-modal-title'>book</strong><button type='button' class='quick-btn' onclick="document.getElementById('book-modal').classList.remove('open')">close</button></div><div id='book-modal-body'></div><details style='margin-top:10px;'><summary class='muted' style='cursor:pointer;'>raw json</summary><pre class='book-pre' id='book-modal-pre'></pre></details></div></div>
  <form method='post' action='/admin/quick-order' id='quick-order-form' style='display:none;'>
    <input type='hidden' name='ticker' id='quickTicker'>
    <input type='hidden' name='broker' id='quickBroker'>
    <input type='hidden' name='side' id='quickSide'>
    <input type='hidden' name='qty' id='quickQty'>
    <input type='hidden' name='symbol' id='quickSymbol'>
    <input type='hidden' name='venue' id='quickVenue'>
  </form>
</body>
</html>
"""


def _record_webhook_decision(decision: Dict[str, Any], payload: Dict[str, Any], materialized: Dict[str, Any]) -> None:
    with LOG_PATH.open('a') as f:
        f.write(json.dumps(decision, ensure_ascii=False, default=str) + '\n')

    try:
        record_execution_analytics(decision)
    except Exception:
        _safe_append_journal({
            'time': _utcnow_iso(),
            'kind': 'analytics-write-error',
            'ticker': str((payload or {}).get('sourceTicker') or ''),
            'side': str((payload or {}).get('side') or ''),
            'qty': (payload or {}).get('qty', ''),
            'brokers': [str(item.get('broker') or '') for item in ((decision.get('executionResult') or {}).get('destinations') or []) if isinstance(item, dict)],
            'status': 'error',
            'details': traceback.format_exc()[:2000],
        })

    origin = str(decision.get('origin') or 'webhook').strip() or 'webhook'
    destination_kind = 'quick-order-destination' if origin == 'quick-order' else 'webhook-destination'
    summary_kind = 'quick-order' if origin == 'quick-order' else 'webhook'
    destinations = ((decision.get('executionResult') or {}).get('destinations') or [])
    destination_debug = []
    for dest in destinations:
        broker = str(dest.get('broker') or '?')
        symbol = str(dest.get('symbol') or '')
        venue = str(dest.get('category') or dest.get('exchange') or dest.get('account') or '')
        req = dest.get('request') or {}
        result_obj = dest.get('results') or {}
        err = str(dest.get('error') or _result_error_text(result_obj) or '')
        dry_run = bool(dest.get('dryRun') or _result_is_dry_run(result_obj))
        broker_status = 'dry_run' if dry_run else ('execution_error' if err else 'placed')
        piece = f"{broker}:{symbol}"
        if venue:
            piece += f"@{venue}"
        units_hint = _journal_units_hint(req, symbol)
        if units_hint:
            piece += f" {units_hint}"
        if req:
            piece += f" req={_short_json(req, 220)}"
        risk_details = _extract_bingx_risk_details(dest)
        if risk_details:
            piece += f" risk={risk_details[:260]}"
        if err:
            piece += f" err={err[:220]}"
        destination_debug.append(piece)

        append_journal({
            'time': decision.get('receivedAt'),
            'kind': destination_kind,
            'ticker': payload.get('sourceTicker'),
            'side': payload.get('side'),
            'qty': payload.get('qty'),
            'qtyUnit': _payload_qty_unit(payload),
            'brokers': [broker],
            'status': broker_status,
            'symbol': symbol,
            'venue': venue,
            'details': (_journal_units_hint(req, symbol) + (" | " if _journal_units_hint(req, symbol) else '')) + f"payload={_short_json(payload)}" + (f" | request={_short_json(req, 260)}" if req else '') + (f" | risk={risk_details}" if risk_details else '') + (f" | result={_short_json(result_obj, 260)}" if result_obj else '') + (f" | error={err[:260]}" if err else ''),
        })

    summary_needed = bool(decision.get('error')) or len(destinations) <= 1
    if summary_needed:
        route_destinations = _route_destinations(materialized)
        single_destination = route_destinations[:1]
        append_journal({
            'time': decision.get('receivedAt'),
            'kind': summary_kind,
            'ticker': payload.get('sourceTicker'),
            'side': payload.get('side'),
            'qty': payload.get('qty'),
            'qtyUnit': _payload_qty_unit(payload),
            'brokers': [d.get('broker') for d in single_destination if d.get('broker')],
            'status': decision.get('status'),
            'details': (_journal_units_hint(single_destination[0] if single_destination else {}, str((single_destination[0] if single_destination else {}).get('symbol') or '')) + (" | " if single_destination else '')) + f"payload={_short_json(payload)}" + (f" | route={_short_json(materialized, 280)}" if materialized else '') + (f" | exec={' || '.join(destination_debug)}" if destination_debug else '') + (f" | error={decision.get('error')}" if decision.get('error') else ''),
        })


def _process_webhook_job(job: Dict[str, Any]) -> None:
    payload = copy.deepcopy(job.get('payload') or {})
    materialized = copy.deepcopy(job.get('route') or {})
    decision = {
        'receivedAt': job.get('receivedAt') or _utcnow_iso(),
        'jobId': job.get('jobId'),
        'origin': job.get('origin') or 'webhook',
        'payload': payload,
        'execution': job.get('execution') or {},
        'route': materialized,
        'status': 'accepted',
    }

    try:
        execution_result = execute_route_sync(payload, materialized)
        decision['executionResult'] = execution_result
        destinations = (execution_result or {}).get('destinations') or []
        per_statuses = []
        for dest in destinations:
            result_obj = dest.get('results') or {}
            err = str(dest.get('error') or _result_error_text(result_obj) or '')
            dry_run = bool(dest.get('dryRun') or _result_is_dry_run(result_obj))
            if dry_run:
                per_statuses.append('dry_run')
            elif err:
                per_statuses.append('execution_error')
            else:
                per_statuses.append('placed')
        if per_statuses and all(status == 'placed' for status in per_statuses):
            decision['status'] = 'placed'
        elif per_statuses and all(status == 'dry_run' for status in per_statuses):
            decision['status'] = 'dry_run'
        elif per_statuses and any(status == 'execution_error' for status in per_statuses):
            decision['status'] = 'partial_error'
    except Exception as e:
        decision['status'] = 'execution_error'
        decision['error'] = str(e)

    _record_webhook_decision(decision, payload, materialized)


def _webhook_worker_loop() -> None:
    while True:
        job = WEBHOOK_QUEUE.get()
        try:
            if job is None:
                return
            try:
                _process_webhook_job(job)
            except Exception as e:
                payload = copy.deepcopy((job or {}).get('payload') or {})
                route = copy.deepcopy((job or {}).get('route') or {})
                try:
                    with LOG_PATH.open('a') as f:
                        f.write(json.dumps({
                            'receivedAt': (job or {}).get('receivedAt') or _utcnow_iso(),
                            'jobId': (job or {}).get('jobId'),
                            'payload': payload,
                            'route': route,
                            'status': 'worker_error',
                            'error': str(e),
                        }, ensure_ascii=False, default=str) + '\n')
                except Exception:
                    pass
                try:
                    _append_multi_destination_journal(
                        'execution_error',
                        (job or {}).get('receivedAt') or _utcnow_iso(),
                        payload,
                        route,
                        lambda destination: f"jobId={(job or {}).get('jobId')} | payload={_short_json(payload)} | error={e}",
                    )
                except Exception:
                    pass
                traceback.print_exc()
        finally:
            WEBHOOK_QUEUE.task_done()


def _enqueue_webhook_job(payload: Dict[str, Any], materialized: Dict[str, Any], default_execution: Dict[str, Any], origin: str = 'webhook') -> Dict[str, Any]:
    received_at = _utcnow_iso()
    job = {
        'jobId': secrets.token_hex(8),
        'receivedAt': received_at,
        'origin': origin,
        'payload': copy.deepcopy(payload),
        'route': copy.deepcopy(materialized),
        'execution': copy.deepcopy(default_execution),
    }
    WEBHOOK_QUEUE.put(job)
    decision = {
        'receivedAt': received_at,
        'jobId': job['jobId'],
        'payload': payload,
        'execution': default_execution,
        'route': materialized,
        'status': 'accepted',
        'queued': True,
        'queueDepth': WEBHOOK_QUEUE.qsize(),
    }
    _append_multi_destination_journal(
        'accepted',
        received_at,
        payload,
        materialized,
        lambda destination: (_journal_units_hint(destination or {}, str((destination or {}).get('symbol') or '')) + (" | " if destination else '')) + f"jobId={job['jobId']} | payload={_short_json(payload)} | route={_short_json(destination or materialized, 280)} | queueDepth={decision['queueDepth']}",
    )
    return decision


class Handler(BaseHTTPRequestHandler):
    def _json(self, code, body):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(body, ensure_ascii=False, default=str).encode())

    def _html(self, code, body: str, cookie: str = ''):
        self.send_response(code)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        if cookie:
            self.send_header('Set-Cookie', cookie)
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def _bytes(self, code: int, data: bytes, content_type: str, filename: str = ''):
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        if filename:
            self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location: str, cookie: str = ''):
        self.send_response(302)
        self.send_header('Location', location)
        if cookie:
            self.send_header('Set-Cookie', cookie)
        self.end_headers()

    def _require_admin(self):
        reload_env()
        route_path = self.path.split('?', 1)[0]
        if not _is_protected_path(route_path):
            return False
        if _is_admin_authenticated(self.headers):
            return False
        if self.path.startswith('/api/') or self.path.startswith('/admin/'):
            self._json(401, {'error': 'auth_required'})
        else:
            self._html(401, _render_login_page())
        return True

    def do_GET(self):
        route_path = self.path.split('?', 1)[0]
        if route_path == '/login':
            return self._html(200, _render_login_page())
        if route_path == '/logout':
            token = _parse_cookie(self.headers.get('Cookie', '')).get(USER_SESSION_COOKIE, '')
            if token:
                USER_SESSIONS.pop(token, None)
            return self._redirect('/login', f'{USER_SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax')
        if self._require_admin():
            return
        current_user = _current_user(self.headers)
        if route_path == '/':
            config = load_config()
            observed = load_observed_signals()
            return self._html(200, _render_admin_ui(config, observed, current_user))
        if route_path == '/healthz':
            return self._json(200, {'status': 'ok', 'build': _build_summary(), 'queueDepth': WEBHOOK_QUEUE.qsize(), 'metricsUpdatedAt': METRICS_CACHE.get('updated_at') or 0.0})
        if route_path == '/settings':
            message = ''
            if 'saved=1' in self.path:
                message = 'settings saved'
            elif 'backup=created' in self.path:
                message = 'backup created'
            elif 'backup=restored' in self.path:
                message = 'backup restored'
            elif 'backup=deleted' in self.path:
                message = 'backup deleted'
            return self._html(200, _render_settings_page(saved=('saved=1' in self.path), message=message, user=current_user))
        if route_path == '/journal':
            query = parse_qs(self.path.split('?', 1)[1] if '?' in self.path else '', keep_blank_values=True)
            if not _has_permission(current_user, 'canViewJournal'):
                return self._json(403, {'error': 'forbidden'})
            return self._html(200, _render_journal_page(
                week=query.get('week', [''])[0],
                broker=query.get('broker', [''])[0],
                ticker=query.get('ticker', [''])[0],
                status=query.get('status', [''])[0],
                kind=query.get('kind', [''])[0],
                page=int(query.get('page', ['1'])[0] or '1'),
                sort=query.get('sort', ['desc'])[0],
            ))
        if route_path == '/effectiveness':
            if not _has_permission(current_user, 'canViewJournal'):
                return self._json(403, {'error': 'forbidden'})
            return self._html(200, _render_effectiveness_page())
        if route_path == '/journal/download':
            if not _has_permission(current_user, 'canViewJournal'):
                return self._json(403, {'error': 'forbidden'})
            query = parse_qs(self.path.split('?', 1)[1] if '?' in self.path else '', keep_blank_values=True)
            week = query.get('week', [''])[0] or _journal_week_key()
            candidates = [(_journal_dir() / f'journal-{week}.jsonl'), (_journal_dir() / f'journal-{week}.jsonl.gz')]
            for path in candidates:
                if path.exists():
                    data = path.read_bytes()
                    content_type = 'application/gzip' if path.suffix == '.gz' else 'application/x-ndjson'
                    return self._bytes(200, data, content_type, path.name)
            return self._json(404, {'error': 'journal_week_not_found', 'week': week})
        if route_path == '/api/state':
            config = load_config()
            observed = load_observed_signals()
            visible_brokers = [name for name in _available_brokers(config) if _can_access_broker(current_user, name)]
            config_view = copy.deepcopy(config)
            config_view['brokers'] = {name: cfg for name, cfg in (config.get('brokers') or {}).items() if name in visible_brokers}
            mapping = _current_mapping(config)
            filtered_mapping = {ticker: {broker: payload for broker, payload in broker_map.items() if broker in visible_brokers} for ticker, broker_map in mapping.items()}
            return self._json(200, {
                'config': config_view,
                'observedSignals': observed,
                'mapping': filtered_mapping,
                'user': {
                    'username': current_user.get('username') or '',
                    'role': current_user.get('role') or '',
                    'permissions': current_user.get('effectivePermissions') or {},
                },
            })
        if route_path == '/api/broker-metrics':
            config = load_config()
            metrics = _get_live_metrics_cached(config)
            allowed = [name for name in (metrics.keys()) if name != 'newTickers' and name != 'updatedAt' and _can_access_broker(current_user, name)]
            filtered = {key: metrics[key] for key in ('updatedAt', 'newTickers') if key in metrics}
            for name in allowed:
                filtered[name] = metrics.get(name)
            return self._json(200, filtered)
        if route_path == '/api/effectiveness-overview':
            if not _has_permission(current_user, 'canViewJournal'):
                return self._json(403, {'error': 'forbidden'})
            try:
                return self._json(200, analytics_overview(limit=20))
            except Exception as e:
                return self._json(500, {'error': 'analytics_overview_failed', 'details': str(e)})
        if route_path == '/settings/backup/download':
            if not _has_permission(current_user, 'canDownloadBackups'):
                return self._json(403, {'error': 'forbidden'})
            query = parse_qs(self.path.split('?', 1)[1] if '?' in self.path else '', keep_blank_values=True)
            name = query.get('name', [''])[0]
            try:
                tar_path = _export_backup_tarball(name)
            except FileNotFoundError:
                return self._json(404, {'error': 'backup_not_found'})
            return self._bytes(200, tar_path.read_bytes(), 'application/gzip', tar_path.name)
        if route_path == '/users':
            if not _has_permission(current_user, 'canManageUsers'):
                return self._json(403, {'error': 'forbidden'})
            store = _load_user_store()
            users = store.get('users') or []
            roles = store.get('roles') or {}
            brokers = _available_brokers(load_config())
            sections = _settings_sections()
            rows = []
            for user in users:
                perms = _merge_permissions(str(user.get('role') or ''), user.get('permissions') or {}, roles)
                rows.append(f"<tr><td><code>{html.escape(str(user.get('username') or ''))}</code></td><td>{html.escape(str(user.get('role') or ''))}</td><td>{html.escape(', '.join(perms.get('brokers') or []))}</td><td>{html.escape(', '.join(perms.get('settingsSections') or []))}</td><td>{'yes' if perms.get('canAddTickers') else 'no'}</td><td>{'yes' if perms.get('canManageBackups') else 'no'}</td></tr>")
            roles_options = ''.join(f"<option value='{html.escape(name)}'>{html.escape((payload.get('label') or name))}</option>" for name, payload in roles.items())
            broker_checks = ''.join(f"<label style='margin-right:8px;'><input type='checkbox' name='brokers' value='{html.escape(name)}'> {html.escape(name)}</label>" for name in brokers)
            section_checks = ''.join(f"<label style='margin-right:8px;'><input type='checkbox' name='settingsSections' value='{html.escape(name)}'> {html.escape(name)}</label>" for name in sections)
            page = f"""<!doctype html><html lang='ru'><head><meta charset='utf-8'><title>Users</title><style>body{{font-family:-apple-system,BlinkMacSystemFont,sans-serif;margin:16px;background:#111827;color:#f3f4f6}}table{{width:100%;border-collapse:collapse;background:#1f2937}}th,td{{border:1px solid #374151;padding:8px}}.panel{{background:#1f2937;padding:12px;border-radius:10px;margin-bottom:12px}}.mini-input{{padding:6px 8px;border-radius:8px;border:1px solid #4b5563;background:#17191d;color:#eceff3}}button{{background:#2563eb;color:#fff;border:0;border-radius:8px;padding:8px 12px}}a{{color:#93c5fd;text-decoration:none}}</style></head><body><div class='panel'><div style='display:flex;justify-content:space-between;align-items:center;'><h2 style='margin:0;'>Users</h2><div><a href='/'>Admin</a> · <a href='/settings'>Settings</a></div></div></div><div class='panel'><form method='post' action='/users/create'><div style='display:flex;gap:8px;flex-wrap:wrap;align-items:center;'><input class='mini-input' type='text' name='username' placeholder='username' required><input class='mini-input' type='password' name='password' placeholder='password' required><select class='mini-input' name='role'>{roles_options}</select></div><div style='margin-top:10px;'><div style='margin-bottom:4px;'>Brokers</div>{broker_checks}</div><div style='margin-top:10px;'><div style='margin-bottom:4px;'>Settings sections</div>{section_checks}</div><div style='margin-top:10px; display:flex; gap:10px; flex-wrap:wrap;'><label><input type='checkbox' name='canAddTickers' value='1'> can add tickers</label><label><input type='checkbox' name='canAssign' value='1'> can assign</label><label><input type='checkbox' name='canManageBackups' value='1'> can manage backups</label><label><input type='checkbox' name='canDownloadBackups' value='1'> can download backups</label><label><input type='checkbox' name='canEditMappings' value='1'> can edit mappings</label><label><input type='checkbox' name='canQuickOrder' value='1'> can quick order</label></div><div style='margin-top:12px;'><button type='submit'>Add user</button></div></form></div><table><thead><tr><th>Username</th><th>Role</th><th>Brokers</th><th>Sections</th><th>Add tickers</th><th>Backups</th></tr></thead><tbody>{''.join(rows) or '<tr><td colspan="6">No users</td></tr>'}</tbody></table></body></html>"""
            return self._html(200, page)
        return self._json(404, {'error': 'not_found'})

    def do_POST(self):
        reload_env()
        if self.path == '/login':
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)
            username = form.get('username', ['admin'])[0].strip() or 'admin'
            password = form.get('password', [''])[0]
            user = _find_user(username)
            if not user:
                admin_password = _admin_password()
                if username == 'admin' and admin_password and password == admin_password:
                    store = _load_user_store()
                    user = {'username': 'admin', 'role': 'admin', 'permissions': {}, 'disabled': False}
                else:
                    return self._html(401, _render_login_page('wrong username or password'))
            if user.get('disabled') or not _password_matches(password, str(user.get('passwordHash') or '')):
                return self._html(401, _render_login_page('wrong username or password'))
            token = _create_user_session(str(user.get('username') or username))
            return self._redirect('/', f'{USER_SESSION_COOKIE}={token}; Path=/; Max-Age={ADMIN_SESSION_TTL_SECONDS}; HttpOnly; SameSite=Lax')

        if self._require_admin():
            return

        current_user = _current_user(self.headers)

        if self.path == '/users/create':
            if not _has_permission(current_user, 'canManageUsers'):
                return self._json(403, {'error': 'forbidden'})
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)
            username = form.get('username', [''])[0].strip()
            password = form.get('password', [''])[0]
            role = form.get('role', ['viewer'])[0].strip() or 'viewer'
            if not username or not password:
                return self._json(400, {'error': 'missing_username_or_password'})
            store = _load_user_store()
            if _find_user(username):
                return self._json(400, {'error': 'user_exists'})
            custom_permissions = {
                'brokers': _dedupe_keep_order(form.get('brokers', [])),
                'settingsSections': _dedupe_keep_order(form.get('settingsSections', [])),
            }
            for key in BOOLEAN_PERMISSION_KEYS:
                custom_permissions[key] = form.get(key, ['0'])[0] == '1'
            store.setdefault('users', []).append({
                'username': username,
                'passwordHash': _password_hash(password),
                'role': role,
                'permissions': custom_permissions,
                'disabled': False,
                'createdAt': _utcnow_iso(),
            })
            _save_user_store(store)
            return self._redirect('/users')

        if self.path == '/settings/save':
            if not _has_permission(current_user, 'canManageBackups') and not _is_superuser(current_user):
                allowed_sections = [section for section in _settings_sections() if _can_access_section(current_user, section)]
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)
            _create_backup('pre-settings-save')
            env_values = _ordered_env_values()
            root_input = form.get('env|WEBHOOK_ROUTER_ROOT', [env_values.get('WEBHOOK_ROUTER_ROOT', str(ROOT))])[0]
            changed_keys = []
            for field in SETTINGS_FIELDS:
                env_key = field['key']
                section = str(field.get('section') or '')
                if not _can_access_section(current_user, section):
                    continue
                form_key = f'env|{env_key}'
                if form_key not in form:
                    continue
                raw_value = form.get(form_key, [''])[0]
                new_value = _normalize_env_value(field, raw_value, root_input)
                if env_values.get(env_key) != new_value:
                    changed_keys.append(env_key)
                env_values[env_key] = new_value
            save_env_file(env_values, ENV_PATH)
            reload_env()
            if env_values.get('FINAM_SECRET_PATH'):
                _write_text_secret(env_values.get('FINAM_SECRET_PATH', ''), form.get('secret|FINAM_SECRET', [''])[0])
            if env_values.get('ALOR_CONFIG_PATH'):
                _write_text_secret(env_values.get('ALOR_CONFIG_PATH', ''), form.get('secret|ALOR_CONFIG_JSON', [''])[0])
            if env_values.get('SCHWAB_CONFIG_PATH'):
                _write_text_secret(env_values.get('SCHWAB_CONFIG_PATH', ''), form.get('secret|SCHWAB_CONFIG_JSON', [''])[0])
            reread_env = parse_env_file(ENV_PATH)
            failed_keys = []
            for field in SETTINGS_FIELDS:
                env_key = field['key']
                form_key = f'env|{env_key}'
                if form_key not in form:
                    continue
                expected = env_values.get(env_key, '')
                actual = reread_env.get(env_key, '')
                if actual != expected:
                    failed_keys.append(env_key)
            append_journal({
                'time': _utcnow_iso(),
                'kind': 'settings-save',
                'status': 'error' if failed_keys else 'ok',
                'brokers': [],
                'changedKeys': changed_keys,
                'details': '' if not failed_keys else f"save_verify_failed: {','.join(failed_keys)}",
                'reloadApplied': True,
            })
            if failed_keys:
                return self._html(400, _render_settings_page(error=f"settings not saved: {', '.join(failed_keys)}"))
            return self._redirect('/settings?saved=1')

        if self.path == '/settings/test-broker':
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)
            broker = form.get('broker', [''])[0]
            tests = {broker: _broker_connection_test(broker)} if broker else {}
            if broker and broker in tests:
                config = load_config()
                lookup_sync = _sync_broker_lookup_lists(config, broker)
                BROKER_TEST_CACHE[broker] = tests[broker]
                info = tests[broker] or {}
                sync_details = info.get('details') or info.get('text') or ''
                if lookup_sync.get('details'):
                    sync_details = (sync_details + ' | ' if sync_details else '') + str(lookup_sync.get('details'))
                append_journal({
                    'time': _utcnow_iso(),
                    'kind': 'settings-sync',
                    'ticker': '',
                    'side': '',
                    'qty': '',
                    'brokers': [broker],
                    'status': 'ok' if info.get('ok') else 'error',
                    'details': sync_details,
                })
                return self._json(200, {
                    'ok': bool(info.get('ok')),
                    'broker': broker,
                    'test': info,
                    'sync': lookup_sync,
                    'activeSymbols': lookup_sync.get('activeSymbols') or [],
                    'activeWithLimits': lookup_sync.get('activeWithLimits') or [],
                })
            return self._json(200, {'ok': True, 'broker': broker or '', 'test': tests})

        if self.path == '/settings/backup/create':
            if not _has_permission(current_user, 'canManageBackups'):
                return self._json(403, {'error': 'forbidden'})
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)
            label = form.get('label', ['manual'])[0] or 'manual'
            _create_backup(label)
            return self._redirect('/settings?backup=created')

        if self.path == '/settings/backup/restore':
            if not _has_permission(current_user, 'canManageBackups'):
                return self._json(403, {'error': 'forbidden'})
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)
            name = form.get('name', [''])[0]
            try:
                _restore_backup(name)
            except Exception:
                return self._html(400, _render_settings_page(error='backup restore failed'))
            return self._redirect('/settings?backup=restored')

        if self.path == '/settings/backup/delete':
            if not _has_permission(current_user, 'canManageBackups'):
                return self._json(403, {'error': 'forbidden'})
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)
            name = form.get('name', [''])[0]
            try:
                _delete_backup(name)
            except Exception:
                return self._html(400, _render_settings_page(error='backup delete failed'))
            return self._redirect('/settings?backup=deleted')

        if self.path == '/admin/save-mappings':
            if not _has_permission(current_user, 'canEditMappings'):
                return self._json(403, {'error': 'forbidden'})
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)

            tickers = form.get('ticker', [])
            selections: Dict[str, Dict[str, Dict[str, Any]]] = {ticker: {} for ticker in tickers}
            for key in form.keys():
                if '|' not in key:
                    continue
                parts = key.split('|')
                field = parts[0]
                if len(parts) != 3:
                    continue
                _, ticker, broker = parts
                bucket = selections.setdefault(ticker, {}).setdefault(broker, {
                    'enabled': False,
                    'symbol': '',
                    'venue': '',
                    'qty': '',
                    'qtyMultiplier': '',
                    'riskPct': '',
                    'limits': {},
                })
                if field == 'map':
                    bucket['enabled'] = True
                elif field == 'symbol':
                    bucket['symbol'] = form.get(key, [''])[0]
                elif field == 'venue':
                    bucket['venue'] = form.get(key, [''])[0]
                elif field == 'qty':
                    bucket['qty'] = form.get(key, [''])[0]
                elif field == 'qtyMultiplier':
                    bucket['qtyMultiplier'] = form.get(key, [''])[0]
                elif field == 'riskPct':
                    bucket['riskPct'] = form.get(key, [''])[0]

            target_ticker = form.get('saveTicker', [''])[0].strip()
            config = load_config()
            if target_ticker:
                config = save_single_ticker_mapping(config, target_ticker, selections.get(target_ticker, {}), '')
                return self._json(200, {'status': 'ok', 'savedTicker': target_ticker, 'routes': len(config.get('routes', []))})

            config = save_mappings(config, selections, {})
            return self._json(200, {'status': 'ok', 'savedTickers': len(selections), 'routes': len(config.get('routes', []))})

        if self.path == '/admin/add-ticker':
            if not _has_permission(current_user, 'canAddTickers'):
                return self._json(403, {'error': 'forbidden'})
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)
            ticker = form.get('ticker', [''])[0].strip().upper()
            atomic_qty_raw = form.get('atomicQty', [''])[0].strip()
            if not ticker:
                return self._json(400, {'error': 'missing_ticker'})
            try:
                atomic_qty = abs(float(atomic_qty_raw)) if atomic_qty_raw else 1
            except Exception:
                return self._json(400, {'error': 'invalid_atomic_qty', 'qty': atomic_qty_raw})
            payload = {
                'sourceTicker': ticker,
                'side': 'buy',
                'qty': atomic_qty,
                'source': 'manual-ui',
            }
            with OBSERVED_SIGNALS_LOCK:
                observed = load_observed_signals()
                _upsert_observed_signal(observed, payload, increment_count=False)
                save_observed_signals(observed)
            append_journal({
                'time': _utcnow_iso(),
                'kind': 'routing-save',
                'ticker': ticker,
                'side': '',
                'qty': atomic_qty,
                'brokers': [],
                'status': 'added',
                'details': 'manual ticker added to list',
            })
            return self._json(200, {'status': 'ok', 'ticker': ticker, 'qty': atomic_qty})

        if self.path == '/admin/set-test-mode':
            if not _has_permission(current_user, 'canEditMappings'):
                return self._json(403, {'error': 'forbidden'})
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)
            broker = form.get('broker', [''])[0]
            enabled = form.get('enabled', ['0'])[0] == '1'
            if not _can_access_broker(current_user, broker):
                return self._json(403, {'error': 'forbidden_broker'})
            config = load_config()
            broker_cfg = config.get('brokers', {}).get(broker)
            if not broker_cfg:
                return self._json(404, {'error': 'broker_not_found'})
            if broker not in ('bybit', 'bingx'):
                return self._json(400, {'error': 'test_mode_not_supported'})
            broker_cfg.setdefault('defaultDestination', {})['testnet'] = enabled
            save_config(config)
            return self._json(200, {'status': 'ok', 'broker': broker, 'testMode': enabled})

        if self.path == '/admin/book-view':
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)
            broker = form.get('broker', [''])[0].strip().lower()
            symbol = form.get('symbol', [''])[0].strip()
            if not _can_access_broker(current_user, broker):
                return self._json(403, {'error': 'forbidden_broker'})
            if not broker or not symbol:
                return self._json(400, {'error': 'missing_broker_or_symbol'})
            try:
                if broker == 'bingx':
                    config = load_config()
                    bingx_cfg = (config.get('brokers', {}).get('bingx', {}) or {}).get('defaultDestination', {}) or {}
                    client = BingXBroker(testnet=bool(bingx_cfg.get('testnet', False)))
                    book = client.get_book_ticker(symbol)
                    contract = client.get_contracts(symbol)
                    depth = client.get_depth(symbol, limit=20)
                    book_data = (book.get('data') or {}).get('book_ticker') or {}
                    contract_items = (contract.get('data') or []) if isinstance(contract.get('data'), list) else []
                    contract_item = contract_items[0] if contract_items else {}
                    depth_data = depth.get('data') or {}
                    asks = depth_data.get('asks') or depth_data.get('a') or []
                    bids = depth_data.get('bids') or depth_data.get('b') or []
                    if not asks and book_data.get('ask_price') not in (None, ''):
                        asks = [[book_data.get('ask_price'), book_data.get('ask_qty')]]
                    if not bids and book_data.get('bid_price') not in (None, ''):
                        bids = [[book_data.get('bid_price'), book_data.get('bid_qty')]]
                    def _rows(items):
                        out = []
                        for item in items[:12]:
                            if isinstance(item, dict):
                                out.append({'price': item.get('price') or item.get('px'), 'qty': item.get('qty') or item.get('quantity') or item.get('size')})
                            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                                out.append({'price': item[0], 'qty': item[1]})
                        return out
                    bid = book_data.get('bid_price')
                    ask = book_data.get('ask_price')
                    spread = None
                    try:
                        if bid is not None and ask is not None:
                            spread = float(ask) - float(bid)
                    except Exception:
                        spread = None
                    return self._json(200, {
                        'broker': broker,
                        'symbol': symbol,
                        'book': book,
                        'contract': contract,
                        'depth': depth,
                        'view': {
                            'stats': {
                                'symbol': symbol,
                                'bid': bid,
                                'ask': ask,
                                'spread': spread,
                                'minQty': contract_item.get('tradeMinQuantity'),
                                'minUsdt': contract_item.get('tradeMinUSDT'),
                            },
                            'asks': _rows(asks),
                            'bids': _rows(bids),
                        },
                    })
                if broker == 'bybit':
                    config = load_config()
                    bybit_cfg = (config.get('brokers', {}).get('bybit', {}) or {}).get('defaultDestination', {}) or {}
                    client = BybitBroker(testnet=bool(bybit_cfg.get('testnet', False)))
                    venue = form.get('venue', ['linear'])[0].strip() or 'linear'
                    symbol_normalized = symbol.replace('-', '').upper()
                    book = client._request('GET', '/v5/market/orderbook', params={'category': venue, 'symbol': symbol_normalized, 'limit': 25})
                    instruments = client._request('GET', '/v5/market/instruments-info', params={'category': venue, 'symbol': symbol_normalized})
                    result = book.get('result') or {}
                    instrument_list = ((instruments.get('result') or {}).get('list')) or []
                    instrument_item = instrument_list[0] if instrument_list else {}
                    asks = result.get('a') or result.get('asks') or []
                    bids = result.get('b') or result.get('bids') or []
                    def _rows(items):
                        out = []
                        for item in items[:12]:
                            if isinstance(item, dict):
                                out.append({'price': item.get('price') or item.get('px'), 'qty': item.get('size') or item.get('qty') or item.get('quantity')})
                            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                                out.append({'price': item[0], 'qty': item[1]})
                        return out
                    ask = asks[0][0] if asks and isinstance(asks[0], (list, tuple)) and len(asks[0]) >= 1 else None
                    bid = bids[0][0] if bids and isinstance(bids[0], (list, tuple)) and len(bids[0]) >= 1 else None
                    spread = None
                    try:
                        if bid is not None and ask is not None:
                            spread = float(ask) - float(bid)
                    except Exception:
                        spread = None
                    return self._json(200, {
                        'broker': broker,
                        'symbol': symbol_normalized,
                        'venue': venue,
                        'book': book,
                        'instrument': instruments,
                        'view': {
                            'stats': {
                                'symbol': symbol_normalized,
                                'bid': bid,
                                'ask': ask,
                                'spread': spread,
                                'minQty': instrument_item.get('minOrderQty') or instrument_item.get('lotSizeFilter', {}).get('minOrderQty'),
                                'minUsdt': instrument_item.get('minNotionalValue') or instrument_item.get('lotSizeFilter', {}).get('minNotionalValue'),
                            },
                            'asks': _rows(asks),
                            'bids': _rows(bids),
                        },
                    })
            except Exception as e:
                return self._json(500, {'error': str(e), 'broker': broker, 'symbol': symbol})
            return self._json(400, {'error': 'book_view_not_supported', 'broker': broker, 'symbol': symbol})

        if self.path == '/admin/quick-order':
            if not _has_permission(current_user, 'canQuickOrder'):
                return self._json(403, {'error': 'forbidden'})
            length = int(self.headers.get('Content-Length', '0'))
            raw = self.rfile.read(length).decode()
            form = parse_qs(raw, keep_blank_values=True)
            ticker = form.get('ticker', [''])[0]
            broker = form.get('broker', [''])[0]
            if not _can_access_broker(current_user, broker):
                return self._json(403, {'error': 'forbidden_broker'})
            side = form.get('side', [''])[0]
            qty = form.get('qty', [''])[0]
            symbol = form.get('symbol', [''])[0]
            venue = form.get('venue', [''])[0]
            destination = {}
            payload = {
                'sourceTicker': ticker,
                'side': side,
                'qty': qty,
            }

            try:
                config = load_config()
                destination = _current_destination(config, ticker, broker)
                destination['symbol'] = symbol or destination.get('symbol')
                venue_key = _broker_venue_key(broker)
                destination[venue_key] = venue or destination.get(venue_key)

                if qty in ('', None):
                    error_payload = {'error': 'missing_quick_qty', 'ticker': ticker, 'broker': broker}
                    _safe_append_journal({
                        'time': _utcnow_iso(),
                        'kind': 'quick-order',
                        'ticker': ticker,
                        'side': side,
                        'qty': qty,
                        'qtyUnit': _payload_qty_unit(payload),
                        'brokers': [broker],
                        'status': 'error',
                        'symbol': destination.get('symbol', symbol),
                        'venue': destination.get(venue_key, venue),
                        'details': f"error=missing_quick_qty | payload={_short_json(payload)}",
                    })
                    return self._json(400, error_payload)
                try:
                    quick_qty_num = abs(float(qty))
                except Exception:
                    error_payload = {'error': 'invalid_quick_qty', 'qty': qty, 'ticker': ticker, 'broker': broker}
                    _safe_append_journal({
                        'time': _utcnow_iso(),
                        'kind': 'quick-order',
                        'ticker': ticker,
                        'side': side,
                        'qty': qty,
                        'qtyUnit': _payload_qty_unit(payload),
                        'brokers': [broker],
                        'status': 'error',
                        'symbol': destination.get('symbol', symbol),
                        'venue': destination.get(venue_key, venue),
                        'details': f"error=invalid_quick_qty | payload={_short_json(payload)}",
                    })
                    return self._json(400, error_payload)
                if quick_qty_num <= 0:
                    error_payload = {'error': 'invalid_quick_qty', 'qty': qty, 'ticker': ticker, 'broker': broker}
                    _safe_append_journal({
                        'time': _utcnow_iso(),
                        'kind': 'quick-order',
                        'ticker': ticker,
                        'side': side,
                        'qty': qty,
                        'qtyUnit': _payload_qty_unit(payload),
                        'brokers': [broker],
                        'status': 'error',
                        'symbol': destination.get('symbol', symbol),
                        'venue': destination.get(venue_key, venue),
                        'details': f"error=invalid_quick_qty_nonpositive | payload={_short_json(payload)}",
                    })
                    return self._json(400, error_payload)

                destination['qtyMode'] = 'fixed'
                destination['side'] = side
                destination['qty'] = int(quick_qty_num) if float(quick_qty_num).is_integer() else quick_qty_num
                if broker == 'bingx':
                    destination['qtyKind'] = 'usdt'
                    destination['openQtyKind'] = 'usdt'
                payload['qty'] = destination['qty']
                if broker == 'bingx':
                    payload['qtyKind'] = 'usdt'

                route = {
                    'id': f'quick-{ticker}-{broker}',
                    'name': f'Quick order {ticker} -> {broker}',
                    'destinations': [destination],
                }

                decision = _enqueue_webhook_job(payload, route, config.get('defaultExecution', {}), origin='quick-order')
                _safe_append_journal({
                    'time': _utcnow_iso(),
                    'kind': 'quick-order',
                    'ticker': ticker,
                    'side': side,
                    'qty': destination['qty'],
                    'qtyUnit': _payload_qty_unit(payload),
                    'brokers': [broker],
                    'status': 'accepted',
                    'symbol': destination.get('symbol', symbol),
                    'venue': destination.get(venue_key, venue),
                    'details': (
                        _journal_units_hint(destination, str(destination.get('symbol') or symbol))
                        + (" | " if destination else '')
                        + f"jobId={decision.get('jobId','')}"
                        + f" | payload={_short_json(payload)}"
                        + f" | route={_short_json(route, 260)}"
                        + f" | queueDepth={decision.get('queueDepth')}"
                    ),
                })

                return self._json(202, {
                    'status': 'accepted',
                    'quickOrder': True,
                    'queued': True,
                    'jobId': decision.get('jobId'),
                    'orderQty': destination['qty'],
                    'queueDepth': decision.get('queueDepth'),
                    'result': decision,
                    'details': '',
                })
            except Exception as e:
                _safe_append_journal({
                    'time': _utcnow_iso(),
                    'kind': 'quick-order',
                    'ticker': ticker,
                    'side': side,
                    'qty': payload.get('qty', qty),
                    'qtyUnit': _payload_qty_unit(payload),
                    'brokers': [broker] if broker else [],
                    'status': 'error',
                    'symbol': (destination or {}).get('symbol', symbol),
                    'venue': (destination or {}).get(_broker_venue_key(broker), venue) if broker else venue,
                    'details': f"error={str(e)} | payload={_short_json(payload)} | traceback={_short_text(traceback.format_exc(), 1200)}",
                })
                return self._json(500, {
                    'error': 'quick_order_internal_error',
                    'details': str(e),
                    'ticker': ticker,
                    'broker': broker,
                })

        if self.path != '/webhook':
            return self._json(404, {'error': 'not_found'})

        length = int(self.headers.get('Content-Length', '0'))
        raw = self.rfile.read(length)
        raw_text = raw.decode(errors='replace')
        try:
            payload = json.loads(raw_text)
        except Exception as e:
            append_journal({
                'time': _utcnow_iso(),
                'kind': 'webhook',
                'ticker': '',
                'side': '',
                'qty': '',
                'brokers': [],
                'status': 'invalid_json',
                'details': f"{str(e)} | raw={_short_text(raw_text)}",
            })
            return self._json(400, {'error': 'invalid_json', 'details': str(e)})

        missing = [k for k in ['sourceTicker', 'side', 'qty'] if k not in payload]
        if missing:
            append_journal({
                'time': _utcnow_iso(),
                'kind': 'webhook',
                'ticker': str(payload.get('sourceTicker') or payload.get('ticker') or ''),
                'side': str(payload.get('side') or ''),
                'qty': payload.get('qty', ''),
                'qtyUnit': _payload_qty_unit(payload),
                'brokers': [],
                'status': 'missing_fields',
                'details': f"missing={','.join(missing)} | payload={_short_json(payload)}",
            })
            return self._json(400, {'error': 'missing_fields', 'missing': missing})

        register_observed_signal(payload)
        config = load_config()
        route = match_route(payload, config)
        if not route:
            append_journal({
                'time': _utcnow_iso(),
                'kind': 'webhook',
                'ticker': payload.get('sourceTicker'),
                'side': payload.get('side'),
                'qty': payload.get('qty'),
                'qtyUnit': _payload_qty_unit(payload),
                'brokers': [],
                'status': 'route_not_found',
                'details': f"payload={_short_json(payload)}",
            })
            return self._json(404, {'error': 'route_not_found', 'message': 'Ticker observed and added to admin UI'})

        materialized = materialize_route(payload, route, config)
        decision = _enqueue_webhook_job(payload, materialized, config.get('defaultExecution', {}))
        return self._json(202, decision)


def main():
    analytics_init = {'ok': False, 'path': str(ANALYTICS_DB_PATH), 'error': ''}
    try:
        analytics_init = init_analytics_db()
    except Exception as e:
        analytics_init = {'ok': False, 'path': str(ANALYTICS_DB_PATH), 'error': str(e)}
    server = ThreadingHTTPServer((SERVER_HOST, SERVER_PORT), Handler)
    for idx in range(WEBHOOK_WORKER_COUNT):
        threading.Thread(target=_webhook_worker_loop, name=f'webhook-worker-{idx+1}', daemon=True).start()
    threading.Thread(target=_metrics_sync_loop, name='broker-metrics-sync', daemon=True).start()
    build = _build_summary()
    append_journal({
        'time': _utcnow_iso(),
        'kind': 'server-start',
        'ticker': '',
        'side': '',
        'qty': '',
        'brokers': [],
        'status': 'ok' if analytics_init.get('ok') else 'degraded',
        'details': f'listening on http://{SERVER_HOST}:{SERVER_PORT}/ pid={os.getpid()} workers={WEBHOOK_WORKER_COUNT} metricsSync={METRICS_SYNC_INTERVAL_SECONDS}s builtAt={build.get("builtAt","")} fileCount={build.get("fileCount","")} analyticsDb={analytics_init.get("path","")} analyticsOk={analytics_init.get("ok")} analyticsError={analytics_init.get("error","")[:200]}',
        'version': build.get('version', 'unknown'),
        'serverHash': build.get('serverHash', ''),
    })
    print(f'listening on http://{SERVER_HOST}:{SERVER_PORT}/ workers={WEBHOOK_WORKER_COUNT}')
    server.serve_forever()


if __name__ == '__main__':
    main()
