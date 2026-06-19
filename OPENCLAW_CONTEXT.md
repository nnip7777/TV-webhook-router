# webhook-router — fast context for future sessions

This file exists to minimize token spend when resuming work on this project.
Read this first before scanning large code/docs.

## Project
- Path: `/Users/nik/strategy/webhook-router`
- Goal: local webhook router / execution service with broker adapters, routing rules, analytics, quick orders, and versioned patch deploys.

## Default resume order
1. Read this file.
2. If task is about deploy/versioning, read `README.md`, `VERSION`, `CHANGELOG.md` only if needed.
3. If task is about execution flow, inspect only:
   - `app/server.py`
   - `app/execution.py`
   - `external/smart_order_executor.py`
   - relevant adapter file only if necessary
4. Avoid broad repo scans unless the task clearly needs them.

## Versioned patch workflow
- This project uses a simple built-in patch workflow.
- Key files:
  - `VERSION`
  - `BUILD.json`
  - `CHANGELOG.md`
  - `scripts/generate_build_manifest.py`
  - `scripts/create_patch.py`
- Standard release flow:
  1. make code changes
  2. bump `VERSION`
  3. prepend entry to `CHANGELOG.md`
  4. run `python3 scripts/generate_build_manifest.py`
  5. run `python3 scripts/create_patch.py`
- Patch output path:
  - `patch/<version>`

## Preferred production deploy workflow
If patch is already built locally, give only these 2 command batches:

1. Upload patch:
```bash
scp -r patch/<version> root@72.56.246.125:/opt/webhook-router/patch/
```

2. Apply on VPS:
```bash
cd /opt/webhook-router && python3 scripts/apply_patch.py patch/<version> /opt/webhook-router && systemctl restart webhook-router && sleep 2 && curl -fsS http://127.0.0.1:8787/healthz
```

- VPS: `root@72.56.246.125`
- Deploy root: `/opt/webhook-router`

## Execution architecture quick map
- `app/server.py`
  - `/webhook` intake
  - route matching
  - `materialize_route(...)`
  - queues job and later calls `execute_route_sync(...)`
- `app/execution.py`
  - broker-specific execution entrypoints
  - `_execute_via_workspace_executor(...)`
  - `_execute_finam(...)`
  - `execute_route_sync(...)`
- `external/smart_order_executor.py`
  - sequential order placement logic for Alor/Finam
  - order book read / limit price selection / iterative execution
- `app/finam_adapter.py`
  - simpler direct Finam order path
  - use only when task is about direct non-target execution path

## Finam + Alor target-direction behavior
This is a durable product decision.

Signals like `2long` / `2short` for Finam and Alor must behave like this:
- opposite position exists -> close it fully first
- same-side position exists -> add/increase
- flat -> open target side

Sizing rules:
- close size = actual current opposite position size
- open/add size = saved route/destination `qty`
- do NOT use BingX crypto-style USDT sizing for Finam/Alor

Execution rules:
- use LIMIT orders, not market orders
- execute sequentially, one order at a time
- for each step:
  1. read order book
  2. place one limit order
  3. inspect effect / re-read position
  4. only then decide whether another order is needed
- do NOT pre-place batches like "5 orders at once"

## Current implementation status
As of version `2026.06.10-160`:
- Finam target-direction was routed into the shared smart executor path
- Alor and Finam share the stepwise target-direction logic in `external/smart_order_executor.py`
- patch `2026.06.10-160` was built locally

## Practical code-reading hints
- If user asks "разобрался с версионостью / делай патч": you usually only need:
  - this file
  - `README.md`
  - `VERSION`
  - `CHANGELOG.md`
  - changed files
- If user asks about Finam/Alor execution semantics: read this file first, then inspect:
  - `app/execution.py`
  - `external/smart_order_executor.py`
- Avoid re-reading unrelated analytics/UI files unless request is specifically about them.

## Known preferences for this project
- User prefers concise factual status, not long preambles.
- If patch is already built, return only the 2 deploy command batches.
- When discussing deploy, do not add extra local build commands unless explicitly asked.
