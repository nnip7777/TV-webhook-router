# Changelog

## 2026.04.14-06
- Fixed Finam sync diagnostics regression: imported `FINAM_ACCOUNT_ID` into `server.py` so `settings-sync` no longer fails with `name 'FINAM_ACCOUNT_ID' is not defined`.

## 2026.04.14-05
- Generalized broker sync diagnostics for all brokers: `settings-sync` journal entries now prefer structured `details`, not just `text`.
- Added richer sync details for Alor, Bybit, Finam, and Schwab so failed/ok checks are easier to diagnose from the journal.

## 2026.04.14-04
- Improved Bybit broker sync diagnostics: journal/details now include HTTP status, retCode, retMsg, and raw response fragment when sync fails.

## 2026.04.14-03
- Fixed broker `sync` buttons to submit reliably from the settings page form.
- Journal now displays timestamps in local/server time format instead of raw UTC ISO strings.
- Replaced newly added UTC timestamp writes with a shared `_utcnow_iso()` helper.

## 2026.04.14-02
- Extended journal table to show version, server hash, and details columns.
- Improved `server-start` journal entries to include build-related startup details (`builtAt`, `fileCount`, pid, listen address).
- Regenerated build manifest for the updated server build.

## 2026.04.14-01
- Added journal visibility for invalid webhooks (`invalid_json`, `missing_fields`).
- Added journal entries for settings saves (`settings-save`) and broker sync tests (`settings-sync`).
- Added journal entry for server start (`server-start`).
- Added build/version infrastructure: `VERSION`, `BUILD.json`, and build manifest generator.
- Planned surfacing of build info in UI, `/healthz`, and startup journal.
