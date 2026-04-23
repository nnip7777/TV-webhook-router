# Changelog

## 2026.04.14-01
- Added journal visibility for invalid webhooks (`invalid_json`, `missing_fields`).
- Added journal entries for settings saves (`settings-save`) and broker sync tests (`settings-sync`).
- Added journal entry for server start (`server-start`).
- Added build/version infrastructure: `VERSION`, `BUILD.json`, and build manifest generator.
- Planned surfacing of build info in UI, `/healthz`, and startup journal.
