# Changelog

## 2026.04.15-43
- Improved Schwab quick-order confirmation: capture `order_id` and `Location` from successful Schwab `place_order` responses, and include the order ID in journal details so accepted orders are traceable.
- Fixed Schwab broker metrics sync to request account positions explicitly, so the admin UI `pos` field can reflect actual Schwab holdings instead of staying blank due to missing positions data.
- Clarified the operational meaning of Schwab `201`: accepted order creation, not guaranteed fill, so UI/journal now have better evidence for follow-up checks.

## 2026.04.15-42
- Tightened Schwab quick-order status handling so journal/UI only show `ok` on explicit successful broker HTTP status, and journal details now keep the Schwab status code for visibility.
- Fixed narrow-browser admin layout so margin/GO hints no longer spill into the next broker cell and block `buy` clicks.
- Changed the top manual row to be ticker-only: broker cells stay inactive until the ticker is added to the list, then broker routing is edited on the normal row.

## 2026.04.15-41
- Added a manual first row under the broker header so a user can create a new routing rule without waiting for an incoming webhook ticker.
- The manual row has a master checkbox in the ticker cell to activate the row, then per-broker symbol/venue/qty inputs work through the same lookup syntax and `Save` flow as normal rows.
- Saving the manual row creates a regular UI-managed route and reloads the page so the new ticker appears in the standard table immediately.

## 2026.04.15-40
- Fixed a UI render crash introduced after 2026.04.15-39: the admin page referenced `margin_symbol` before assignment while resolving displayed broker position qty.
- This crash surfaced as `502 Bad Gateway` after successful login because nginx could not reach a healthy response from the Python app when rendering the main UI.
- No behavior change beyond restoring the admin page after login.

## 2026.04.15-39
- Refined default qty seeding for newly discovered tickers: when no routing rule exists yet, broker-synced open position size can seed the default qty shown for editing.
- Preserved the post-save behavior from the previous fix: once a route exists, live broker position data no longer overwrites the saved order qty semantics.
- This matches the intended workflow: discover a new ticker, start from the live open position, adjust if needed, then save and continue with stable route-managed qty.

## 2026.04.15-38
- Fixed quick-order default qty seeding so broker-synced live positions no longer populate the saved order size. Only real observed webhook payloads can seed default order qty for new tickers.
- Improved UI position lookup by checking multiple symbol candidates (`symbol`, `ticker`, margin symbol) before leaving the broker position field empty.
- This prevents open positions discovered from the broker from masquerading as quick-order size while making the position field more reliable for non-hooked holdings.

## 2026.04.15-37
- Fixed journal details line breaks in the web UI by rendering formatted detail separators as real HTML `<br>` line breaks instead of relying on plain newline preservation.
- This keeps raw jsonl logs unchanged while making `payload`, `request`, `result`, `error`, and `body` sections visibly stacked in the journal page.

## 2026.04.15-36
- Improved journal detail readability in the web UI: `payload`, `request`, `result`, `error`, `route`, `exec`, and `body` sections now render on separate lines instead of one long collapsed string.
- Added wrapped, preformatted details cells in the journal so multiline broker errors remain readable without changing the raw jsonl log format.

## 2026.04.15-35
- Fixed the UI quantity semantics mismatch: the visible small field in each broker cell now represents current broker position only and is no longer saved as the route order size.
- The quick-order qty field between `buy` / `sell` is now the persisted order size for the broker route, so `Save` locks in the same qty that manual quick orders use.
- New / unseen tickers now seed their default order qty from the last observed webhook payload qty instead of silently falling back to unrelated live position data.

## 2026.04.15-34
- Fixed Finam order placement to use the current REST schema (`symbol`, `side`, `quantity`, `type`, `timeInForce`) instead of the obsolete `securityBoard` / `securityCode` / `buySell` payload.
- Added Finam symbol normalization so routes that still store `symbol=NGJ6` with `exchange=RTSX` are sent to the API as `NGJ6@RTSX`, matching Finam's expected `ticker@mic` format.
- This should eliminate the misleading `Ticker must not be empty` error for valid futures routes and let Finam reach actual broker-side validation/execution.

## 2026.04.15-33
- Fixed webhook journal status handling for workspace-executor brokers like Alor: list-shaped execution results no longer crash the background worker with `'list' object has no attribute 'get'`.
- Multi-destination routes are no longer written as combined journal rows on `accepted` or worker-fallback errors. Each destination now gets its own journal row, so `alor, finam` mixed lines are eliminated.
- Destination execution is now sequential instead of concurrent, matching the expectation that separate broker rules should be processed one after another.

## 2026.04.14-32
- Fixed a queue worker reliability hole: unexpected exceptions inside webhook background processing no longer kill the worker thread silently.
- Added an immediate `accepted` journal entry at webhook enqueue time, so a signal is visible in the journal even before broker execution starts.
- Added fallback worker error logging for queued jobs that fail before normal execution journaling, preventing silent drops where `/webhook` returns 202 but nothing reaches the brokers or journal.

## 2026.04.14-30
- Fixed Finam live execution default: Finam destinations no longer fall back to `dryRun=true` unless the route or payload explicitly requests dry-run.
- Fixed Alor exchange normalization for the embedded smart executor: legacy `FORTS` routing values are now normalized to `MOEX` before orderbook/order calls, so the Alor OpenAPI no longer rejects the exchange value in this path.
- This should restore the intended behavior for mixed Alor+Finam fan-out routes: Alor reaches the real execution path, Finam no longer silently stays in simulation mode.

## 2026.04.14-29
- Fixed journal fan-out logging: multi-destination webhook executions no longer create one mixed summary execution row across several brokers.
- For fan-out routes, execution rows are now written only per destination/broker so each broker keeps its own status, request, and error context.
- Summary `webhook` rows remain only for single-destination execution cases or true top-level execution failures.

## 2026.04.14-28
- Added calm journal row highlighting for `server-start` so restart boundaries are immediately visible in the log view.
- Added a separate subtle background for `error` / `execution_error` style rows so failures stand out without turning the journal into a noisy alarm board.
- Kept normal `executed` / `dry_run` rows on the default background so the eye is drawn to restart and failure boundaries first.

## 2026.04.14-27
- Moved webhook execution off the request thread: `/webhook` now returns immediately with HTTP 202 and queued job metadata while background workers execute broker routes.
- Switched route execution to concurrent per-destination tasks with hard timeouts so one slow broker no longer blocks the entire webhook or the main HTTP loop.
- Added periodic background broker metrics/position sync and automatic publishing of live broker symbols into observed tickers so externally opened positions show up in the admin UI after refresh/reload.
- Server now runs with `ThreadingHTTPServer`, background webhook workers, and richer `/healthz` runtime info (`queueDepth`, `metricsUpdatedAt`).

## 2026.04.14-26
- Made fan-out webhook summary status honest: the top-level webhook result now reflects `executed`, `dry_run`, or `partial_error` based on per-destination outcomes instead of pretending the whole multi-broker run was a single uniform success/failure.
- This complements the per-destination journal split so mixed broker outcomes are visible both in detail rows and in the summary status.

## 2026.04.14-25
- Split fan-out webhook execution logging into separate journal entries per destination.
- In addition to the summary `webhook` row, the journal now writes `webhook-destination` rows so Alor/Finam/Bybit/etc. results no longer collapse into one mixed status line.

## 2026.04.14-24
- Improved deep broker error diagnostics for the embedded Alor/Finam smart executor.
- HTTP failures now include the execution stage (`auth`, `orderbook`, `order`, etc.) and a response body fragment, so cases like insufficient funds/margin should surface in the journal instead of collapsing into bare `Orderbook failed: 400`.

## 2026.04.14-23
- Added automatic per-broker execution debug logging into webhook journal entries.
- Webhook journal `details` now include the payload, materialized route fragment, and broker-separated request/error context so live debugging no longer requires manual scripts on the server.

## 2026.04.14-22
- Extended broker `sync` so it now also tries to load live lookup symbols/venues into broker config instead of only running a connection test.
- Added initial live instrument universe loading for Bybit, Alor, and Finam; lookup lists are merged into `routing.json` and then reused by the admin UI.
- Schwab lookup sync is still marked not supported yet.

## 2026.04.14-21
- Changed admin UI/save defaults so broker symbol/venue are populated from the instrument catalog first, not blindly from the raw webhook ticker.
- New or updated routes now prefer broker-specific catalog mappings (for example `NGJ6` -> `NG-4.26` on Alor) when present.

## 2026.04.14-20
- Added journal entries for UI routing changes (`routing-save`) so creating/updating/removing a mapping is visible in `/journal`.
- Added UI route activity timestamps (`uiCreatedAt`/`uiUpdatedAt`) and changed row sorting so active/recently-touched tickers stay at the top while stale/no-routing rows sink downward.

## 2026.04.14-19
- Added the first broker-specific instrument catalog hint layer in the admin UI.
- Each broker cell now computes nearest catalog candidates from `config/instruments.json` and shows the best current suggestion/hint (including proxy/manual-review/fallback markers) instead of an empty subhint.

## 2026.04.14-18
- Fixed the new centralized settings layer for boolean defaults: `ALOR_ALLOW_MARGIN` now defaults/render-saves as `true` instead of appearing as `false` when unset.
- Empty bool inputs now fall back to the field default instead of silently normalizing to `false`.

## 2026.04.14-17
- Refactored settings render/save flow to use a single shared settings registry instead of ad-hoc field wiring in multiple places.
- Removed the special `BYBIT_BIND_INTERFACE` shadow-field hack and added save read-back verification so the UI no longer claims success if env values were not actually persisted.
- New settings like `ALOR_ALLOW_MARGIN` now go through the same centralized render/save path as the rest.

## 2026.04.14-16
- Made Alor `allowMargin` configurable through settings/UI via `ALOR_ALLOW_MARGIN=true|false`.
- Wired the Alor executor to read that env setting instead of hardcoding `allowMargin: true`.

## 2026.04.14-15
- Added broker-specific `allowMargin: true` to Alor order payloads in the embedded smart executor.
- This applies only to Alor order requests and is not used for Bybit, Finam, or Schwab.

## 2026.04.14-14
- Expanded webhook journal diagnostics: invalid JSON, missing fields, route-not-found, and execution results now include the accepted payload/raw body snippet in `details`.
- This makes it easier to see what TradingView actually sent when a webhook is malformed or routed unexpectedly.

## 2026.04.14-13
- Applied the quick-qty rule consistently across broker cells: per-cell quick qty is authoritative, row-level default qty is fallback only.
- Prevented broker metrics refresh from overwriting a manually entered quick qty with the row-level default when the quick input is not focused.

## 2026.04.14-12
- Fixed Bybit POST signing so the signed payload string matches the actual JSON body sent on order placement.
- Fixed quick-order quantity source: the per-cell quick qty input now takes priority over the row-level default qty field.

## 2026.04.14-11
- Fixed quick-order UX/status so it no longer reports `ok` blindly when the broker-level response is actually a reject/error.
- Quick-order journal entries now record broker-level execution errors in `details` and use `execution_error` instead of unconditional `ok`.
- Live broker-cell lookup now uses the current symbol input value instead of only the stale `data-broker-symbol` attribute.

## 2026.04.14-10
- Added an explicit fallback save path for `BYBIT_BIND_INTERFACE` using a shadow hidden field copied on form submit.
- This is a targeted workaround for the still-unreliable browser submit behavior on the settings page for that field.

## 2026.04.14-09
- Fixed invalid nested forms on the settings page by moving backup forms outside the main settings save form.
- This addresses settings fields that were still not reliably persisting (including `BYBIT_BIND_INTERFACE`) despite prior blur-on-submit handling.

## 2026.04.14-08
- Fixed settings form submit behavior so the currently focused field is blurred/committed before save; this fixes cases where freshly typed values (including `BYBIT_BIND_INTERFACE`) were reset or not saved unless Enter/blur happened first.

## 2026.04.14-07
- Added configurable `BYBIT_BIND_INTERFACE` setting and settings UI field.
- Taught `bybit_adapter.py` to send Bybit requests through a specific Linux interface using an interface-bound HTTP adapter.
- Extended Bybit sync diagnostics to show the bound interface in journal/details when used.

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
