## 2026.05.01-141
- Added a short retry window when fetching BingX post-trade fills/fees from `allFillOrders` and `allOrders`, to handle exchange-side lag where trade history is not immediately visible right after execution.
- Kept polling-delta fills only as the final fallback when BingX still does not expose the real fee rows after the retry window.

## 2026.05.01-140
- Added BingX commission/fill retrieval from the real futures API: primary source is `/openApi/swap/v2/trade/allFillOrders`, with `/openApi/swap/v2/trade/allOrders` as a fallback when detailed fill rows are unavailable.
- Updated BingX execution analytics to prefer actual fill rows and actual fee amounts from API responses instead of inferring commission from order polling snapshots.
- Normalized BingX fee signs so negative exchange fee values are stored as positive commission costs in analytics.

## 2026.05.01-139
- Replaced the disabled `/effectiveness` stub with a lightweight page that loads a static shell and fetches a small JSON overview from SQLite on demand.
- Added `/api/effectiveness-overview` so the page can show linked counters, the latest signal/execution, recent fills, recent round-trips, and precomputed daily aggregates without rebuilding analytics.
- Added incremental `analytics_counters` tracking so the page can show table growth without full-table recounts on every request.
- Added Effectiveness links to the admin/settings navigation.

## 2026.05.01-138
- Added a new SQLite analytics layer in `app/analytics.py` with linked `signals`, `executions`, `orders`, `fills`, `open_lots`, `round_trips`, `round_trip_fills`, and `daily_trade_stats` tables for durable trade economics analysis.
- Hooked analytics writes into the normal execution path without startup backfill: server startup only initializes the schema, while new executions append incrementally from live data.
- Extended BingX maker execution logging to persist per-attempt observed fill parts, timestamps, average fill price, and commissions so later lot-size and commission analysis uses real execution fragments instead of theoretical quotes.
- Added `WEBHOOK_ROUTER_ANALYTICS_DB_PATH` setting and included `app/analytics.py` in build/patch packaging.

## 2026.04.30-137
- Removed the remaining patch 132 effectiveness payload overhead from `app/server.py`: journal writes no longer duplicate `tradeSummary` request/result/legs blobs on every execution record.
- Dropped the unused effectiveness parsing/cache scaffolding left behind after the page was disabled, reducing request-path and journal-side overhead without touching execution logic.
- Kept `/effectiveness` as an explicit disabled stub page until a separate safe redesign lands.

## 2026.04.28-131
- Fixed BingX target-direction hedge detection for non-index symbols like GOLD: target-direction now always checks/forces dual-side position mode and runs the close-opposite-before-open flow, instead of silently falling back to a plain maker open order.

## 2026.04.28-130
- Fixed server startup regression in 129 by importing `Decimal` for journal payload sanitization.

## 2026.04.28-129
- Journal entries are now sanitized before JSON encoding, so diagnostic payloads with `Decimal` values no longer degrade into stringified blobs and raw BingX state can be inspected reliably.
- Added full raw BingX position payload snapshots (`positionPayload*Raw`) around target-direction close/open phases to debug cases where `get_positions(symbol)` appears to miss an existing opposite leg.

## 2026.04.28-127
- Reverted the incorrect BingX `target-direction` idempotency change: repeated same-direction signals must still increase the existing target-side leg.
- Continued hardening of target-direction close/reconcile flow remains in place; repeated signals should only fail when an opposite leg cannot actually be flattened.
- Added raw per-symbol BingX position row snapshots to execution journaling before close, after close, before target open, and after target open to catch partial or delayed `get_positions()` responses.
- Fixed journal/error classification so broker API failures like Bybit `retCode != 0` are recorded as `execution_error` instead of misleading `placed` successes.
- Added Bybit target-direction flip handling: the router now inspects live positions, closes the opposite leg with a reduce-only market order on the proper `positionIdx`, then opens/increases the target-side leg instead of sending a blind one-step order.
- Hardened BingX maker repost loop against live hanging orders: `PENDING` is now treated as a cancel-required state, and the router verifies cancel outcome instead of silently leaving a stray open order on the book.
- Fixed a critical BingX target-direction close-retry bug: remaining opposite-leg close passes now use the true close side and close `positionSide`, instead of accidentally retrying toward the target side and turning a flip into a stray maker open order.

## 2026.04.28-126
- Hardened BingX target-direction execution against fractional-contract drift by carrying close-leg remaining quantities as `Decimal` buckets and quantizing close retries upward to executable contract precision.
- Added an explicit flat-before-open gate plus final post-open position reconcile, so directional execution now fails if the opposite leg still exists or if the target leg does not become the only live side.
- Added depth-aware price planning metadata from BingX order book and surfaced it in execution logs; current placement still reposts limits, but now records whether visible depth covered the requested size for later tuning/splitting.

## 2026.04.27-125
- Fixed BingX target-direction execution so opposite-leg close is retried against the live remaining position until the actual opposite side reaches zero.
- Kept BingX semantics explicit: opposite-leg closing is done in contracts, while target-direction open is executed in USDT.
- Extended BingX journal details with target-direction close/open pass diagnostics (`targetClosePasses`, `targetOpenPasses`, `closeStillOpenQty`, `targetOpenQtyKind`).

## 2026.04.26-124
- Fixed a real Python scoping bug in BingX execution: inner reassignment of `open_qty_kind` inside `_run()` made earlier references raise `UnboundLocalError` on step-side quick orders.
- Renamed the target-direction open leg variable to keep routed quick-order `qty/qtyKind` behavior from 123 intact while eliminating the scope collision.

## 2026.04.26-123
- Fixed BingX execution regression in non-target-direction quick orders where `openQtyKind` could be referenced before initialization.
- Preserved the 122 semantics: quick-order fixed `qty` still comes from route, and request `qtyKind` still follows the routed destination.

## 2026.04.26-122
- Fixed BingX quick-order target-direction semantics so request `qtyKind` stays `usdt` when the routed destination is configured that way.
- Preserved `route.qty` as the source of truth for final request quantity while still keeping `openQtyKind` explicit for prepare/open stages.

## 2026.04.26-121
- Fixed BingX target-direction execution to preserve routed fixed `qty` and `qtyKind` instead of overwriting them from the incoming webhook payload.
- `openQtyKind` now remains separate, so target-direction flows can still open in quote mode when configured without corrupting the logged/requested final qty.

# Changelog

## 2026.04.17-59
- Added BingX split-tunnel helper scripts for macOS and Linux so BingX hosts can be routed outside a full-tunnel VPN without changing the rest of the system traffic.
- Documented the BingX network-routing approach in `README.md`, including the macOS host-route method and the Linux combination of host routes plus optional `BINGX_BIND_INTERFACE`.
- Included the new split-tunnel scripts in build and patch packaging so the routing helpers ship with the deployed patch.

## 2026.04.17-58
- Added first-class BingX Perpetual broker support through a dedicated adapter and execution path, without routing BingX orders through the legacy smart executor.
- BingX execution is limit-only: `market` mode is rejected, one signal produces one order, and when no explicit price is provided the router reads the top of book and uses best ask for `buy` or best bid for `sell`.
- Added BingX admin/config wiring: env/settings fields, broker defaults for older routing configs, lookup sync from BingX contracts, connection test, live balance/position metrics, quick-order support, and patch/build packaging for the new adapter.

## 2026.04.17-57
- Restricted `smart_order_executor` to limit-only operation. `use_limit=false` now fails fast with an explicit error instead of attempting any market-order path.
- Kept the corrected one-signal-one-order model from `2026.04.17-56`: one incoming signal reads the book once, takes the best current price for the side, and places exactly one limit order.
- This supersedes `2026.04.17-56`, which still carried an unnecessary market-order branch.

## 2026.04.17-56
- Removed the incorrect split-order model from `smart_order_executor`: one incoming signal now produces exactly one broker order.
- Limit execution now reads the live order book once for that one order, takes the best current level for the side (`buy` -> best ask, `sell` -> best bid), and places a single limit order at that price.
- Market execution now actually uses market order methods instead of silently reusing limit-order placement logic.
- This supersedes `2026.04.16-55`, which still assumed one signal could fan out into multiple internally generated orders.

## 2026.04.16-55
- Refined Alor smart limit placement for split orders: before each child limit order the router now re-reads the live order book and recalculates the target limit price from the fresh best bid/ask.
- Split logic now divides only by quantity; price is no longer precomputed once for the whole batch and then reused for later child orders.
- This supersedes `2026.04.16-54`, because child orders must track the moving book instead of reusing a stale earlier level.

## 2026.04.16-54
- Corrected Alor NG price normalization: NG now uses `0.001` tick size instead of the incorrect `0.005` assumption.
- Removed the midpoint pricing logic for smart limit orders. The router now takes the best current book level directly for the requested side instead of averaging bid/ask.
- Removed split-order price drifting: split child orders now keep the same selected book price instead of shifting each child by ±0.1%.
- This supersedes `2026.04.16-53`, which fixed the `symbol` NameError but still preserved the wrong pricing model.

## 2026.04.16-53
- Fixed the Alor smart-order regression that still raised `name 'symbol' is not defined` during orderbook analysis for maker executions.
- `SmartOrderExecutor.analyze_orderbook()` now explicitly accepts `symbol` and uses the real routed instrument when rounding bid/ask prices to valid Alor tick sizes.
- This is the patch to deploy for the current NG/Alor execution failure (`NGJ6` -> `NG-4.26`).

## 2026.04.15-52
- Fixed all syntax errors in Alor order processing: removed escape characters from function calls and definitions.
- Completed correction of split_order function signature and usage throughout the codebase.
- All price validation functions now work correctly with proper symbol parameter handling.

## 2026.04.15-51
- Fixed critical syntax error in Alor order processing: removed erroneous escape characters from function signatures.
- Corrected split_order function definition to properly compile and execute.
- Resolved compilation issues that prevented patch application.

## 2026.04.15-50
- Fixed syntax error in Alor order processing: resolved escape character issues in function signatures.
- Corrected split_order function to properly handle symbol parameter for price validation.
- Ensured all price adjustments now correctly use symbol-specific tick size validation.

## 2026.04.15-49
- Fixed critical bug in Alor order processing: resolved 'symbol not defined' error in price rounding function.
- Corrected split_order function signature to properly accept symbol parameter for price validation.
- Ensured all price adjustments now correctly use symbol-specific tick size validation.

## 2026.04.15-48
- Fixed Alor limit order price validation: corrected logic to ensure prices conform to exchange tick sizes.
- Enhanced order placement to validate and adjust prices from orderbook before sending to Alor.
- Resolves "price not multiple of minimum step" errors that occurred when placing limit orders.

## 2026.04.15-47
- Fixed Alor limit order price precision: added automatic rounding to correct tick sizes to prevent "price not multiple of minimum step" errors.
- Implemented `_round_alor_price()` helper function that adjusts prices for specific symbols like NG futures (0.005 tick size).
- Resolves issue where Alor would reject limit orders due to incorrect price formatting.

## 2026.04.15-46
- Fixed Schwab quick-order result semantics: `REJECTED/CANCELED/EXPIRED` no longer appear as `accepted`; they now return error state in UI and journal.
- Added explicit rejected-state badge in the position cell: pending orders still show yellow `WORK`, but rejected Schwab orders now show red `REJ`.
- Kept the reject detail text in the broker cell so pending/rejected order outcomes remain readable without opening the journal.

## 2026.04.15-45
- Added transient Schwab pending-order state to broker metrics: when the latest Schwab order is created but not filled, the UI now shows `WORK` in `pos` with a yellow badge instead of leaving the field blank.
- Enriched Schwab quick-order handling so pending statuses like `WORKING` / `QUEUED` survive into the metrics refresh path until a real position appears.
- Replaced disabled broker controls in the top manual add-ticker row with compact broker info cards showing local time, local session status, working hours, markets, target, and live/dry-run sync mode.

## 2026.04.15-44
- Added immediate Schwab order lookup after successful quick-order placement so the result includes `order_status` alongside `order_id`.
- Changed Schwab quick-order semantics from false `ok` to `accepted` when the broker created the order but the status is not yet filled/executed.
- Surfaced Schwab `order_status` in journal/details and quick-order flash messages, making pre-market or pending orders visible instead of looking completed.

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
