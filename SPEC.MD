## Session Boot Rules
Always run chcp 65001 in PowerShell as the first command so the console uses UTF-8. Apply UTF-8 encoding for all file and console read/write/print operations.
Immediately set [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 and export PYTHONIOENCODING=utf-8 (plus similar flags for other runtimes) so downstream tools avoid mojibake.
Use python virtual env "trend" for backend

## Workflow
After user approval, commit changes using Git.
Log every new requirement in this SPEC.MD file for future reference.
Whenever a Python package is installed or upgraded, update requirements.txt to keep dependency pins in sync.
Frontend assets (HTML/CSS/JS) must be saved as UTF-8 without BOM to avoid rendering garbled characters in Chinese UI text.

## Lessons Learned
- Refresh cache aggressively when frontend scripts change (e.g., bump query string version) so browsers pick up the latest fixes immediately.
- Normalize external data encodings before persistence; AkShare responses may need `latin1` → `utf-8` decoding to prevent downstream mojibake.
- Always verify language preference persistence after any localization refactor by navigating across pages; regression in one page often hints at missing storage/attribute fallbacks.
- Start database services explicitly (admin PowerShell `Start-Service postgresql-x64-18` or `pg_ctl`) before running sync jobs to avoid connection timeouts; lack of DeepSeek results from a down DB, not AI issues.
- Shut down VS Code (or restart the Python language server) before stopping PostgreSQL so the editor releases connections and doesn’t leave orphaned `postgres.exe` backends attached to log files.
- PostgreSQL connection counts climb when external tools leave sessions idle—inspect with `pg_stat_activity` and terminate stale `trend_view` backends before blaming DAO code.
- DAO helpers now enforce managed connections with explicit close + transaction handling; keep statement/idle timeout values tuned in `settings.*.json` so stray sessions get reclaimed automatically.
- When editing frontend assets that contain non-ASCII text, preserve UTF-8 encoding (prefer escaped literals if tooling is unreliable) and verify syntax immediately after the change.
- Avoid bulk encoding conversions on large files unless a backup exists; work on copies or use targeted replacements to prevent irreversible corruption.
- After any change that touches encoding-sensitive code, reload the page/run the bundler right away so unexpected “unexpected token” errors surface early.
- Confirm Git/history availability before destructive edits; when in doubt, stash or make a safety commit prior to risky transformations.

## Documentation Standards
Keep all Markdown files (e.g., README.md, SPEC.MD) written entirely in English, even if requirements are delivered in another language.

## Feature Log
- 2025-10-20 (FEATURE frontend-002): Added control panel with scheduled sync automation, runtime configuration toggles, and manual job triggers.
- 2025-10-28 (FEATURE frontend-009): Refreshed stock detail experience with hero metrics, localized summaries, ECharts-powered candlestick visualization (last-year scope with volume bars and empty-state fallback), and removal of the trading snapshot card.
- 2025-10-29 (REQUIREMENT backend-011/frontend-010): Track and display Tushare业绩快报与业绩预告数据；新增同步流程、控制面板卡片，以及Market菜单下的列表入口。
- 2025-10-30 (REQUIREMENT backend-012): Migrated performance express sync from Tushare to AkShare `stock_yjkb_em` and rebuilt the persistence table schema to match the new dataset.
- 2025-10-30 (REQUIREMENT backend-013): Migrated performance forecast sync from Tushare to AkShare `stock_yjyg_em` with a redesigned schema aligned to the new data shape.
- 2025-10-30 (FEATURE backend/frontend-019): Added industry fund flow ingestion via AkShare `stock_fund_flow_industry`, new fund-flow control card, and front-end industry fund flow dashboard with ranking filters.
- 2025-10-30 (FEATURE backend/frontend-020): Added concept fund flow ingestion via AkShare `stock_fund_flow_concept`, extended control panel fund-flow group, and delivered concept fund flow dashboard with ranking tabs.
- 2025-10-30 (FEATURE backend/frontend-021): Added individual stock fund flow ingestion via AkShare `stock_fund_flow_individual`, new fund-flow sync control, and dedicated dashboard with ranking tabs and per-stock metrics.
- 2025-10-30 (FEATURE backend/frontend-022): Added big deal tracking via AkShare `stock_fund_flow_big_deal`, including persistence schema/DAO, control panel sync card, dashboards, and stock-detail integrations for big-deal and individual fund flow snapshots.
- 2025-10-30 (FIX backend-022): Hardened fund flow DAO upserts by de-duplicating conflict keys per batch and wiring date column metadata to avoid `ON CONFLICT` and signature errors during control-panel sync jobs.
- 2025-11-04 (FEATURE backend/frontend-045): Added Shanghai/Shenzhen-Hong Kong connect fund flow ingestion via AkShare `stock_hsgt_fund_flow_summary_em`, new control panel card, aggregate job integration, and dedicated dashboard with channel/direction filters.
- 2025-11-04 (REQUIREMENT backend/frontend-052): Deliver a news-driven "Sector Insight" module that aggregates classified headlines by industry/sector/theme, prepares AI reasoning payloads, and surfaces a dedicated view under the News navigation.
- 2025-11-08 (FEATURE backend/frontend-053): Added sector insight reasoning (control card + News page) powered by DeepSeek reasoner, including new REST endpoints, snapshot rendering, and manual sync hooks in the control panel.
- 2025-11-10 (FEATURE backend/frontend-108): Delivered concept insight pipeline combining fund-flow hotlists, Tushare concept index history, curated news, and DeepSeek reasoner output; exposed `/market/concept-insight` APIs, a control panel trigger, and a concept insight dashboard under Market with AI summary, metrics, and news context.
- 2025-11-10 (FEATURE backend/frontend-109): Added industry insight reasoning leveraging weighted fund-flow hotlists plus tagged news, stored DeepSeek responses via new DAO/service, exposed `/market/industry-insight` + history endpoints, wired control panel sync job, and launched the “Industry Reasoning” page under 推理分析 with AI summary, industry cards, and history log.
- 2025-11-09 (FEATURE backend/frontend-062): Introduced fund-flow based sector hotlist service (top industries/concepts across multi-period rankings), `/fund-flow/sector-hotlist` API, fund navigation placement, refreshed sector insight page, and supporting unit tests.
- 2025-11-10 (UPDATE backend-063): Patched HSGT fund flow sync to enrich the latest records via `stock_hsgt_fund_flow_summary_em`, ensuring north/southbound metrics stay current despite historical endpoint gaps, with new unit coverage for the summary merge.
- 2025-11-06 (FEATURE backend/frontend-046): Added margin account statistics ingestion via AkShare `stock_margin_account_info`, introduced schema/DAO/service wiring, control panel sync card, and dashboard with financing balance trend chart plus date-range filters.
- 2025-11-07 (FEATURE backend/frontend-047): Integrated LeGu market activity snapshot (`stock_market_activity_legu`) with new persistence, control panel card, localized summary grid, and translation set.
- 2025-11-08 (FEATURE backend/frontend-048): Added market fund flow history ingestion using AkShare `stock_market_fund_flow`, updated fund-flow aggregate bundle, control panel navigation/card, translations, and chart/table dashboard with range filters.
- 2025-11-08 (FEATURE backend/frontend-049): Implemented macro insight reasoning powered by DeepSeek, storing latest macro datasets (plus 10 historical points), new API/page, and control panel automation.
- 2025-11-08 (FEATURE backend-050): Added market overview aggregation service/API bundling realtime indices, index history, insights, and fund-flow snapshots for downstream DeepSeek reasoning.
- 2025-11-05 (UPDATE backend/frontend-046): Switched HSGT module to use AkShare `stock_hsgt_hist_em` (北向资金) historical series, refreshed schema/service/API, and rebuilt the dashboard with yearly filter and extended metrics.
- 2025-11-05 (FEATURE backend/frontend-050): Added margin account statistics ingestion via AkShare `stock_margin_account_info`, including schema/DAO/service, REST API, Control Panel card, fund flow aggregate integration, and a dedicated dashboard with pagination and yearly filters.
- 2025-11-05 (FEATURE backend/frontend-051): Added LeGu market activity (赚钱效应) module with AkShare ingestion, API, trading control card, and a Market navigation page summarizing breadth and limit statistics.
- 2025-10-30 (UPDATE frontend/backend-018): Reworked performance express & forecast pages plus stock detail highlights for the AkShare schemas (card layout restored, table columns realigned, links to stock detail, single-column tiles, and full-text change reasons with compact empty states).
- 2025-10-31 (FEATURE backend/frontend-025): Integrated Tonghuashun `stock_zyjs_ths` main business ingestion, control panel sync entry, and stock detail overview display.
- 2025-11-02 (FEATURE backend/frontend-031): Added A-share index history ingestion via AkShare `index_zh_a_hist`, including DAO/service, daily 17:00 scheduler, control panel trigger, navigation entry under Market, and new index-history page with ECharts trend plus tabular metrics for SSE Composite, SZSE Component, ChiNext, and STAR 50 indices.
- 2025-11-02 (FEATURE backend/frontend-032): Introduced Sina realtime China index snapshot ingest (`stock_zh_index_spot_sina`) with manual control-panel trigger, dedicated API, Market navigation page, localized table view, and control dashboard wiring.
- 2025-10-31 (UPDATE backend/frontend-025): Moved stock basic + main business syncs under the new “Base Data” control category and switched the main business job to incremental updates by comparing against existing records.
- 2025-10-31 (FEATURE backend/frontend-026): Added Eastmoney main composition ingestion (stock_zygc_em), control panel sync card, and stock detail revenue-mix visualisation grouped by category.
- 2025-10-31 (UPDATE frontend-026): Restyled stock detail business section with paired cards beneath the candlestick (business overview + revenue composition) including progress visuals for key categories.
- 2025-11-04 (FEATURE backend/frontend-030): Delivered the unified news ingestion & classification pipeline plus Market Insight DeepSeek reasoner summary, new FastAPI endpoints, control trigger, and dedicated frontend page.
- 2025-10-20 (FEATURE backend-001): Added Tushare API client, DAO layer, and service to fetch and upsert A-share stock basics into PostgreSQL using configuration-driven credentials.
- 2025-10-20 (FEATURE backend-002): Added daily trade ingestion pipeline covering API client helper, reusable DAO for daily trade data, batching service with duration reporting, and schema template for `daily_trade`.
- 2025-10-20 (FEATURE backend-003): Delivered FastAPI entrypoint exposing health, stock listing, and sync endpoints backed by existing services.
- 2025-10-20 (FEATURE frontend-001): Delivered multilingual stock list page with filter controls, fundamentals/statistics tabs, and responsive layout based on project mock-up.
- 2025-10-28 (REQUIREMENT backend-010): Income statement sync must perform per-code incremental updates (timestamp anchored), retry safely on rate limit errors, and avoid bulk downloads for already-seeded securities.
- 2025-10-21 (FEATURE backend-004): Sync endpoints now capture database statistics and runtime durations so the control panel job cards can display last update information immediately.
- 2025-10-22 (FEATURE backend-005): Replaced market-cap sync with daily indicator ingestion (daily_basic), including new DAO/service, scheduled + manual triggers, and UI columns for market cap, PE ratio, and turnover rate.
- 2025-10-22 (FEATURE backend-006): Added financial report sync (income API) with period-based fetching, persistence, scheduler, and control panel integration.
- 2025-10-23 (FEATURE backend-006 update): Renamed the financial report pipeline to income statements, switched to code-by-code `pro.income` fetching (latest 8 rows per stock), aligned control panel/API naming, and added automatic stock-basic fallback seeding when no local codes exist.
- 2025-10-24 (FEATURE backend-007): Added financial indicator sync (fina_indicator) with new DAO/service, code-by-code ingestion, scheduler, REST endpoints, and control panel card.
- 2025-10-24 (FEATURE backend-008): Added finance breakfast ingestion via AkShare (Eastmoney morning digest), including schema/DAO/service, 07:00 scheduler, REST/control endpoints, and frontend controls.
- 2025-10-24 (FEATURE frontend-003): Added "Daily Finance" section to sidebar, centralised i18n registry, rendered finance breakfast list with language persistence and AkShare-sourced data fixes.
- 2025-10-25 (FEATURE frontend-004): Updated Daily Finance cards to single-column summary-first layout and surfaced DeepSeek AI extracts with daily overview plus importance-ranked events.
- 2025-10-26 (FEATURE backend-009): Split finance breakfast AI extracts into dedicated summary/detail columns and aligned service/DAO/frontend parsing with the new schema.
- 2025-10-26 (FIX backend-009a): Prevent finance breakfast sync from overwriting existing article content when upstream data omits the field.
- 2025-10-26 (FEATURE backend-010): Made finance breakfast sync incremental by publication date and chained post-sync jobs to backfill article content via Eastmoney crawler and DeepSeek AI summaries/details.
- 2025-10-26 (FEATURE frontend-005): Centralised sidebar markup/script, introduced Configuration Center page, and ensured navigation stays consistent across app pages.
- 2025-10-27 (FEATURE backend-011): Added derived daily trade metrics pipeline (schema/DAO/service), scheduled 19:00 job, control panel card, and exposed metrics via `/stocks` for technical stats.
- 2025-10-27 (FEATURE frontend-006): Extended market intelligence dashboard with technical and fundamental tabs, added finance summary columns (EPS/revenue/profit/ROE), and introduced shared sidebar/config center components.
- 2025-10-27 (FEATURE backend-012): Derived fundamental metrics (net profit/revenue/ROE YoY & QoQ) pipeline with new DAO/service, scheduler, control panel trigger, and API endpoint.
- 2025-10-27 (FEATURE frontend-007): Added fundamental metrics control card and extended fundamentals tab to display YoY/QoQ KPIs with new columns.
- 2025-10-28 (FEATURE frontend-008): Renamed the Market Intelligence dashboard (formerly Basic Info), refreshed menu/title copy, set `index.html` as the canonical entry, and updated documentation references.
- 2025-10-28 (FEATURE frontend/backend-013): Added volume spike metric (latest volume ÷ 10-day average) to daily trade metrics sync and surfaced the column on the Market Intelligence trading stats tab.
- 2025-10-29 (FEATURE frontend/backend-014): Added stock favorites watchlist with detail-page star toggle, REST endpoints, and Market Intelligence watchlist mode filtered via navigation.
- 2025-10-29 (UPDATE frontend-010): Simplified stock detail layout (removed profile card) and retitled fundamentals widget to “Financial Stats”.
- 2025-10-29 (FIX backend/frontend-014a): Normalised favorite group values end-to-end and exposed them on stock list responses so watchlist filters display the assigned group name instead of defaulting to “Ungrouped”.
- 2025-10-29 (UPDATE frontend-011): Removed the favorites group column from the Market Intelligence stock table to declutter the layout.
- 2025-10-29 (FEATURE frontend/backend-015): Enhanced favorites with grouped watchlist support, detail-page heart toggle + toast, and Market Intelligence portfolio mode with group filter/banner integration.
- 2025-10-29 (UPDATE frontend-012): Removed exchange and keyword filter controls from the Market Intelligence dashboard per latest requirements.
- 2025-10-29 (UPDATE frontend/backend-016): Added industry dropdown, market cap and PE range filters, ROE suffix, and refined filter layout for the Market Intelligence dashboard.
- 2025-10-29 (UPDATE frontend-013): Watchlist view now offers a toggle to hide or show the search and filter panel for focused portfolio monitoring.
- 2025-10-29 (UPDATE frontend-014): Refined Portfolio Monitor with hidden search panel, tag-based group selector, updated copy, and streamlined watchlist banner.
- 2025-10-29 (UPDATE frontend-015): Enabled header search to look up stocks by code/name independently of combination filters on the Market Intelligence dashboard.
- 2025-10-29 (UPDATE frontend/backend-017): Added multi-period performance sorting to the trading stats tab with API support for ascending/descending order.
- 2025-10-29 (FIX backend-018): Market Intelligence search now ignores blank keywords so empty input no longer constrains results.
- 2025-10-31 (FEATURE backend/frontend-028): Added global index snapshot ingestion via AkShare `index_global_spot_em`, control-panel automation, “外围” navigation group, language-aware refresh button, and a dedicated global indices dashboard.
- 2025-10-31 (FEATURE backend/frontend-029): Delivered Dollar Index historical sync using AkShare `index_global_hist_em`, new control-panel card, peripheral menu link, and a Dollar Index page with localized one-year trend chart plus refreshed data table/controls.
- 2025-10-31 (FEATURE backend/frontend-030): Added RMB central parity module via AkShare `currency_boc_safe`, including PostgreSQL schema/DAO/service, scheduled control-panel job, peripheral navigation entry, localized one-year trend visualization, and comprehensive exchange-rate table with quotation notes.
- 2025-10-31 (FEATURE backend/frontend-034): Delivered social financing incremental statistics (AkShare macro_china_shrzgm) with new PostgreSQL schema/DAO/service, control panel card and monthly release guidance, macro navigation entry, dedicated dashboard with trend visual + table, and manual refresh workflow.
- 2025-10-31 (FEATURE backend/frontend-033): Added macro leverage ratio module (AkShare macro_cnbs) including PostgreSQL schema/DAO/service, control panel automation with release guidance, macro navigation entry ahead of peripheral data, dedicated frontend dashboard with trend chart and data table, and manual refresh workflow.
- 2025-10-31 (FEATURE backend/frontend-035): Introduced CPI monthly module (AkShare macro_china_cpi_monthly) with schema/DAO/service, dual nightly scheduler, control panel sync card with tooltip guidance, macro navigation entry, localized chart + table page, and manual refresh button.
- 2025-10-31 (UPDATE frontend-036): Reworked control panel category layout with horizontal tab navigation, new aggregate section, tooltip icons on macro cards, and enforced default Chinese UI state with active language highlighting across new macro dashboards.
- 2025-11-01 (FEATURE backend/frontend-037): Added manufacturing PMI module (AkShare macro_china_pmi_yearly) with schema/DAO/service, nightly scheduler, control panel automation + tooltip, macro navigation entry, localized PMI page with chart/list and manual refresh workflow.
- 2025-11-01 (FEATURE backend/frontend-038): Added M2 money supply module (AkShare macro_china_m2_yearly) including schema/DAO/service, triple 17:01 scheduler (10/11/12), control panel automation with tooltip, macro navigation entry, localized M2 dashboard with chart/table and manual refresh.
- 2025-10-31 (FEATURE backend/frontend-032): Built peripheral market insight pipeline aggregating global indices, dollar index, RMB midpoint, and commodity quotes, persisted summaries with DeepSeek integration, added control-panel automation, and exposed the new Peripheral Insight dashboard.
- 2025-10-31 (FEATURE backend/frontend-031): Added Federal Reserve statements ingestion (requests + BeautifulSoup crawler), new PostgreSQL schema/DAO/service with twice-daily scheduler, control-panel sync card, peripheral navigation entry, and a localized Fed Statements landing page with full-text rendering.
- 2025-11-03 (FEATURE backend/frontend-039): Added China PPI module (AkShare macro_china_ppi) with schema/DAO/service, dual 10:00 monthly schedulers, control panel integration, macro navigation entry, and localized dual-line chart plus filtered list view.
- 2025-11-03 (REQUIREMENT backend/frontend-040): Introduced PBC interest rate decision module (AkShare macro_bank_china_interest_rate) with backend ingestion, manual control panel trigger, macro navigation entry, localized trend/table page, and tooltip guidance.
- 2025-11-03 (FEATURE backend/frontend-041): Added global finance flash module (AkShare stock_info_global_em) with schema/DAO/service, configurable interval scheduler, control panel automation card, dedicated global flash page, and navigation entry under News.

## Unified News Pipeline (2025-11-03)

### Overview
- Added a shared news ingestion and classification pipeline that standardises all upstream sources before storage and downstream consumption.
- Initial integrations cover Eastmoney 7×24 Global Flash (`backend/src/services/global_flash_service.py`) and AkShare Finance Breakfast (`backend/src/services/finance_breakfast_service.py`). New sources can plug in by emitting the same payload contract.

### Storage Model
- `news_articles` (`backend/config/news_articles_schema.sql`)
  - Deterministic `article_id` derived from source + upstream key + publish timestamp.
  - Persists canonical metadata (title, summary, optional content, language, content type) and processing state fields (`processing_status`, attempt counters, `last_error`).
  - Timestamps normalised to Asia/Shanghai and stored without timezone; indexes support dedupe and queue scans.
- `news_insights` (`backend/config/news_insights_schema.sql`)
  - One-to-one with `news_articles.article_id`; captures LLM relevance/impact outputs, hierarchical impact tags, and arbitrary JSON metadata.

### Ingestion Flow
1. Source service fetches raw DataFrame from AkShare/Eastmoney.
2. `_prepare_*_frame` coerces column names, trims text, and filters invalid rows.
3. `_frame_to_pipeline_records` converts rows into payload dictionaries with ISO publish timestamps and content-type hints.
4. DAO deduplication (`existing_article_ids`) prevents re-inserting previously seen headlines.
5. `news_pipeline_service.ingest_articles` handles timestamp normalisation, auto-fills `content_fetched` flags, stores raw payload JSON, and writes the batch via `NewsArticleDAO.upsert`.
6. Finance Breakfast performs a second-stage content fetch (`fetch_eastmoney_detail`) prior to persistence when article bodies are available.

### Processing & Classification
- Status lifecycle (`NewsArticleDAO`): `pending` → `relevance_in_progress` → (`ready_for_impact` or `completed`) → `impact_in_progress` → `completed`/`error`.
- Batch acquisition uses `FOR UPDATE SKIP LOCKED` to protect concurrent workers and increments attempt counters (`relevance_attempts`, `impact_attempts`).
- Relevance batches (`classify_relevance_batch`) build prompts from title/summary/content, call DeepSeek, and persist results via `save_relevance_results`.
- Impact batches (`classify_impact_batch`) reuse DeepSeek with a richer prompt, parse both camelCase and snake_case fields, merge metadata, and write structured impact summaries/analysis using `save_impact_results`.
- `reset_in_progress_articles()` resets stuck rows back to `pending`; `last_error` is propagated for diagnostics when parsing or API calls fail.

### Scheduling & Control Panel
- APScheduler jobs in `backend/src/app.py`:
  - Global flash ingestion runs hourly with additional intraday guards (respecting trading calendar logic).
  - Impact/relevance classification executes every 10 minutes (`schedule_global_flash_classification_job`) and logs progress through the control monitor.
  - Manual endpoints (`/control/sync/global-flash`, `/control/sync/finance-breakfast`, `/control/sync/global-flash-classification`) allow ad-hoc retriggers; payloads are optional.
- Control panel cards display Asia/Shanghai timestamps thanks to timezone-aware monitor updates.

### API & Frontend Consumption
- Aggregation helper `list_news_articles` returns unified article + insight payloads with localised timestamps.
- REST surfaces:
  - `/news/articles` (generic feed with optional `source` and `onlyRelevant` filters).
  - `/news/global-flash` for the flash dashboard, `/finance-breakfast` for morning briefs.
- Frontend bundles (`frontend/public/global-flash.js`, `frontend/public/finance-breakfast.js`) render the new insight fields: impact judgement, relevance/impact confidence, subject level, scope, time sensitivity, quant signals, and topic chips for markets/industries/sectors/themes/stocks.

### Operational Notes
- DeepSeek credentials remain mandatory for classification; missing tokens result in skipped batches with explicit log messages.
- Inspect pipeline health via `SELECT processing_status, COUNT(*) FROM news_articles GROUP BY 1` and join `news_insights` for recent impact metadata.
- When adding sources, reuse `ingest_articles` and emit `source`, `source_item_id`, `title`, `summary`, `content`, `published_at`, and `url` fields; the rest of the pipeline is source-agnostic.
- `extra_metadata` always retains the model payload (including `raw_response`) to simplify troubleshooting and future prompt tuning.

## Margin Account Dashboard (2025-11-06)

### Summary
- Tracks Eastmoney margin account statistics (融资融券) via AkShare `stock_margin_account_info`.
- Persists long-term history in `backend/config/margin_account_schema.sql`, surfaces via `MarginAccountDAO` and `margin_account_service`.
- Control panel card `#margin-account-card` provides manual sync and progress telemetry; bundle participates in fund-flow aggregate job.

### Backend Flow
1. `fetch_margin_account_info` pulls the full AkShare dataset and column map `MARGIN_ACCOUNT_COLUMN_MAP` aligns field names.
2. `_prepare_margin_account_frame` (service layer) coerces numeric fields, normalises dates, and drops duplicates.
3. `MarginAccountDAO.upsert` ensures schema creation, deduplicates on `trade_date`, and tracks updates with `updated_at`.
4. `MarginAccountDAO.list_entries` supports optional `start_date`, `end_date`, `limit`, `offset`, and returns `available_years` for UI filters.

### API & Frontend
- Endpoint `/margin/account` delivers paginated records plus metadata:
  - `total`, `availableYears`, `lastSyncedAt`, and item array containing per-field values.
- Front-end bundle `frontend/public/margin-account.js` renders:
  - Dual-axis ECharts line chart (financing balance vs. financing purchase amount).
  - Responsive table with financing balances, investor counts, collateral ratios, etc.
  - Date-range inputs with reset button; default result length 100 rows.
- Styles consolidated in `styles.css` (`.fund-flow-chart`, `.fund-flow-table` classes).

### Operational Notes
- Sync job persists thousands of rows on first run; subsequent runs are idempotent.
- Control panel surfaces finished timestamps using Asia/Shanghai timezone from `SyncMonitor`.
- Table defaults to descending trade date; consider enabling server-side caching if query load grows.

## Market Activity Snapshot (2025-11-07)

### Summary
- Uses LeGu `stock_market_activity_legu` to capture daily “market heat” metrics (涨跌家数、涨跌停、活跃度等).
- Persisted via `market_activity_schema.sql` and `MarketActivityDAO`; dataset treated as snapshot rather than history.
- Control card `#market-activity-card` allows manual refresh, reporting dataset timestamp sourced from the API payload.

### Backend Flow
1. `fetch_market_activity_legu` returns metric/value pairs; service annotates `display_order`, numeric conversions, and canonical timestamp.
2. `_prepare_market_activity_frame` stores both `value_text` and `value_number` for mixed string/percent metrics.
3. `MarketActivityDAO.upsert` deduplicates on `metric`, updates data while preserving `dataset_timestamp` for the latest run.

### API & Frontend
- Endpoint `/market/activity` returns `items` plus `datasetTimestamp` (ISO string).
- Front-end view `frontend/public/market-activity.html` renders:
  - Localized summary grid grouping advancers/decliners, limit-up/down, and activity score.
  - Table fallback for all metric rows.
  - Language toggle built on shared `i18n.js`.

### Operational Notes
- Snapshot job runs quickly; no pagination needed.
- Charting not required—UI focuses on cards and table layout.
- Monitor entry `market_activity` logs dataset timestamp for cross-checking with UI.

## Market Fund Flow Dashboard (2025-11-08)

### Summary
- Introduces Eastmoney big-board fund flow history via AkShare `stock_market_fund_flow`.
- Schema template `backend/config/market_fund_flow_schema.sql` with DAO/service pair (`MarketFundFlowDAO`, `market_fund_flow_service`).
- Fund-flow aggregate job now chains industry → concept → individual → HSGT → margin account → market fund flow → big deal.

### Backend Flow
1. Service fetches, renames columns using `MARKET_FUND_FLOW_COLUMN_MAP`, and `_prepare_market_fund_flow_frame` normalises numerics/percents (converted to ratios).
2. `MarketFundFlowDAO.upsert` deduplicates by `trade_date`; new `stats()` helper returns count, `updated_at`, and latest trade date.
3. `MarketFundFlowDAO.list_entries` supports pagination plus optional `start_date` / `end_date`, returns `available_years` for future filtering.
4. `_run_market_fund_flow_job` captures total row count, persists last duration, and defends against empty-table index errors.

### API & Frontend
- Endpoint `/fund-flow/market` accepts `startDate`, `endDate`, `limit`, `offset`; response includes `total`, `latestTradeDate`, `lastSyncedAt`, `availableYears`, and record list.
- Control panel card `#market-fund-flow-card` exposes manual sync; translations added for titles, subtitles, and aggregate description.
- Front-end assets (`market-fund-flow.html/js`) deliver:
  - Date-range filters (default limit 200) with reset.
  - ECharts multi-series line chart for main/large/medium/small net inflow amounts.
  - Responsive card grid summarising per-day prices, percentage moves, and multi-tier net inflow balances (replacing the wide table layout).
- Sidebar navigation includes “Market Fund Flow” under the Funds section.

## Macro Insight Reasoner (2025-11-08)

### Summary
- Consolidates key macroeconomic datasets (macro leverage, social financing, CPI, PPI, PMI, M2, PBC benchmark rates) and generates an AI summary describing the latest macro stance for A-shares.
- Persists results in `macro_insights` (`backend/config/macro_insight_schema.sql`) with JSON payloads for datasets, optional DeepSeek response, and model metadata.
- Control panel now has a “Macro Insight Reasoning” card and the macro aggregate bundle triggers the reasoning step automatically.

### Backend Flow
1. `generate_macro_insight` fetches each macro dataset via existing list services, normalises values, and keeps the latest reading plus 10 historical points per indicator (PMI is split by series).
2. The payload records translation keys (`titleKey`, `labelKey`) so the frontend can localise columns without duplicating config logic.
3. When DeepSeek credentials are configured the service calls `generate_finance_analysis` with a strict JSON prompt (`MACRO_INSIGHT_PROMPT`) to obtain sentiment, drivers, risks, and watch points; the raw response and parsed JSON are stored.
4. `MacroInsightDAO` (new) upserts snapshots keyed by `snapshot_date` and exposes `fetch_latest`/`stats` for the API and control panel telemetry.

### API & Frontend
- Endpoint `/macro/insight` returns the latest snapshot (`MacroInsightResponse`) containing summary JSON, model name, dataset array, and warnings for missing feeds.
- New pages `frontend/public/macro-insight.html/js` and `frontend/public/market-overview.html/js` render:
  - LLM summary with bias badge, overview、policy outlook、risk/watch lists。
  - Highlight cards derived from key indicators。
  - Dataset grids showing the retained 11 rows per indicator with translated headers。
  - Market overview page also displays realtime indices、fund-flow tables、market activity、and includes a streaming DeepSeek reasoning panel triggered via `/market/overview/reason`.
- Sidebar highlights the new entries under the Market/Macro sections; control panel button triggers `/control/sync/macro-insight` (runLLM default true).

### Operational Notes
- `SyncMacroInsightRequest` accepts `runLLM` so environments without DeepSeek tokens can still persist raw datasets/metadata.
- Macro aggregate job sequence adds the reasoning step after traditional sync tasks (progress bar now counts the extra stage).
- `SyncMonitor` persistence expanded to store status/progress/finished timestamps across restarts, ensuring the new card retains last-run details on refresh.

## Market Overview Aggregator (2025-11-08)

### Summary
- Provides a unified payload that combines realtime index snapshots, recent index history, existing market/macro/peripheral insights, and the latest fund-flow/market-activity metrics.
- Exposed via `/market/overview` for downstream DeepSeek reasoning (no LLM step yet).

### Backend Flow
1. `build_market_overview_payload` pulls:
   - Realtime indices filtered by turnover > ¥5,000亿.
   - Latest 10 trading days from `index_history` for configured benchmark indices.
   - Latest market, macro, peripheral insights (reference articles stripped).
   - Recent 10-day slices for market fund flow, HSGT fund flow (默认“北向资金”), and margin account tables.
   - Latest market activity snapshot (赚钱效应) along with warnings/metrics.
2. Aggregation normalises JSON fields (peripheral metrics) and returns a timestamped payload ready for LLM consumption.
3. FastAPI endpoint `/market/overview` returns the structured payload (`MarketOverviewResponse`).

### Operational Notes
- Designed as a read-only API; reasoning endpoint `/market/overview/reason` returns a plain-text stream for incremental UI updates.
- Unit test `test_market_overview_service.py` ensures each slice is capped at 10 entries and required sections exist.

## Market Insight Reasoner (2025-11-04)

### Summary
- DeepSeek Reasoner aggregates the latest high-impact news into a 24-hour market narrative aimed at A-share index interpretation.
- Results persist in `news_market_insights` (`backend/config/news_market_insights_schema.sql`) with prompt/response usage metrics, JSON summary, and referenced headline metadata.
- Manual trigger only: `/control/sync/market-insight` enqueues the job; control card reports elapsed time, token usage, and Asia/Shanghai timestamps.
- Headline intake now prioritises up to 40 items by confidence、影响层级、主题覆盖和时效性；后台会抓取更大的候选集并做去重，以扩大资讯覆盖面且控制上下文长度。
- 单条新闻的摘要/分析文本在入模前做了截断，默认能把 token 规模稳定在 40 条以内；如需更多样本可在触发请求里把 `articleLimit` 调大（上限 50）。
- 控制面板新增“宏观聚合”任务，一键串行运行杠杆率、社融、CPI、PPI、PMI、M2、央行利率等宏观数据同步，便于每日集中刷新。

### Backend Flow
1. `collect_recent_market_headlines` joins `news_articles` + `news_insights` for completed rows tagged with market-level impact or explicit market scopes.
2. Headlines are formatted for the prompt block with publish timestamps normalised to local time.
3. `generate_market_insight_summary` calls `generate_finance_analysis(..., model_override="deepseek-reasoner", return_usage=True)` to request structured JSON.
4. Parsed JSON is stored alongside the raw response; token counts and elapsed ms persist for monitoring.
5. `NewsMarketInsightDAO.insert_summary` performs idempotent upsert so retries overwrite the latest run cleanly.

### API & Frontend
- `/news/market-insight` returns the latest summary (or a fallback list of qualifying headlines when no summary exists).
- Response model surfaces:
  - Summary metadata (`generatedAt`, `windowStart`, `windowEnd`, headline count, token usage, latency, model).
  - Structured JSON payload (sentiment, confidence, overview, drivers, sectors/indices/risk/actions, detailed notes per headline).
  - Referenced headlines with impact summaries/analysis/confidence.
- New sidebar entry under “News” → “Market Insight”; front-end bundle `frontend/public/market-insight.js` renders:
  - Highlight card with sentiment chip, KPI grid, and sectioned lists。
  - Responsive article grid featuring impact badges, summaries, analysis paragraphs, and scope chips。
  - Manual “Generate Insight” button（40条默认样本，内置任务状态提示，409 冲突时提示“任务运行中”）。
- Styles consolidated in `frontend/public/styles.css` for sentiment chips, insight cards, and news chips; query-string cache busting updated (`styles.css?v=20261110`, `sidebar.js?v=20261110`, `i18n.js?v=20261110`).

### Operational Notes
- Uses Asia/Shanghai naive timestamps end-to-end; monitor entries convert to local time when persisting job state.
- Reasoner model default output cap 32k tokens; adjust `MAX_OUTPUT_TOKENS` in `market_insight_service` if future prompts expand.
- If the reasoner returns invalid JSON, service stores raw text while keeping the run result for debugging (`raw_response` column).
- Fallback mode surfaces the qualifying headline list so the frontend still provides context even when no summary exists.

## Database Schema
- The stock metadata table definition is stored at `backend/config/stock_basic_schema.sql`. Services/DAOs apply this template automatically.
- Daily trade schema template lives at `backend/config/daily_trade_schema.sql` and is consumed by the daily trade DAO.

## Dependency Log
- 2025-10-20: Installed `apscheduler==3.11.0` to support scheduled backend sync jobs.
- 2025-10-20: Installed `pandas==1.5.3`, `psycopg2-binary==2.9.6`, and `tushare==1.4.24` in the trend environment and recorded them in `requirements.txt`.
- 2025-10-20: Installed `fastapi==0.119.1`, `uvicorn==0.38.0`, and upgraded `anyio==4.11.0` to support the backend HTTP API server.
- 2025-10-21: Installed `libpq` via conda-forge in the trend environment to expose the `pg_config` binary needed for PostgreSQL client builds.
- 2025-10-21: Downgraded the trend Conda environment to Python 3.10.18 to match the pinned dependency wheel availability and reinstalled packages from `requirements.txt`.
- 2025-10-21: Reinstalled `pydantic==1.10.15`, added `numpy==1.23.5` for compatibility with pinned pandas, and added `greenlet==3.2.4` to satisfy SQLAlchemy runtime requirements.
- 2025-10-24: Installed `akshare==1.11.98` to ingest Eastmoney finance breakfast summaries.
