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
- 2025-10-30 (UPDATE frontend/backend-018): Reworked performance express & forecast pages plus stock detail highlights for the AkShare schemas (card layout restored, table columns realigned, links to stock detail, single-column tiles, and full-text change reasons with compact empty states).
- 2025-10-31 (FEATURE backend/frontend-025): Integrated Tonghuashun `stock_zyjs_ths` main business ingestion, control panel sync entry, and stock detail overview display.
- 2025-10-31 (UPDATE backend/frontend-025): Moved stock basic + main business syncs under the new “Base Data” control category and switched the main business job to incremental updates by comparing against existing records.
- 2025-10-31 (FEATURE backend/frontend-026): Added Eastmoney main composition ingestion (stock_zygc_em), control panel sync card, and stock detail revenue-mix visualisation grouped by category.
- 2025-10-31 (UPDATE frontend-026): Restyled stock detail business section with paired cards beneath the candlestick (business overview + revenue composition) including progress visuals for key categories.
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
