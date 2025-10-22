Session Boot Rules
Always run chcp 65001 in PowerShell as the first command so the console uses UTF-8. Apply UTF-8 encoding for all file and console read/write/print operations.
Immediately set [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 and export PYTHONIOENCODING=utf-8 (plus similar flags for other runtimes) so downstream tools avoid mojibake.

Workflow
After user approval, commit changes using Git.
Log every new requirement in this SPEC.MD file for future reference.
Whenever a Python package is installed or upgraded, update requirements.txt to keep dependency pins in sync.

Documentation Standards
Keep all Markdown files (e.g., README.md, SPEC.MD) written entirely in English, even if requirements are delivered in another language.

## Feature Log
- 2025-10-20 (FEATURE frontend-002): Added control panel with scheduled sync automation, runtime configuration toggles, and manual job triggers.
- 2025-10-20 (FEATURE backend-001): Added Tushare API client, DAO layer, and service to fetch and upsert A-share stock basics into PostgreSQL using configuration-driven credentials.
- 2025-10-20 (FEATURE backend-002): Added daily trade ingestion pipeline covering API client helper, reusable DAO for daily trade data, batching service with duration reporting, and schema template for `daily_trade`.
- 2025-10-20 (FEATURE backend-003): Delivered FastAPI entrypoint exposing health, stock listing, and sync endpoints backed by existing services.
- 2025-10-20 (FEATURE frontend-001): Delivered multilingual stock list page with filter controls, fundamentals/statistics tabs, and responsive layout based on project mock-up.
- 2025-10-21 (FEATURE backend-004): Sync endpoints now capture database statistics and runtime durations so the control panel job cards can display last update information immediately.
- 2025-10-22 (FEATURE backend-005): Replaced market-cap sync with daily indicator ingestion (daily_basic), including new DAO/service, scheduled + manual triggers, and UI columns for market cap, PE ratio, and turnover rate.

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




