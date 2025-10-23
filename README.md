# Trend View

This repository contains the Trend View backend services and frontend assets.

## Prerequisites

- Python 3.10+
- PostgreSQL instance running with the credentials defined in `backend/config/settings.local.json`
- Node tooling is not required; the current frontend ships as static assets.

Install backend dependencies:

```sh
python -m pip install -r requirements.txt
```

## Backend - FastAPI

1. Ensure the database credentials in `backend/config/settings.local.json` are correct and that the database is reachable.
2. Populate base data (optional but recommended):

   ```sh
   python -c "from backend.src.services import sync_stock_basic, sync_daily_trade; sync_stock_basic(); sync_daily_trade(batch_size=50, window_days=30)"
   ```

3. Start the API server:

   ```sh
   uvicorn backend.src:app --host 0.0.0.0 --port 8000 --reload
   uvicorn backend.src.app:app --reload
   ```

   The service exposes:
   - `GET http://localhost:8000/health`
   - `GET http://localhost:8000/stocks?keyword=bank&limit=20`
   - `POST http://localhost:8000/sync/stock-basic`
   - `POST http://localhost:8000/sync/daily-trade`
   - `POST http://localhost:8000/sync/daily-indicators`
   - `POST http://localhost:8000/control/sync/daily-indicators`
   - `POST http://localhost:8000/sync/income-statements`
   - `POST http://localhost:8000/control/sync/income-statements`
   - `POST http://localhost:8000/sync/financial-indicators`
   - `POST http://localhost:8000/control/sync/financial-indicators`
   - `POST http://localhost:8000/sync/finance-breakfast`
   - `POST http://localhost:8000/control/sync/finance-breakfast`

## Frontend - Static Assets

The UI is a static stock list page located under `frontend/public`.

```sh
cd frontend/public
python -m http.server 3000
```

Open `http://localhost:3000/index.html` to interact with the page. The frontend consumes the backend `/stocks` endpoint (defaults to `http://localhost:8000`; override by defining `window.API_BASE_URL` before loading `app.js` if necessary).

## Folder Structure

- `backend/src/` - FastAPI app, services, DAOs, and API clients.
- `backend/config/` - Config templates and schema definitions.
- `frontend/public/` - Static HTML/CSS/JS for the stock list page.

## Control Panel

Use the control panel at `frontend/public/control.html` to trigger manual data updates and adjust runtime settings.
- The page calls `/control/sync/*` endpoints for on-demand jobs.
- `/control/status` exposes progress information for stock_basic, daily_trade, daily_indicator, income_statement, financial_indicator, and finance_breakfast jobs.
- Configuration changes (ST/delisted filters, daily trade window) persist to `backend/config/control_config.json`.
- Daily indicator sync fetches valuation metrics (daily_basic) each trading day at 17:05 by default; you can trigger it manually when needed.
- Income statement sync pulls the latest eight statements per stock via `pro.income`, iterating code-by-code each day at 18:00 or on demand. When the local stock list is empty the job auto-fetches stock basics to obtain codes before continuing.
- Financial indicator sync collects profitability and efficiency ratios via `fina_indicator`, iterating code-by-code with a default limit of eight rows per stock and running nightly at 18:30 (also available on demand).
- Finance breakfast sync retrieves the Eastmoney morning digest through AkShare each day at 07:00, with on-demand refresh support from the control panel.




