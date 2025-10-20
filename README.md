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

## Backend – FastAPI

1. Ensure the database credentials in `backend/config/settings.local.json` are correct and that the database is reachable.
2. Populate base data (optional but recommended):

   ```sh
   python -c "from backend.src.services import sync_stock_basic, sync_daily_trade; sync_stock_basic(); sync_daily_trade(batch_size=50, window_days=30)"
   ```

3. Start the API server:

   ```sh
   uvicorn backend.src:app --host 0.0.0.0 --port 8000 --reload
   ```

   The service exposes:
   - `GET http://localhost:8000/health`
   - `GET http://localhost:8000/stocks?keyword=bank&limit=20`
   - `POST http://localhost:8000/sync/stock-basic`
   - `POST http://localhost:8000/sync/daily-trade`

## Frontend – Static Assets

The UI is a static stock list page located under `frontend/public`.

```sh
cd frontend/public
python -m http.server 3000
```

Open `http://localhost:3000/index.html` to interact with the page. The frontend consumes the backend `/stocks` endpoint (defaults to `http://localhost:8000`; override by defining `window.API_BASE_URL` before loading `app.js` if necessary).

## Folder Structure

- `backend/src/` – FastAPI app, services, DAOs, and API clients.
- `backend/config/` – Config templates and schema definitions.
- `frontend/public/` – Static HTML/CSS/JS for the stock list page.
