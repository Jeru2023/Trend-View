"""
Data access object for daily trade prices.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd
from psycopg2 import sql

from ..api_clients import DAILY_TRADE_FIELDS
from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "daily_trade_schema.sql"
DATE_FORMAT = "%Y%m%d"


class DailyTradeDAO(PostgresDAOBase):
    """Handles persistence tasks for the ``daily_trade`` table."""

    _conflict_keys: Sequence[str] = ("ts_code", "trade_date")

    def __init__(
        self,
        config: PostgresSettings,
        table_name: str = "daily_trade",
    ) -> None:
        super().__init__(config=config)
        self._table_name = table_name
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        """Ensure the destination table exists."""
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN IF NOT EXISTS is_intraday BOOLEAN NOT NULL DEFAULT FALSE"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )

    def clear_table(self) -> int:
        """Remove all rows from the daily trade table."""
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DELETE FROM {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                return cur.rowcount or 0

    def delete_date_range(self, start_date: str, end_date: str) -> int:
        """
        Remove rows whose ``trade_date`` falls within the provided range (inclusive).
        """
        start_dt = datetime.strptime(start_date, DATE_FORMAT).date()
        end_dt = datetime.strptime(end_date, DATE_FORMAT).date()

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "DELETE FROM {schema}.{table} WHERE trade_date BETWEEN %s AND %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (start_dt, end_dt),
                )
                rowcount = cur.rowcount or 0

        return rowcount

    def stats(self) -> dict[str, Optional[datetime]]:
        """Return total row count and latest updated timestamp."""
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                count, last_updated = cur.fetchone()
        return {"count": count or 0, "updated_at": last_updated}

    def latest_trade_date(self, *, include_intraday: bool = False) -> Optional[datetime]:
        """Return the most recent trade_date available in the table."""
        where_clause = sql.SQL("")
        if not include_intraday:
            where_clause = sql.SQL(" WHERE is_intraday = FALSE")

        with self.connect() as conn:
            self.ensure_table(conn)
            query = sql.SQL("SELECT MAX(trade_date) FROM {schema}.{table}").format(
                schema=sql.Identifier(self.config.schema),
                table=sql.Identifier(self._table_name),
            ) + where_clause
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
        return row[0] if row else None

    def latest_trade_dates_for_codes(self, codes: Sequence[str]) -> Dict[str, datetime]:
        """Return the latest trade_date for each provided code."""
        if not codes:
            return {}

        with self.connect() as conn:
            self.ensure_table(conn)
            query = sql.SQL(
                "SELECT ts_code, MAX(trade_date) FROM {schema}.{table} "
                "WHERE ts_code = ANY(%s) AND is_intraday = FALSE GROUP BY ts_code"
            ).format(
                schema=sql.Identifier(self.config.schema),
                table=sql.Identifier(self._table_name),
            )
            with conn.cursor() as cur:
                cur.execute(query, (list(codes),))
                rows = cur.fetchall()

        return {code: trade_date for code, trade_date in rows if trade_date is not None}

    def upsert(self, dataframe: pd.DataFrame) -> int:
        """Synchronise the provided DataFrame into the daily trade table."""
        if dataframe.empty:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            affected = self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe,
                columns=DAILY_TRADE_FIELDS,
                conflict_keys=self._conflict_keys,
                date_columns=("trade_date",),
            )

        return affected

    def fetch_latest_metrics(self, codes: Sequence[str]) -> Dict[str, dict]:
        """
        Return the latest trade metrics for the provided security codes.
        """
        if not codes:
            return {}

        query = sql.SQL(
            """
            SELECT DISTINCT ON (ts_code)
                   ts_code,
                   trade_date,
                   close,
                   pct_chg,
                   vol
            FROM {schema}.{table}
            WHERE ts_code = ANY(%s) AND is_intraday = FALSE
            ORDER BY ts_code, trade_date DESC
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (list(codes),))
                rows = cur.fetchall()

        metrics: Dict[str, dict] = {}
        for ts_code, trade_date, close, pct_chg, vol in rows:
            metrics[ts_code] = {
                "trade_date": trade_date,
                "last_price": float(close) if close is not None else None,
                "pct_change": float(pct_chg) if pct_chg is not None else None,
                "volume": float(vol) if vol is not None else None,
            }
        return metrics

    def fetch_close_prices(
        self,
        *,
        start_date: date | datetime | str | None = None,
        end_date: date | datetime | str | None = None,
        include_intraday: bool = False,
    ) -> pd.DataFrame:
        """
        Load close price history within the given date range.
        """
        def _normalize(value: date | datetime | str | None) -> Optional[date]:
            if value is None:
                return None
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
            text = str(value).strip()
            if not text:
                return None
            return datetime.strptime(text, DATE_FORMAT).date()

        start = _normalize(start_date)
        end = _normalize(end_date)

        clauses: list[str] = []
        params: list[object] = []
        if start:
            clauses.append("trade_date >= %s")
            params.append(start)
        if end:
            clauses.append("trade_date <= %s")
            params.append(end)

        if not include_intraday:
            clauses.append("is_intraday = FALSE")

        with self.connect() as conn:
            self.ensure_table(conn)
            base_sql = sql.SQL(
                "SELECT ts_code, trade_date, close, vol AS volume "
                "FROM {schema}.{table}"
            ).format(
                schema=sql.Identifier(self.config.schema),
                table=sql.Identifier(self._table_name),
            ).as_string(conn)

            where_clause = ""
            if clauses:
                where_clause = " WHERE " + " AND ".join(clauses)

            query = f"{base_sql}{where_clause} ORDER BY ts_code, trade_date"
            frame = pd.read_sql_query(query, conn, params=params)

        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        return frame

    def fetch_price_history(
        self,
        ts_code: str,
        *,
        limit: int = 180,
        include_intraday: bool = False,
    ) -> list[dict[str, object]]:
        """
        Return recent OHLCV bars for the given security.
        """
        if not ts_code:
            return []
        if limit is not None and limit <= 0:
            return []

        where_clause = sql.SQL("WHERE ts_code = %s")
        params: list[object] = [ts_code]
        if not include_intraday:
            where_clause += sql.SQL(" AND is_intraday = FALSE")
        query = sql.SQL(
            """
            SELECT trade_date, open, high, low, close, vol, pct_chg, is_intraday
            FROM {schema}.{table}
            {where_clause}
            ORDER BY trade_date DESC
            LIMIT %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, tuple(params + [limit]))
                rows = cur.fetchall()

        history = []
        for (
            trade_date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            pct_change,
            is_intraday,
        ) in reversed(rows):
            history.append(
                {
                    "trade_date": trade_date.isoformat() if isinstance(trade_date, date) else str(trade_date),
                    "open": float(open_price) if open_price is not None else None,
                    "high": float(high_price) if high_price is not None else None,
                    "low": float(low_price) if low_price is not None else None,
                    "close": float(close_price) if close_price is not None else None,
                    "volume": float(volume) if volume is not None else None,
                    "pct_change": float(pct_change) if pct_change is not None else None,
                    "is_intraday": bool(is_intraday),
                }
            )
        return history

    def fetch_latest_bar(
        self,
        ts_code: str,
        *,
        include_intraday: bool = True,
    ) -> Optional[dict[str, object]]:
        if not ts_code:
            return None

        where_clause = sql.SQL("WHERE ts_code = %s")
        params: list[object] = [ts_code]
        if not include_intraday:
            where_clause += sql.SQL(" AND is_intraday = FALSE")

        query = sql.SQL(
            """
            SELECT trade_date,
                   open,
                   high,
                   low,
                   close,
                   vol,
                   pct_chg,
                   is_intraday
            FROM {schema}.{table}
            {where_clause}
            ORDER BY trade_date DESC, updated_at DESC
            LIMIT 1
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                row = cur.fetchone()

        if not row:
            return None

        trade_date, open_price, high_price, low_price, close_price, volume, pct_change, is_intraday = row
        return {
            "trade_date": trade_date.isoformat() if isinstance(trade_date, date) else str(trade_date),
            "open": float(open_price) if open_price is not None else None,
            "high": float(high_price) if high_price is not None else None,
            "low": float(low_price) if low_price is not None else None,
            "close": float(close_price) if close_price is not None else None,
            "volume": float(volume) if volume is not None else None,
            "pct_change": float(pct_change) if pct_change is not None else None,
            "is_intraday": bool(is_intraday),
        }


__all__ = [
    "DailyTradeDAO",
]
