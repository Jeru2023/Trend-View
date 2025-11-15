"""
Data access object for derived daily trade metrics.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "daily_trade_metrics_schema.sql"

DAILY_TRADE_METRICS_FIELDS: Sequence[str] = (
    "ts_code",
    "trade_date",
    "close",
    "pct_change_1y",
    "pct_change_6m",
    "pct_change_3m",
    "pct_change_1m",
    "pct_change_2w",
    "pct_change_1w",
    "ma_20",
    "ma_10",
    "ma_5",
    "volume_spike",
)


class DailyTradeMetricsDAO(PostgresDAOBase):
    """Handles persistence tasks for derived daily trade metrics."""

    _conflict_keys: Sequence[str] = ("ts_code", "trade_date")

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "daily_trade_metrics_table", "daily_trade_metrics")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
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
                    "ADD COLUMN IF NOT EXISTS volume_spike NUMERIC"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )

    def upsert(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
            affected = self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe,
                columns=DAILY_TRADE_METRICS_FIELDS,
                conflict_keys=self._conflict_keys,
                date_columns=("trade_date",),
            )

        return affected

    def upsert_partial(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0
        with self.connect() as conn:
            self.ensure_table(conn)
            return self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe,
                columns=DAILY_TRADE_METRICS_FIELDS,
                conflict_keys=self._conflict_keys,
                date_columns=("trade_date",),
            )

    def stats(self) -> dict[str, Optional[datetime]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT COUNT(*), MAX(updated_at), MAX(trade_date) "
                        "FROM {schema}.{table}"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                count, updated_at, latest_trade_date = cur.fetchone()
        return {
            "count": count or 0,
            "updated_at": updated_at,
            "latest_trade_date": latest_trade_date,
        }

    def fetch_metrics(self, codes: Sequence[str]) -> Dict[str, dict]:
        if not codes:
            return {}

        query = sql.SQL(
            """
            SELECT DISTINCT ON (ts_code)
                   ts_code,
                   pct_change_1y,
                   pct_change_6m,
                   pct_change_3m,
                   pct_change_1m,
                   pct_change_2w,
                   pct_change_1w,
                   ma_20,
                   ma_10,
                   ma_5,
                   volume_spike
            FROM {schema}.{table}
            WHERE ts_code = ANY(%s)
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
        for (
            ts_code,
            pct_1y,
            pct_6m,
            pct_3m,
            pct_1m,
            pct_2w,
            pct_1w,
            ma_20,
            ma_10,
            ma_5,
            volume_spike,
        ) in rows:
            metrics[ts_code] = {
                "pct_change_1y": pct_1y,
                "pct_change_6m": pct_6m,
                "pct_change_3m": pct_3m,
                "pct_change_1m": pct_1m,
                "pct_change_2w": pct_2w,
                "pct_change_1w": pct_1w,
                "ma_20": ma_20,
                "ma_10": ma_10,
                "ma_5": ma_5,
                "volume_spike": volume_spike,
            }
        return metrics


__all__ = [
    "DailyTradeMetricsDAO",
    "DAILY_TRADE_METRICS_FIELDS",
]
