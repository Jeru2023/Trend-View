"""
Data access object for the daily indicator (daily_basic) table.
"""

from __future__ import annotations

from datetime import datetime
import math
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "daily_indicator_schema.sql"

DAILY_INDICATOR_FIELDS: Sequence[str] = (
    "ts_code",
    "trade_date",
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
)


class DailyIndicatorDAO(PostgresDAOBase):
    """Handles persistence tasks for the daily indicator table."""

    _conflict_keys: Sequence[str] = ("ts_code", "trade_date")

    def __init__(
        self,
        config: PostgresSettings,
        table_name: str | None = None,
    ) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "daily_indicator_table", "daily_indicator")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )

    def upsert(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            affected = self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe,
                columns=DAILY_INDICATOR_FIELDS,
                conflict_keys=self._conflict_keys,
                date_columns=("trade_date",),
            )

        return affected

    def stats(self) -> dict[str, Optional[datetime]]:
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

    def fetch_latest_indicators(self, codes: Sequence[str]) -> Dict[str, dict]:
        if not codes:
            return {}

        query = sql.SQL(
            """
            SELECT DISTINCT ON (ts_code)
                   ts_code,
                   trade_date,
                   pe,
                   total_mv,
                   turnover_rate
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

        results: Dict[str, dict] = {}
        for ts_code, trade_date, pe, total_mv, turnover_rate in rows:
            pe_value = float(pe) if pe is not None else None
            if pe_value is not None and not math.isfinite(pe_value):
                pe_value = None

            market_cap_value = float(total_mv) if total_mv is not None else None
            if market_cap_value is not None and not math.isfinite(market_cap_value):
                market_cap_value = None
            if market_cap_value is not None:
                market_cap_value *= 10000  # Tushare reports total_mv in 10k CNY units.

            turnover_value = float(turnover_rate) if turnover_rate is not None else None
            if turnover_value is not None and not math.isfinite(turnover_value):
                turnover_value = None

            results[ts_code] = {
                "pe": pe_value,
                "market_cap": market_cap_value,
                "turnover_rate": turnover_value,
            }
        return results


__all__ = [
    "DAILY_INDICATOR_FIELDS",
    "DailyIndicatorDAO",
]
