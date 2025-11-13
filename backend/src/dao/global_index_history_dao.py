"""DAO for Yahoo Finance global index history."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "global_index_history_schema.sql"

GLOBAL_INDEX_HISTORY_FIELDS: Sequence[str] = (
    "code",
    "name",
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "prev_close",
    "change_amount",
    "change_percent",
    "currency",
    "timezone",
)


class GlobalIndexHistoryDAO(PostgresDAOBase):
    """Persistence helper for global index daily history."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "global_index_history_table", "global_index_history")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            trade_date_index=f"{self._table_name}_trade_date_idx",
        )

    def upsert(self, dataframe: pd.DataFrame, *, conn: Optional[PGConnection] = None) -> int:
        if dataframe.empty:
            return 0

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._upsert_dataframe(
                    owned_conn,
                    schema=self.config.schema,
                    table=self._table_name,
                    dataframe=dataframe,
                    columns=GLOBAL_INDEX_HISTORY_FIELDS,
                    conflict_keys=("code", "trade_date"),
                    date_columns=("trade_date",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=GLOBAL_INDEX_HISTORY_FIELDS,
            conflict_keys=("code", "trade_date"),
            date_columns=("trade_date",),
        )

    def latest_trade_dates(self, codes: Sequence[str]) -> Dict[str, date]:
        if not codes:
            return {}
        query = sql.SQL(
            """
            SELECT code, MAX(trade_date) AS latest
            FROM {schema}.{table}
            WHERE code = ANY(%s)
            GROUP BY code
            """
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (list(codes),))
                rows = cur.fetchall()

        return {code: latest for code, latest in rows if isinstance(latest, date)}

    def fetch_recent_rows(self, codes: Sequence[str], per_code: int = 2) -> Dict[str, List[Dict[str, object]]]:
        if not codes or per_code <= 0:
            return {}
        query = sql.SQL(
            """
            SELECT code,
                   name,
                   trade_date,
                   open_price,
                   high_price,
                   low_price,
                   close_price,
                   volume,
                   prev_close,
                   change_amount,
                   change_percent,
                   currency,
                   timezone,
                   ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) AS rn
            FROM {schema}.{table}
            WHERE code = ANY(%s)
            """
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        grouped: Dict[str, List[Dict[str, object]]] = {symbol: [] for symbol in codes}
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (list(codes),))
                for row in cur.fetchall():
                    code, name, trade_date, open_price, high_price, low_price, close_price, volume, prev_close, change_amount, change_percent, currency, timezone_value, rn = row
                    if rn > per_code:
                        continue
                    grouped.setdefault(code, []).append(
                        {
                            "code": code,
                            "name": name,
                            "trade_date": trade_date,
                            "open_price": open_price,
                            "high_price": high_price,
                            "low_price": low_price,
                            "close_price": close_price,
                            "volume": volume,
                            "prev_close": prev_close,
                            "change_amount": change_amount,
                            "change_percent": change_percent,
                            "currency": currency,
                            "timezone": timezone_value,
                        }
                    )
        for code in grouped:
            grouped[code].sort(key=lambda item: item.get("trade_date"), reverse=True)
        return grouped

    def list_history(self, code: str, *, limit: int = 500) -> List[Dict[str, object]]:
        normalized = (code or "").strip()
        if not normalized:
            return []

        limit_value = max(1, min(int(limit), 2000))
        columns = [
            "code",
            "name",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "prev_close",
            "change_amount",
            "change_percent",
            "currency",
            "timezone",
        ]
        query = sql.SQL(
            """
            SELECT code,
                   name,
                   trade_date,
                   open_price,
                   high_price,
                   low_price,
                   close_price,
                   volume,
                   prev_close,
                   change_amount,
                   change_percent,
                   currency,
                   timezone
            FROM {schema}.{table}
            WHERE code = %s
            ORDER BY trade_date DESC
            LIMIT %s
            """
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (normalized, limit_value))
                rows = cur.fetchall()

        return [{column: value for column, value in zip(columns, row)} for row in rows]

    def stats(self) -> Dict[str, Optional[datetime]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                count, latest = cur.fetchone()
        return {"count": count or 0, "updated_at": latest}


__all__ = ["GlobalIndexHistoryDAO", "GLOBAL_INDEX_HISTORY_FIELDS"]
