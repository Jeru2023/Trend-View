"""
DAO for foreign commodity futures realtime snapshots.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "futures_realtime_schema.sql"

FUTURES_REALTIME_FIELDS: Sequence[str] = (
    "name",
    "code",
    "last_price",
    "price_cny",
    "change_amount",
    "change_percent",
    "open_price",
    "high_price",
    "low_price",
    "prev_settlement",
    "open_interest",
    "bid_price",
    "ask_price",
    "quote_time",
    "trade_date",
)


class FuturesRealtimeDAO(PostgresDAOBase):
    """Persistence helper for futures realtime data."""

    _conflict_keys: Sequence[str] = ("name",)

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "futures_realtime_table", "futures_realtime")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (updated_at DESC)").format(
                    index=sql.Identifier(f"{self._table_name}_updated_at_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
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
                    columns=FUTURES_REALTIME_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=("trade_date",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=FUTURES_REALTIME_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=("trade_date",),
        )

    def list_entries(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, object]:
        query = sql.SQL(
            """
            SELECT name,
                   code,
                   last_price,
                   price_cny,
                   change_amount,
                   change_percent,
                   open_price,
                   high_price,
                   low_price,
                   prev_settlement,
                   open_interest,
                   bid_price,
                   ask_price,
                   quote_time,
                   trade_date,
                   updated_at
            FROM {schema}.{table}
            ORDER BY name
            LIMIT %s OFFSET %s
            """
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        count_query = sql.SQL(
            "SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table}"
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query)
                total, last_updated = cur.fetchone()

                cur.execute(query, (limit, offset))
                rows = cur.fetchall()

        items: List[Dict[str, object]] = []
        for row in rows:
            (
                name,
                code,
                last_price,
                price_cny,
                change_amount,
                change_percent,
                open_price,
                high_price,
                low_price,
                prev_settlement,
                open_interest,
                bid_price,
                ask_price,
                quote_time,
                trade_date,
                updated_at,
            ) = row
            items.append(
                {
                    "name": name,
                    "code": code,
                    "last_price": last_price,
                    "price_cny": price_cny,
                    "change_amount": change_amount,
                    "change_percent": change_percent,
                    "open_price": open_price,
                    "high_price": high_price,
                    "low_price": low_price,
                    "prev_settlement": prev_settlement,
                    "open_interest": open_interest,
                    "bid_price": bid_price,
                    "ask_price": ask_price,
                    "quote_time": quote_time,
                    "trade_date": trade_date,
                    "updated_at": updated_at,
                }
            )

        return {
            "total": total or 0,
            "items": items,
            "updated_at": last_updated,
        }

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
                count, last_updated = cur.fetchone()
        return {"count": count or 0, "updated_at": last_updated}
