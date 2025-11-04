"""DAO for market fund flow historical data."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "market_fund_flow_schema.sql"

MARKET_FUND_FLOW_FIELDS: Sequence[str] = (
    "trade_date",
    "shanghai_close",
    "shanghai_change_percent",
    "shenzhen_close",
    "shenzhen_change_percent",
    "main_net_inflow_amount",
    "main_net_inflow_ratio",
    "huge_order_net_inflow_amount",
    "huge_order_net_inflow_ratio",
    "large_order_net_inflow_amount",
    "large_order_net_inflow_ratio",
    "medium_order_net_inflow_amount",
    "medium_order_net_inflow_ratio",
    "small_order_net_inflow_amount",
    "small_order_net_inflow_ratio",
)

NUMERIC_FIELDS: Sequence[str] = tuple(field for field in MARKET_FUND_FLOW_FIELDS if field != "trade_date")


class MarketFundFlowDAO(PostgresDAOBase):
    """Persistence helper for market fund flow history."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "market_fund_flow_table", "market_fund_flow")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            trade_date_idx=f"{self._table_name}_trade_date_idx",
        )

    def upsert(self, dataframe: pd.DataFrame, *, conn: Optional[PGConnection] = None) -> int:
        if dataframe.empty:
            return 0

        deduped = dataframe.drop_duplicates(subset=("trade_date",), keep="last")

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._upsert_dataframe(
                    owned_conn,
                    schema=self.config.schema,
                    table=self._table_name,
                    dataframe=deduped,
                    columns=MARKET_FUND_FLOW_FIELDS,
                    conflict_keys=("trade_date",),
                    date_columns=("trade_date",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=deduped,
            columns=MARKET_FUND_FLOW_FIELDS,
            conflict_keys=("trade_date",),
            date_columns=("trade_date",),
        )

    def stats(self) -> Dict[str, Optional[datetime]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT COUNT(*), MAX(updated_at), MAX(trade_date) FROM {schema}.{table}"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                count, last_updated, latest_trade_date = cur.fetchone()
        return {
            "count": count or 0,
            "updated_at": last_updated,
            "latest_trade_date": latest_trade_date,
        }

    def list_entries(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, object]:
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        conditions: List[sql.SQL] = []
        params: List[object] = []

        if start_date:
            conditions.append(sql.SQL("f.trade_date >= %s"))
            params.append(start_date)
        if end_date:
            conditions.append(sql.SQL("f.trade_date <= %s"))
            params.append(end_date)

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        base_query = sql.SQL(
            """
            SELECT f.trade_date,
                   f.shanghai_close,
                   f.shanghai_change_percent,
                   f.shenzhen_close,
                   f.shenzhen_change_percent,
                   f.main_net_inflow_amount,
                   f.main_net_inflow_ratio,
                   f.huge_order_net_inflow_amount,
                   f.huge_order_net_inflow_ratio,
                   f.large_order_net_inflow_amount,
                   f.large_order_net_inflow_ratio,
                   f.medium_order_net_inflow_amount,
                   f.medium_order_net_inflow_ratio,
                   f.small_order_net_inflow_amount,
                   f.small_order_net_inflow_ratio,
                   f.updated_at
            FROM {schema}.{table} AS f
            {where_clause}
            ORDER BY f.trade_date DESC
            LIMIT %s OFFSET %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        count_query = sql.SQL(
            "SELECT COUNT(*), MAX(updated_at), MAX(trade_date) FROM {schema}.{table} AS f {where_clause}"
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        query_params = params + [limit, offset]

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query, params)
                total_row = cur.fetchone()
                total = total_row[0] if total_row else 0
                last_updated = total_row[1] if total_row else None
                latest_trade_date = total_row[2] if total_row else None
                cur.execute(base_query, query_params)
                rows = cur.fetchall()

            available_years: List[int] = []
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT DISTINCT EXTRACT(YEAR FROM trade_date)::int
                        FROM {schema}.{table}
                        ORDER BY 1 DESC
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                available_years = [row[0] for row in cur.fetchall()]

        columns = list(MARKET_FUND_FLOW_FIELDS) + ["updated_at"]
        items: List[Dict[str, object]] = []
        for row in rows:
            payload = dict(zip(columns, row))
            for field in NUMERIC_FIELDS:
                value = payload.get(field)
                if isinstance(value, Decimal):
                    payload[field] = float(value) if value.is_finite() else None
            items.append(payload)

        return {
            "total": total or 0,
            "items": items,
            "updated_at": last_updated,
            "latest_trade_date": latest_trade_date,
            "available_years": available_years,
        }


__all__ = ["MarketFundFlowDAO", "MARKET_FUND_FLOW_FIELDS"]
