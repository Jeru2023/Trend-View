"""
DAO for HSGT (Shanghai-Hong Kong / Shenzhen-Hong Kong Connect) fund flow history data.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "hsgt_fund_flow_schema.sql"
DEFAULT_SYMBOL = "北向资金"

HSGT_FUND_FLOW_FIELDS: Sequence[str] = (
    "symbol",
    "trade_date",
    "net_buy_amount",
    "buy_amount",
    "sell_amount",
    "net_buy_amount_cumulative",
    "fund_inflow",
    "balance",
    "market_value",
    "leading_stock",
    "leading_stock_change_percent",
    "hs300_index",
    "hs300_change_percent",
    "leading_stock_code",
)

_NUMERIC_FIELDS: Sequence[str] = (
    "net_buy_amount",
    "buy_amount",
    "sell_amount",
    "net_buy_amount_cumulative",
    "fund_inflow",
    "balance",
    "market_value",
    "leading_stock_change_percent",
    "hs300_index",
    "hs300_change_percent",
)


class HSGTFundFlowDAO(PostgresDAOBase):
    """Persistence helper for HSGT fund flow history data."""

    _conflict_keys: Sequence[str] = ("symbol", "trade_date")

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "hsgt_fund_flow_table", "hsgt_fund_flow")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        # Drop legacy table layout without symbol column
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                  AND column_name = 'symbol'
                """,
                (self.config.schema, self._table_name),
            )
            has_symbol_column = cur.fetchone() is not None

        if not has_symbol_column:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {schema}.{table} CASCADE").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )

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

        deduped = dataframe.drop_duplicates(subset=self._conflict_keys, keep="last")

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._upsert_dataframe(
                    owned_conn,
                    schema=self.config.schema,
                    table=self._table_name,
                    dataframe=deduped,
                    columns=HSGT_FUND_FLOW_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=("trade_date",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=deduped,
            columns=HSGT_FUND_FLOW_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=("trade_date",),
        )

    def stats(self) -> Dict[str, Optional[datetime]]:
        available_years: List[int] = []

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

    def list_entries(
        self,
        *,
        symbol: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> Dict[str, object]:
        symbol_value = (symbol or DEFAULT_SYMBOL).strip()

        conditions: List[sql.SQL] = []
        params: List[object] = []

        if symbol_value:
            conditions.append(sql.SQL("f.symbol = %s"))
            params.append(symbol_value)

        if start_date:
            conditions.append(sql.SQL("f.trade_date >= %s"))
            params.append(start_date)

        if end_date:
            conditions.append(sql.SQL("f.trade_date <= %s"))
            params.append(end_date)

        conditions.append(sql.SQL("f.net_buy_amount IS NOT NULL"))

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        base_query = sql.SQL(
            """
            SELECT f.trade_date,
                   f.symbol,
                   f.net_buy_amount,
                   f.buy_amount,
                   f.sell_amount,
                   f.net_buy_amount_cumulative,
                   f.fund_inflow,
                   f.balance,
                   f.market_value,
                   f.leading_stock,
                   f.leading_stock_change_percent,
                   f.hs300_index,
                   f.hs300_change_percent,
                   f.leading_stock_code,
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
            "SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table} AS f {where_clause}"
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
                cur.execute(base_query, query_params)
                rows = cur.fetchall()

            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT DISTINCT EXTRACT(YEAR FROM trade_date)::int
                        FROM {schema}.{table}
                        WHERE symbol = %s
                          AND net_buy_amount IS NOT NULL
                        ORDER BY 1 DESC
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (symbol_value or DEFAULT_SYMBOL,),
                )
                available_years = [row[0] for row in cur.fetchall()]

        columns = [
            "trade_date",
            "symbol",
            "net_buy_amount",
            "buy_amount",
            "sell_amount",
            "net_buy_amount_cumulative",
            "fund_inflow",
            "balance",
            "market_value",
            "leading_stock",
            "leading_stock_change_percent",
            "hs300_index",
            "hs300_change_percent",
            "leading_stock_code",
            "updated_at",
        ]

        items: List[Dict[str, object]] = []
        for row in rows:
            payload = dict(zip(columns, row))
            for field in _NUMERIC_FIELDS:
                value = payload.get(field)
                if isinstance(value, Decimal):
                    payload[field] = float(value) if value.is_finite() else None
            items.append(payload)

        return {
            "total": total or 0,
            "items": items,
            "updated_at": last_updated,
            "available_years": available_years,
        }

    def purge_rows_without_net_buy(self, conn: Optional[PGConnection] = None) -> int:
        if conn is None:
            with self.connect() as owned_conn:
                return self.purge_rows_without_net_buy(owned_conn)

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {schema}.{table} WHERE net_buy_amount IS NULL").format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            return cur.rowcount or 0


__all__ = ["HSGTFundFlowDAO", "HSGT_FUND_FLOW_FIELDS"]
