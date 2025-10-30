"""DAO for industry fund flow data sourced from AkShare."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence

import math
import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "industry_fund_flow_schema.sql"

INDUSTRY_FUND_FLOW_FIELDS: Sequence[str] = (
    "symbol",
    "industry",
    "rank",
    "industry_index",
    "price_change_percent",
    "stage_change_percent",
    "inflow",
    "outflow",
    "net_amount",
    "company_count",
    "leading_stock",
    "leading_stock_change_percent",
    "current_price",
)


class IndustryFundFlowDAO(PostgresDAOBase):
    """Persistence helper for industry fund flow data."""

    _conflict_keys: Sequence[str] = ("symbol", "industry")

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "industry_fund_flow_table", "industry_fund_flow")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
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
                    columns=INDUSTRY_FUND_FLOW_FIELDS,
                    conflict_keys=self._conflict_keys,
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=INDUSTRY_FUND_FLOW_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=(),
        )

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

    def list_entries(
        self,
        *,
        symbol: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, object]:
        conditions: List[sql.SQL] = []
        params: List[object] = []

        if symbol:
            conditions.append(sql.SQL("f.symbol = %s"))
            params.append(symbol)

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        base_query = sql.SQL(
            """
            SELECT f.symbol,
                   f.industry,
                   f.rank,
                   f.industry_index,
                   f.price_change_percent,
                   f.stage_change_percent,
                   f.inflow,
                   f.outflow,
                   f.net_amount,
                   f.company_count,
                   f.leading_stock,
                   f.leading_stock_change_percent,
                   f.current_price,
                   f.updated_at
            FROM {schema}.{table} AS f
            {where_clause}
            ORDER BY f.symbol ASC, f.rank ASC NULLS LAST, f.industry ASC
            LIMIT %s OFFSET %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        count_query = sql.SQL(
            "SELECT COUNT(*) FROM {schema}.{table} AS f {where_clause}"
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
                total = cur.fetchone()[0] or 0
                cur.execute(base_query, query_params)
                rows = cur.fetchall()

        columns = [
            "symbol",
            "industry",
            "rank",
            "industry_index",
            "price_change_percent",
            "stage_change_percent",
            "inflow",
            "outflow",
            "net_amount",
            "company_count",
            "leading_stock",
            "leading_stock_change_percent",
            "current_price",
            "updated_at",
        ]

        def _to_float(value: object) -> Optional[float]:
            if value is None:
                return None
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(numeric):
                return None
            return numeric

        def _to_int(value: object) -> Optional[int]:
            if value is None:
                return None
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return None

        items: List[dict[str, object]] = []
        for row in rows:
            record = dict(zip(columns, row))
            for key in (
                "industry_index",
                "price_change_percent",
                "stage_change_percent",
                "inflow",
                "outflow",
                "net_amount",
                "leading_stock_change_percent",
                "current_price",
            ):
                record[key] = _to_float(record.get(key))
            record["company_count"] = _to_int(record.get("company_count"))
            record["rank"] = _to_int(record.get("rank"))
            items.append(record)

        return {"total": int(total), "items": items}


__all__ = [
    "INDUSTRY_FUND_FLOW_FIELDS",
    "IndustryFundFlowDAO",
]
