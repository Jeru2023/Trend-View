"""
Data access object for computed fundamental metrics.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "fundamental_metrics_schema.sql"

FUNDAMENTAL_METRICS_FIELDS: Sequence[str] = (
    "ts_code",
    "net_income_end_date_latest",
    "net_income_end_date_prev1",
    "net_income_end_date_prev2",
    "revenue_end_date_latest",
    "roe_end_date_latest",
    "net_income_yoy_latest",
    "net_income_yoy_prev1",
    "net_income_yoy_prev2",
    "net_income_qoq_latest",
    "revenue_yoy_latest",
    "revenue_qoq_latest",
    "roe_yoy_latest",
    "roe_qoq_latest",
)


class FundamentalMetricsDAO(PostgresDAOBase):
    """Handles persistence for derived fundamental metrics."""

    _conflict_keys: Sequence[str] = ("ts_code",)

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "fundamental_metrics_table", "fundamental_metrics")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )

    def replace_all(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            with self.connect() as conn:
                self.ensure_table(conn)
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
                            schema=sql.Identifier(self.config.schema),
                            table=sql.Identifier(self._table_name),
                        )
                    )
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
                columns=FUNDAMENTAL_METRICS_FIELDS,
                conflict_keys=self._conflict_keys,
                date_columns=(
                    "net_income_end_date_latest",
                    "net_income_end_date_prev1",
                    "net_income_end_date_prev2",
                    "revenue_end_date_latest",
                    "roe_end_date_latest",
                ),
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
                count, updated_at = cur.fetchone()
        return {"count": count or 0, "updated_at": updated_at}

    def fetch_metrics(self, codes: Sequence[str]) -> dict[str, dict]:
        if not codes:
            return {}

        query = sql.SQL(
            """
            SELECT ts_code,
                   net_income_yoy_latest,
                   net_income_yoy_prev1,
                   net_income_yoy_prev2,
                   net_income_qoq_latest,
                   revenue_yoy_latest,
                   revenue_qoq_latest,
                   roe_yoy_latest,
                   roe_qoq_latest
            FROM {schema}.{table}
            WHERE ts_code = ANY(%s)
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

        metrics: dict[str, dict] = {}
        for (
            ts_code,
            net_income_yoy_latest,
            net_income_yoy_prev1,
            net_income_yoy_prev2,
            net_income_qoq_latest,
            revenue_yoy_latest,
            revenue_qoq_latest,
            roe_yoy_latest,
            roe_qoq_latest,
        ) in rows:
            metrics[ts_code] = {
                "net_income_yoy_latest": net_income_yoy_latest,
                "net_income_yoy_prev1": net_income_yoy_prev1,
                "net_income_yoy_prev2": net_income_yoy_prev2,
                "net_income_qoq_latest": net_income_qoq_latest,
                "revenue_yoy_latest": revenue_yoy_latest,
                "revenue_qoq_latest": revenue_qoq_latest,
                "roe_yoy_latest": roe_yoy_latest,
                "roe_qoq_latest": roe_qoq_latest,
            }
        return metrics

    def query_metrics(
        self,
        *,
        keyword: str | None = None,
        market: str | None = None,
        exchange: str | None = None,
        include_st: bool = True,
        include_delisted: bool = True,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, object]:
        """
        Retrieve paginated fundamental metrics with optional filters.
        """
        stock_table = sql.Identifier(self.config.stock_table)
        schema_identifier = sql.Identifier(self.config.schema)
        metrics_table = sql.Identifier(self._table_name)

        select_base = sql.SQL(
            """
            SELECT fm.ts_code,
                   sb.name,
                   sb.industry,
                   sb.market,
                   sb.exchange,
                   fm.net_income_end_date_latest,
                   fm.net_income_end_date_prev1,
                   fm.net_income_end_date_prev2,
                   fm.revenue_end_date_latest,
                   fm.roe_end_date_latest,
                   fm.net_income_yoy_latest,
                   fm.net_income_yoy_prev1,
                   fm.net_income_yoy_prev2,
                   fm.net_income_qoq_latest,
                   fm.revenue_yoy_latest,
                   fm.revenue_qoq_latest,
                   fm.roe_yoy_latest,
                   fm.roe_qoq_latest
            FROM {schema}.{metrics_table} AS fm
            LEFT JOIN {schema}.{stock_table} AS sb
                ON sb.ts_code = fm.ts_code
            """
        ).format(schema=schema_identifier, metrics_table=metrics_table, stock_table=stock_table)

        count_base = sql.SQL(
            """
            SELECT COUNT(*)
            FROM {schema}.{metrics_table} AS fm
            LEFT JOIN {schema}.{stock_table} AS sb
                ON sb.ts_code = fm.ts_code
            """
        ).format(schema=schema_identifier, metrics_table=metrics_table, stock_table=stock_table)

        conditions: list[sql.SQL] = []
        params: list[object] = []

        if keyword:
            like_value = f"%{keyword}%"
            conditions.append(
                sql.SQL(
                    "(fm.ts_code ILIKE %s OR sb.name ILIKE %s OR sb.industry ILIKE %s OR sb.symbol ILIKE %s)"
                )
            )
            params.extend([like_value, like_value, like_value, like_value])

        if market and market.lower() != "all":
            conditions.append(sql.SQL("sb.market = %s"))
            params.append(market)

        if exchange and exchange.lower() != "all":
            conditions.append(sql.SQL("sb.exchange = %s"))
            params.append(exchange)

        if not include_delisted:
            exclude_statuses = ["D", "P"]
            placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in exclude_statuses)
            conditions.append(
                sql.SQL("COALESCE(sb.list_status, 'L') NOT IN ({statuses})").format(statuses=placeholders)
            )
            params.extend(exclude_statuses)

        if not include_st:
            for prefix in ("ST", "*ST"):
                conditions.append(sql.SQL("sb.name NOT ILIKE %s"))
                params.append(f"{prefix}%")

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conditions)

        query = (
            select_base
            + where_clause
            + sql.SQL(" ORDER BY fm.ts_code LIMIT %s OFFSET %s")
        )
        query_params = params + [limit, offset]

        count_query = count_base + where_clause
        count_params = params.copy()

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query, count_params)
                total = cur.fetchone()[0]
                cur.execute(query, query_params)
                rows = cur.fetchall()

        items: list[Dict[str, object]] = []
        for row in rows:
            (
                ts_code,
                name,
                industry,
                market_value,
                exchange_value,
                net_income_end_date_latest,
                net_income_end_date_prev1,
                net_income_end_date_prev2,
                revenue_end_date_latest,
                roe_end_date_latest,
                net_income_yoy_latest,
                net_income_yoy_prev1,
                net_income_yoy_prev2,
                net_income_qoq_latest,
                revenue_yoy_latest,
                revenue_qoq_latest,
                roe_yoy_latest,
                roe_qoq_latest,
            ) = row
            items.append(
                {
                    "code": ts_code,
                    "name": name,
                    "industry": industry,
                    "market": market_value,
                    "exchange": exchange_value,
                    "net_income_end_date_latest": net_income_end_date_latest,
                    "net_income_end_date_prev1": net_income_end_date_prev1,
                    "net_income_end_date_prev2": net_income_end_date_prev2,
                    "revenue_end_date_latest": revenue_end_date_latest,
                    "roe_end_date_latest": roe_end_date_latest,
                    "net_income_yoy_latest": net_income_yoy_latest,
                    "net_income_yoy_prev1": net_income_yoy_prev1,
                    "net_income_yoy_prev2": net_income_yoy_prev2,
                    "net_income_qoq_latest": net_income_qoq_latest,
                    "revenue_yoy_latest": revenue_yoy_latest,
                    "revenue_qoq_latest": revenue_qoq_latest,
                    "roe_yoy_latest": roe_yoy_latest,
                    "roe_qoq_latest": roe_qoq_latest,
                }
            )

        return {"total": total, "items": items}


__all__ = [
    "FundamentalMetricsDAO",
    "FUNDAMENTAL_METRICS_FIELDS",
]
