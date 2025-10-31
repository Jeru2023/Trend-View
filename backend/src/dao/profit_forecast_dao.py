"""
DAO for profit forecast (盈利预测) data sourced from AkShare.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "profit_forecast_schema.sql"

PROFIT_FORECAST_FIELDS: Sequence[str] = (
    "symbol",
    "ts_code",
    "stock_name",
    "report_count",
    "rating_buy",
    "rating_add",
    "rating_neutral",
    "rating_reduce",
    "rating_sell",
    "forecast_year",
    "forecast_eps",
)


class ProfitForecastDAO(PostgresDAOBase):
    """Persistence helper for AkShare profit forecast data."""

    _conflict_keys: Sequence[str] = ("symbol", "forecast_year")

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "profit_forecast_table", "profit_forecast")
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
                sql.SQL("CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (ts_code)").format(
                    index=sql.Identifier(f"{self._table_name}_ts_code_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (updated_at DESC)").format(
                    index=sql.Identifier(f"{self._table_name}_updated_at_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )

    def clear_table(self) -> int:
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

    def delete_symbols(self, symbols: Sequence[str], *, conn: Optional[PGConnection] = None) -> None:
        if not symbols:
            return
        query = sql.SQL(
            "DELETE FROM {schema}.{table} WHERE symbol = ANY(%s)"
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))
        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                with owned_conn.cursor() as cur:
                    cur.execute(query, (list(symbols),))
        else:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (list(symbols),))

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
                    columns=PROFIT_FORECAST_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=(),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=PROFIT_FORECAST_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=(),
        )

    def stats(self) -> Dict[str, Optional[datetime]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table}"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                count, last_updated = cur.fetchone()
        return {"count": count or 0, "updated_at": last_updated}

    def list_entries(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        keyword: Optional[str] = None,
        industry: Optional[str] = None,
        forecast_year: Optional[int] = None,
    ) -> Dict[str, object]:
        conditions: List[sql.SQL] = []
        params: List[object] = []

        if keyword:
            like_value = f"%{keyword.strip()}%"
            conditions.append(
                sql.SQL(
                    "("
                    "COALESCE(NULLIF(pf.ts_code, ''), '') ILIKE %s "
                    "OR pf.symbol ILIKE %s "
                    "OR COALESCE(sb.name, '') ILIKE %s "
                    "OR COALESCE(sb.ts_code, '') ILIKE %s"
                    ")"
                )
            )
            params.extend([like_value, like_value, like_value, like_value])

        if industry:
            conditions.append(sql.SQL("COALESCE(sb.industry, '') = %s"))
            params.append(industry)

        if forecast_year is not None:
            conditions.append(sql.SQL("pf.forecast_year = %s"))
            params.append(forecast_year)

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        base_table = sql.Identifier(self.config.schema, self._table_name)
        stock_table = sql.Identifier(self.config.schema, self.config.stock_table)

        total_query = sql.SQL(
            """
            SELECT COUNT(DISTINCT pf.symbol)
            FROM {base_table} AS pf
            LEFT JOIN {stock_table} AS sb ON sb.symbol = pf.symbol
            {where_clause}
            """
        ).format(base_table=base_table, stock_table=stock_table, where_clause=where_clause)

        query = sql.SQL(
            """
            SELECT
                pf.symbol,
                COALESCE(NULLIF(pf.ts_code, ''), sb.ts_code, pf.symbol) AS ts_code,
                MAX(COALESCE(sb.name, pf.stock_name)) AS name,
                MAX(sb.industry) AS industry,
                MAX(sb.market) AS market,
                MAX(pf.report_count) AS report_count,
                MAX(pf.rating_buy) AS rating_buy,
                MAX(pf.rating_add) AS rating_add,
                MAX(pf.rating_neutral) AS rating_neutral,
                MAX(pf.rating_reduce) AS rating_reduce,
                MAX(pf.rating_sell) AS rating_sell,
                MAX(pf.updated_at) AS updated_at,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'year', pf.forecast_year,
                        'eps', pf.forecast_eps
                    )
                    ORDER BY pf.forecast_year
                ) AS forecasts
            FROM {base_table} AS pf
            LEFT JOIN {stock_table} AS sb ON sb.symbol = pf.symbol
            {where_clause}
            GROUP BY pf.symbol,
                     COALESCE(NULLIF(pf.ts_code, ''), sb.ts_code, pf.symbol)
            ORDER BY MAX(pf.report_count) DESC NULLS LAST, pf.symbol
            LIMIT %s OFFSET %s
            """
        ).format(base_table=base_table, stock_table=stock_table, where_clause=where_clause)

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(total_query, params)
                total_row = cur.fetchone()
                total = total_row[0] if total_row else 0

                cur.execute(query, [*params, limit, offset])
                rows = cur.fetchall()

        items: List[dict[str, object]] = []
        for row in rows:
            (
                symbol,
                ts_code,
                name,
                industry_value,
                market_value,
                report_count,
                rating_buy,
                rating_add,
                rating_neutral,
                rating_reduce,
                rating_sell,
                updated_at,
                forecasts_json,
            ) = row

            forecasts: List[dict[str, object]] = []
            if forecasts_json:
                if isinstance(forecasts_json, str):
                    forecasts = json.loads(forecasts_json)
                else:
                    forecasts = [dict(item) for item in forecasts_json]

            items.append(
                {
                    "symbol": symbol,
                    "ts_code": ts_code,
                    "name": name,
                    "industry": industry_value,
                    "market": market_value,
                    "report_count": report_count,
                    "rating_buy": rating_buy,
                    "rating_add": rating_add,
                    "rating_neutral": rating_neutral,
                    "rating_reduce": rating_reduce,
                    "rating_sell": rating_sell,
                    "updated_at": updated_at,
                    "forecasts": forecasts,
                }
            )

        return {"total": total, "items": items}

    def list_years(self) -> List[int]:
        query = sql.SQL(
            "SELECT DISTINCT forecast_year FROM {schema}.{table} ORDER BY forecast_year"
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
        return [int(row[0]) for row in rows if row and row[0] is not None]
