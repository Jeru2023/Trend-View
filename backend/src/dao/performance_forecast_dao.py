"""
DAO for performance forecast (业绩预告) data sourced from AkShare.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import math
import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "performance_forecast_schema.sql"

PERFORMANCE_FORECAST_FIELDS: Sequence[str] = (
    "symbol",
    "ts_code",
    "stock_name",
    "report_period",
    "forecast_metric",
    "change_description",
    "forecast_value",
    "change_rate",
    "change_reason",
    "forecast_type",
    "last_year_value",
    "announcement_date",
    "row_number",
)


class PerformanceForecastDAO(PostgresDAOBase):
    """Persistence helper for AkShare performance forecast data."""

    _conflict_keys: Sequence[str] = ("symbol", "report_period", "forecast_metric", "forecast_type")

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "performance_forecast_table", "performance_forecast")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                """,
                (self.config.schema, self._table_name),
            )
            existing_columns = {row[0] for row in cur.fetchall()}

        if existing_columns and "report_period" not in existing_columns:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP TABLE {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )

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
                    columns=PERFORMANCE_FORECAST_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=("report_period", "announcement_date"),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=PERFORMANCE_FORECAST_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=("report_period", "announcement_date"),
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

    def latest_ann_dates(
        self,
        codes: Sequence[str] | None = None,
        *,
        conn: Optional[PGConnection] = None,
    ) -> Dict[str, Optional[date]]:
        base_query = sql.SQL(
            """
            SELECT COALESCE(NULLIF(ts_code, ''), symbol) AS code,
                   MAX(announcement_date) AS latest_ann_date
            FROM {schema}.{table}
            {where_clause}
            GROUP BY COALESCE(NULLIF(ts_code, ''), symbol)
            """
        )

        where_clause = sql.SQL("")
        params: Sequence[object] = ()
        if codes:
            where_clause = sql.SQL("WHERE COALESCE(NULLIF(ts_code, ''), symbol) = ANY(%s)")
            params = (list(codes),)

        query = base_query.format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                with owned_conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
        else:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

        return {ts_code: ann_date for ts_code, ann_date in rows}

    def list_entries(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        keyword: Optional[str] = None,
    ) -> List[dict[str, object]]:
        conditions: List[sql.SQL] = []
        params: List[object] = []

        if start_date is not None:
            conditions.append(sql.SQL("f.announcement_date >= %s"))
            params.append(start_date)
        if end_date is not None:
            conditions.append(sql.SQL("f.announcement_date <= %s"))
            params.append(end_date)
        if keyword:
            like_value = f"%{keyword.strip()}%"
            conditions.append(
                sql.SQL(
                    "("
                    "COALESCE(NULLIF(f.ts_code, ''), '') ILIKE %s "
                    "OR f.symbol ILIKE %s "
                    "OR COALESCE(f.stock_name, '') ILIKE %s "
                    "OR COALESCE(sb.name, '') ILIKE %s "
                    "OR COALESCE(sb.ts_code, '') ILIKE %s"
                    ")"
                )
            )
            params.extend([like_value, like_value, like_value, like_value, like_value])

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        query = sql.SQL(
            """
            SELECT f.symbol,
                   COALESCE(NULLIF(f.ts_code, ''), sb.ts_code) AS ts_code,
                   COALESCE(sb.name, f.stock_name) AS name,
                   sb.industry,
                   sb.market,
                   f.report_period,
                   f.announcement_date,
                   f.forecast_metric,
                   f.change_description,
                   f.forecast_value,
                   f.change_rate,
                   f.change_reason,
                   f.forecast_type,
                   f.last_year_value,
                   f.row_number,
                   f.updated_at
            FROM {schema}.{table} AS f
            LEFT JOIN {schema}.{stock_table} AS sb ON sb.symbol = f.symbol
            {where_clause}
            ORDER BY f.report_period DESC NULLS LAST,
                     f.announcement_date DESC NULLS LAST,
                     f.symbol ASC
            LIMIT %s OFFSET %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            stock_table=sql.Identifier(self.config.stock_table),
            where_clause=where_clause,
        )

        count_query = sql.SQL("""
            SELECT COUNT(*)
            FROM {schema}.{table} AS f
            LEFT JOIN {schema}.{stock_table} AS sb ON sb.symbol = f.symbol
            {where_clause}
        """)
        count_params = list(params)

        params.extend([limit, offset])

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query.format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                    stock_table=sql.Identifier(self.config.stock_table),
                    where_clause=where_clause,
                ), count_params)
                total = cur.fetchone()[0] or 0
                cur.execute(query, params)
                rows = cur.fetchall()

        columns = [
            "symbol",
            "ts_code",
            "name",
            "industry",
            "market",
            "report_period",
            "announcement_date",
            "forecast_metric",
            "change_description",
            "forecast_value",
            "change_rate",
            "change_reason",
            "forecast_type",
            "last_year_value",
            "row_number",
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

        results: List[dict[str, object]] = []
        for row in rows:
            record = dict(zip(columns, row))
            for key in ("forecast_value", "change_rate", "last_year_value"):
                record[key] = _to_float(record.get(key))
            results.append(record)
        return {"total": int(total), "items": results}


__all__ = [
    "PERFORMANCE_FORECAST_FIELDS",
    "PerformanceForecastDAO",
]
