"""
DAO for performance express (业绩快报) data.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "performance_express_schema.sql"

PERFORMANCE_EXPRESS_FIELDS: Sequence[str] = (
    "ts_code",
    "ann_date",
    "end_date",
    "revenue",
    "operate_profit",
    "total_profit",
    "n_income",
    "total_assets",
    "total_hldr_eqy_exc_min_int",
    "diluted_eps",
    "diluted_roe",
    "yoy_net_profit",
    "bps",
    "perf_summary",
    "update_flag",
)


class PerformanceExpressDAO(PostgresDAOBase):
    """Persistence helper for Tushare performance express data."""

    _conflict_keys: Sequence[str] = ("ts_code", "end_date", "ann_date")

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "performance_express_table", "performance_express")
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
                    columns=PERFORMANCE_EXPRESS_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=("ann_date", "end_date"),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=PERFORMANCE_EXPRESS_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=("ann_date", "end_date"),
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
            SELECT ts_code, MAX(ann_date) AS latest_ann_date
            FROM {schema}.{table}
            {where_clause}
            GROUP BY ts_code
            """
        )

        where_clause = sql.SQL("")
        params: Sequence[object] = ()
        if codes:
            where_clause = sql.SQL("WHERE ts_code = ANY(%s)")
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
            conditions.append(sql.SQL("e.ann_date >= %s"))
            params.append(start_date)
        if end_date is not None:
            conditions.append(sql.SQL("e.ann_date <= %s"))
            params.append(end_date)
        if keyword:
            like_value = f"%{keyword.strip()}%"
            conditions.append(
                sql.SQL("(e.ts_code ILIKE %s OR COALESCE(sb.name, '') ILIKE %s)")
            )
            params.extend([like_value, like_value])

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        query = sql.SQL(
            """
            SELECT e.ts_code,
                   sb.name,
                   sb.industry,
                   sb.market,
                   e.ann_date,
                   e.end_date,
                   e.revenue,
                   e.operate_profit,
                   e.total_profit,
                   e.n_income,
                   e.total_assets,
                   e.total_hldr_eqy_exc_min_int,
                   e.diluted_eps,
                   e.diluted_roe,
                   e.yoy_net_profit,
                   e.bps,
                   e.perf_summary,
                   e.update_flag,
                   e.updated_at
            FROM {schema}.{table} AS e
            LEFT JOIN {schema}.{stock_table} AS sb ON sb.ts_code = e.ts_code
            {where_clause}
            ORDER BY e.ann_date DESC NULLS LAST,
                     e.end_date DESC NULLS LAST,
                     e.ts_code ASC
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
            FROM {schema}.{table} AS e
            LEFT JOIN {schema}.{stock_table} AS sb ON sb.ts_code = e.ts_code
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
            "ts_code",
            "name",
            "industry",
            "market",
            "ann_date",
            "end_date",
            "revenue",
            "operate_profit",
            "total_profit",
            "n_income",
            "total_assets",
            "total_hldr_eqy_exc_min_int",
            "diluted_eps",
            "diluted_roe",
            "yoy_net_profit",
            "bps",
            "perf_summary",
            "update_flag",
            "updated_at",
        ]

        def _to_float(value: object) -> Optional[float]:
            if value is None:
                return None
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return None
            return numeric

        results: List[dict[str, object]] = []
        for row in rows:
            record = dict(zip(columns, row))
            for key in (
                "revenue",
                "operate_profit",
                "total_profit",
                "n_income",
                "total_assets",
                "total_hldr_eqy_exc_min_int",
                "diluted_eps",
                "diluted_roe",
                "yoy_net_profit",
                "bps",
            ):
                record[key] = _to_float(record.get(key))
            results.append(record)
        return {"total": int(total), "items": results}


__all__ = [
    "PERFORMANCE_EXPRESS_FIELDS",
    "PerformanceExpressDAO",
]
