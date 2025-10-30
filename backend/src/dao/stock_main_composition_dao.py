"""DAO for Eastmoney stock main composition data."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "stock_main_composition_schema.sql"

STOCK_MAIN_COMPOSITION_FIELDS: Sequence[str] = (
    "symbol",
    "report_date",
    "category_type",
    "composition",
    "revenue",
    "revenue_ratio",
    "cost",
    "cost_ratio",
    "profit",
    "profit_ratio",
    "gross_margin",
)


class StockMainCompositionDAO(PostgresDAOBase):
    """Persistence helper for stock main composition records."""

    _conflict_keys: Sequence[str] = ("symbol", "report_date", "category_type", "composition")

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "stock_main_composition_table", "stock_main_composition")
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

        deduped = dataframe.drop_duplicates(subset=list(self._conflict_keys), keep="last")

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._upsert_dataframe(
                    owned_conn,
                    schema=self.config.schema,
                    table=self._table_name,
                    dataframe=deduped,
                    columns=STOCK_MAIN_COMPOSITION_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=("report_date",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=deduped,
            columns=STOCK_MAIN_COMPOSITION_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=("report_date",),
        )

    def stats(self) -> dict[str, Optional[object]]:
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

    def list_symbols(self) -> list[str]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT DISTINCT symbol FROM {schema}.{table} ORDER BY symbol").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                rows = cur.fetchall()
        return [row[0] for row in rows if row and isinstance(row[0], str)]

    def get_max_report_date(self, symbol: str) -> Optional[date]:
        normalized = (symbol or "").strip()
        if not normalized:
            return None
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT MAX(report_date) FROM {schema}.{table} WHERE symbol = %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (normalized,),
                )
                value = cur.fetchone()[0]
        return value

    def list_entries(
        self,
        symbol: str,
        *,
        report_date: Optional[date] = None,
        latest_only: bool = True,
    ) -> List[dict[str, object]]:
        normalized = (symbol or "").strip()
        if not normalized:
            return []

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                if report_date is None and latest_only:
                    query = sql.SQL(
                        """
                        SELECT symbol,
                               report_date,
                               category_type,
                               composition,
                               revenue,
                               revenue_ratio,
                               cost,
                               cost_ratio,
                               profit,
                               profit_ratio,
                               gross_margin,
                               updated_at
                        FROM {schema}.{table}
                        WHERE symbol = %s
                          AND report_date = (
                              SELECT MAX(report_date) FROM {schema}.{table} WHERE symbol = %s
                          )
                        ORDER BY report_date DESC, category_type NULLS LAST, composition NULLS LAST
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                    params = (normalized, normalized)
                else:
                    conditions = [sql.SQL("symbol = %s")]
                    params_list: List[object] = [normalized]
                    if report_date is not None:
                        conditions.append(sql.SQL("report_date = %s"))
                        params_list.append(report_date)
                    query = sql.SQL(
                        """
                        SELECT symbol,
                               report_date,
                               category_type,
                               composition,
                               revenue,
                               revenue_ratio,
                               cost,
                               cost_ratio,
                               profit,
                               profit_ratio,
                               gross_margin,
                               updated_at
                        FROM {schema}.{table}
                        WHERE {conditions}
                        ORDER BY report_date DESC, category_type NULLS LAST, composition NULLS LAST
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                        conditions=sql.SQL(" AND ").join(conditions),
                    )
                    params = tuple(params_list)

                cur.execute(query, params)
                rows = cur.fetchall()

        columns = [
            "symbol",
            "report_date",
            "category_type",
            "composition",
            "revenue",
            "revenue_ratio",
            "cost",
            "cost_ratio",
            "profit",
            "profit_ratio",
            "gross_margin",
            "updated_at",
        ]

        results: List[dict[str, object]] = []
        for row in rows:
            record = dict(zip(columns, row))
            results.append(record)

        return results


__all__ = [
    "StockMainCompositionDAO",
    "STOCK_MAIN_COMPOSITION_FIELDS",
]
