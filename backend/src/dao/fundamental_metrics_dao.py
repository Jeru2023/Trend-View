"""
Data access object for computed fundamental metrics.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence

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


__all__ = [
    "FundamentalMetricsDAO",
    "FUNDAMENTAL_METRICS_FIELDS",
]
