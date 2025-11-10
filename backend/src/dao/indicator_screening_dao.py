"""DAO for indicator-based stock screening results."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "indicator_screening_schema.sql"

INDICATOR_SCREENING_COLUMNS: Sequence[str] = (
    "indicator_code",
    "indicator_name",
    "captured_at",
    "rank",
    "stock_code",
    "stock_code_full",
    "stock_name",
    "price_change_percent",
    "stage_change_percent",
    "last_price",
    "volume_shares",
    "volume_text",
    "baseline_volume_shares",
    "baseline_volume_text",
    "volume_days",
    "turnover_percent",
    "turnover_rate",
    "turnover_amount",
    "turnover_amount_text",
    "industry",
    "high_price",
    "low_price",
)


class IndicatorScreeningDAO(PostgresDAOBase):
    """Persistence helper for indicator screening datasets."""

    _conflict_keys: Sequence[str] = ("indicator_code", "stock_code")
    _date_columns: Sequence[str] = ("captured_at",)

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "indicator_screening_table", "indicator_screening")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            indicator_rank_idx=f"{self._table_name}_indicator_rank_idx",
        )

    def upsert(self, dataframe: pd.DataFrame, *, conn: Optional[PGConnection] = None) -> int:
        if dataframe is None or dataframe.empty:
            return 0

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._write_dataframe(owned_conn, dataframe)

        self.ensure_table(conn)
        return self._write_dataframe(conn, dataframe)

    def _write_dataframe(self, conn: PGConnection, dataframe: pd.DataFrame) -> int:
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=INDICATOR_SCREENING_COLUMNS,
            conflict_keys=self._conflict_keys,
            date_columns=self._date_columns,
        )

    def list_entries(
        self,
        *,
        indicator_code: str,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, object]:
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))

        columns = ["indicator_code", "indicator_name", "captured_at", *INDICATOR_SCREENING_COLUMNS[3:]]
        select_list = sql.SQL(", ").join(sql.Identifier(col) for col in columns)

        query = sql.SQL(
            """
            SELECT {columns}
            FROM {schema}.{table}
            WHERE indicator_code = %s
            ORDER BY rank NULLS LAST, stock_code
            LIMIT %s OFFSET %s
            """
        ).format(
            columns=select_list,
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        stats_query = sql.SQL(
            """
            SELECT COUNT(*), MAX(captured_at)
            FROM {schema}.{table}
            WHERE indicator_code = %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(stats_query, (indicator_code,))
                count, latest_captured = cur.fetchone()
                cur.execute(query, (indicator_code, limit, offset))
                rows = cur.fetchall()

        items: List[dict[str, object]] = []
        for row in rows:
            items.append({column: value for column, value in zip(columns, row)})

        return {
            "total": int(count or 0),
            "items": items,
            "latest_captured_at": latest_captured,
            "indicator_code": indicator_code,
        }
