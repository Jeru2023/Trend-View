"""
DAO for market activity (赚钱效应) snapshot data.
"""

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


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "market_activity_schema.sql"

MARKET_ACTIVITY_FIELDS: Sequence[str] = (
    "metric",
    "display_order",
    "value_text",
    "value_number",
    "dataset_timestamp",
)


class MarketActivityDAO(PostgresDAOBase):
    """Persistence helper for market activity snapshots."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "market_activity_table", "market_activity")
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

        deduped = dataframe.drop_duplicates(subset=("metric",), keep="last")

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._upsert_dataframe(
                    owned_conn,
                    schema=self.config.schema,
                    table=self._table_name,
                    dataframe=deduped,
                    columns=MARKET_ACTIVITY_FIELDS,
                    conflict_keys=("metric",),
                    date_columns=("dataset_timestamp",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=deduped,
            columns=MARKET_ACTIVITY_FIELDS,
            conflict_keys=("metric",),
            date_columns=("dataset_timestamp",),
        )

    def list_entries(self) -> Dict[str, object]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT metric,
                               display_order,
                               value_text,
                               value_number,
                               dataset_timestamp,
                               updated_at
                        FROM {schema}.{table}
                        ORDER BY display_order ASC, metric ASC
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                rows = cur.fetchall()

        columns = [
            "metric",
            "display_order",
            "value_text",
            "value_number",
            "dataset_timestamp",
            "updated_at",
        ]

        items: List[Dict[str, object]] = []
        dataset_timestamp: Optional[datetime] = None

        for row in rows:
            payload = dict(zip(columns, row))
            value_number = payload.get("value_number")
            if isinstance(value_number, Decimal):
                payload["value_number"] = float(value_number) if value_number.is_finite() else None
            if dataset_timestamp is None and payload.get("dataset_timestamp"):
                dataset_timestamp = payload.get("dataset_timestamp")
            items.append(payload)

        return {
            "items": items,
            "dataset_timestamp": dataset_timestamp,
        }


__all__ = ["MarketActivityDAO", "MARKET_ACTIVITY_FIELDS"]

