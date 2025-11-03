"""
DAO for Sina real-time China index snapshots.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "realtime_index_schema.sql"

REALTIME_INDEX_FIELDS: Sequence[str] = (
    "code",
    "name",
    "latest_price",
    "change_amount",
    "change_percent",
    "prev_close",
    "open_price",
    "high_price",
    "low_price",
    "volume",
    "turnover",
)


class RealtimeIndexDAO(PostgresDAOBase):
    """Persistence helper for AkShare Sina real-time China index data."""

    _conflict_keys: Sequence[str] = ("code",)

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "realtime_index_table", "realtime_indices")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            table_updated_at_idx=f"{self._table_name}_updated_at_idx",
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
                    columns=REALTIME_INDEX_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=(),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=REALTIME_INDEX_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=(),
        )

    def list_entries(self, *, limit: int = 500, offset: int = 0) -> Dict[str, object]:
        query = sql.SQL(
            """
            SELECT code,
                   name,
                   latest_price,
                   change_amount,
                   change_percent,
                   prev_close,
                   open_price,
                   high_price,
                   low_price,
                   volume,
                   turnover,
                   updated_at
            FROM {schema}.{table}
            ORDER BY code
            LIMIT %s OFFSET %s
            """
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        count_query = sql.SQL(
            "SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table}"
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query)
                total_row = cur.fetchone()
                total = total_row[0] if total_row else 0
                last_updated = total_row[1] if total_row else None

                cur.execute(query, (limit, offset))
                rows = cur.fetchall()

        items: List[dict[str, object]] = []
        for row in rows:
            (
                code,
                name,
                latest_price,
                change_amount,
                change_percent,
                prev_close,
                open_price,
                high_price,
                low_price,
                volume,
                turnover,
                updated_at,
            ) = row
            items.append(
                {
                    "code": code,
                    "name": name,
                    "latest_price": latest_price,
                    "change_amount": change_amount,
                    "change_percent": change_percent,
                    "prev_close": prev_close,
                    "open_price": open_price,
                    "high_price": high_price,
                    "low_price": low_price,
                    "volume": volume,
                    "turnover": turnover,
                    "updated_at": updated_at,
                }
            )

        return {"total": total or 0, "items": items, "updated_at": last_updated}

    def stats(self) -> Dict[str, Optional[datetime]]:
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


__all__ = ["RealtimeIndexDAO", "REALTIME_INDEX_FIELDS"]
