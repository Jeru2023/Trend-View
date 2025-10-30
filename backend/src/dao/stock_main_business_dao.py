"""DAO for Tonghuashun stock main business information."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "stock_main_business_schema.sql"

STOCK_MAIN_BUSINESS_FIELDS: Sequence[str] = (
    "symbol",
    "ts_code",
    "main_business",
    "product_type",
    "product_name",
    "business_scope",
)


class StockMainBusinessDAO(PostgresDAOBase):
    """Persistence helper for Tonghuashun stock main business data."""

    _conflict_keys: Sequence[str] = ("symbol",)

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "stock_main_business_table", "stock_main_business")
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
                    columns=STOCK_MAIN_BUSINESS_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=(),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=deduped,
            columns=STOCK_MAIN_BUSINESS_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=(),
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
                    sql.SQL("SELECT symbol FROM {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                rows = cur.fetchall()
        return [row[0] for row in rows if row and isinstance(row[0], str)]

    def get_entry(self, symbol: str) -> Optional[dict[str, object]]:
        normalized = (symbol or "").strip()
        if not normalized:
            return None

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT symbol,
                               ts_code,
                               main_business,
                               product_type,
                               product_name,
                               business_scope,
                               updated_at
                        FROM {schema}.{table}
                        WHERE symbol = %s
                        LIMIT 1
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (normalized,),
                )
                row = cur.fetchone()
        if not row:
            return None

        return {
            "symbol": row[0],
            "ts_code": row[1],
            "main_business": row[2],
            "product_type": row[3],
            "product_name": row[4],
            "business_scope": row[5],
            "updated_at": row[6],
        }


__all__ = [
    "StockMainBusinessDAO",
    "STOCK_MAIN_BUSINESS_FIELDS",
]
