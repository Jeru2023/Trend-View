"""DAO for stock volume-price reasoning snapshots."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import Json

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "stock_volume_price_reasoning_schema.sql"


class StockVolumePriceReasoningDAO(PostgresDAOBase):
    """Persistence helper for stock volume-price reasoning records."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "stock_volume_price_reasoning_table", "stock_volume_price_reasoning")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            index_name=f"{self._table_name}_stock_idx",
        )

    def insert_snapshot(
        self,
        *,
        stock_code: str,
        stock_name: Optional[str],
        lookback_days: int,
        summary_json: Dict[str, Any],
        raw_text: str,
        model: Optional[str],
        generated_at: datetime,
    ) -> int:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.{table} (
                            stock_code,
                            stock_name,
                            lookback_days,
                            summary_json,
                            raw_text,
                            model,
                            generated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (
                        stock_code,
                        stock_name,
                        lookback_days,
                        Json(summary_json),
                        raw_text,
                        model,
                        generated_at,
                    ),
                )
                new_id = cur.fetchone()[0]
            conn.commit()
        return int(new_id)

    def fetch_latest(self, stock_code: str) -> Optional[Dict[str, Any]]:
        if not stock_code:
            return None
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT id, stock_code, stock_name, lookback_days,
                               summary_json, raw_text, model, generated_at
                        FROM {schema}.{table}
                        WHERE stock_code = %s
                        ORDER BY generated_at DESC
                        LIMIT 1
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (stock_code,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return self._to_dict(row)

    def list_history(
        self,
        stock_code: str,
        *,
        limit: int = 10,
        offset: int = 0,
    ) -> Dict[str, Any]:
        if not stock_code:
            return {"total": 0, "items": []}
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {schema}.{table} WHERE stock_code = %s").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (stock_code,),
                )
                total = cur.fetchone()[0] or 0
                cur.execute(
                    sql.SQL(
                        """
                        SELECT id, stock_code, stock_name, lookback_days,
                               summary_json, raw_text, model, generated_at
                        FROM {schema}.{table}
                        WHERE stock_code = %s
                        ORDER BY generated_at DESC
                        LIMIT %s OFFSET %s
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (stock_code, limit, offset),
                )
                rows = cur.fetchall()
        return {"total": int(total), "items": [self._to_dict(row) for row in rows]}

    def _to_dict(self, row: Any) -> Dict[str, Any]:
        (
            record_id,
            stock_code,
            stock_name,
            lookback_days,
            summary_json,
            raw_text,
            model,
            generated_at,
        ) = row
        return {
            "id": record_id,
            "code": stock_code,
            "name": stock_name,
            "lookbackDays": lookback_days,
            "summary": summary_json,
            "rawText": raw_text,
            "model": model,
            "generatedAt": generated_at,
        }


__all__ = ["StockVolumePriceReasoningDAO"]
