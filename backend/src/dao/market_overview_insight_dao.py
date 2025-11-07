"""DAO for storing market overview reasoning snapshots."""

from __future__ import annotations

from datetime import datetime
from json import dumps, loads
from pathlib import Path
from typing import Any, Dict, Optional

from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "market_overview_insight_schema.sql"


class MarketOverviewInsightDAO(PostgresDAOBase):
    """Persistence helper for market overview reasoning records."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "market_overview_insight_table", "market_overview_insights")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            generated_at_idx=f"{self._table_name}_generated_at_idx",
        )

    def insert_snapshot(
        self,
        *,
        generated_at: datetime,
        summary_json: Dict[str, Any],
        raw_response: str,
        model: Optional[str],
    ) -> None:
        payload = dumps(summary_json, ensure_ascii=False)
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.{table} (generated_at, summary_json, raw_response, model)
                        VALUES (%s, %s::jsonb, %s, %s)
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (generated_at, payload, raw_response, model),
                )

    def fetch_latest(self) -> Optional[Dict[str, Any]]:
        query = sql.SQL(
            """
            SELECT generated_at, summary_json, raw_response, model, created_at
            FROM {schema}.{table}
            ORDER BY generated_at DESC, id DESC
            LIMIT 1
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
        if not row:
            return None

        generated_at, summary_json, raw_response, model, created_at = row
        if isinstance(summary_json, str):
            try:
                parsed_summary = loads(summary_json)
            except Exception:
                parsed_summary = summary_json
        else:
            parsed_summary = summary_json

        return {
            "generated_at": generated_at,
            "summary_json": parsed_summary,
            "raw_response": raw_response,
            "model": model,
            "created_at": created_at,
        }

    def stats(self) -> Dict[str, Any]:
        query = sql.SQL(
            """
            SELECT COUNT(*) AS total, MAX(generated_at) AS latest
            FROM {schema}.{table}
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
        total, latest = row if row else (0, None)
        return {"count": int(total or 0), "latest": latest}


__all__ = ["MarketOverviewInsightDAO"]
