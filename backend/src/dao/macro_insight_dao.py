"""
DAO for storing macro insight snapshots generated from consolidated macro datasets.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "macro_insight_schema.sql"


class MacroInsightDAO(PostgresDAOBase):
    """Persistence helper for macro insight snapshots."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "macro_insight_table", "macro_insights")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (generated_at DESC)").format(
                    index=sql.Identifier(f"{self._table_name}_generated_at_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )

    def upsert_snapshot(
        self,
        *,
        snapshot_date: date,
        generated_at: datetime,
        datasets: Dict[str, Any],
        summary_json: Optional[Dict[str, Any]],
        raw_response: Optional[str],
        model: Optional[str],
    ) -> None:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.{table} (
                            snapshot_date,
                            generated_at,
                            datasets,
                            summary_json,
                            raw_response,
                            model
                        )
                        VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s)
                        ON CONFLICT (snapshot_date)
                        DO UPDATE
                        SET generated_at = EXCLUDED.generated_at,
                            datasets = EXCLUDED.datasets,
                            summary_json = EXCLUDED.summary_json,
                            raw_response = EXCLUDED.raw_response,
                            model = EXCLUDED.model,
                            updated_at = NOW()
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (
                        snapshot_date,
                        generated_at,
                        json.dumps(datasets, ensure_ascii=False),
                        json.dumps(summary_json, ensure_ascii=False) if summary_json is not None else None,
                        raw_response,
                        model,
                    ),
                )

    def fetch_latest(self) -> Optional[Dict[str, Any]]:
        query = sql.SQL(
            """
            SELECT snapshot_date,
                   generated_at,
                   datasets,
                   summary_json,
                   raw_response,
                   model,
                   created_at,
                   updated_at
            FROM {schema}.{table}
            ORDER BY snapshot_date DESC, generated_at DESC
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

        (
            snapshot_date,
            generated_at,
            datasets,
            summary_json,
            raw_response,
            model,
            created_at,
            updated_at,
        ) = row
        return {
            "snapshot_date": snapshot_date,
            "generated_at": generated_at,
            "datasets": datasets,
            "summary_json": summary_json,
            "raw_response": raw_response,
            "model": model,
            "created_at": created_at,
            "updated_at": updated_at,
        }

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

    def list_snapshots(self, *, limit: int = 6, offset: int = 0) -> List[Dict[str, Any]]:
        query = sql.SQL(
            """
            SELECT snapshot_date,
                   generated_at,
                   summary_json,
                   raw_response,
                   model,
                   created_at,
                   updated_at
            FROM {schema}.{table}
            ORDER BY snapshot_date DESC, generated_at DESC
            LIMIT %s OFFSET %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (limit, offset))
                rows = cur.fetchall()

        results: List[Dict[str, Any]] = []
        for row in rows:
            (
                snapshot_date,
                generated_at,
                summary_json,
                raw_response,
                model,
                created_at,
                updated_at,
            ) = row
            results.append(
                {
                    "snapshot_date": snapshot_date,
                    "generated_at": generated_at,
                    "summary_json": summary_json,
                    "raw_response": raw_response,
                    "model": model,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }
            )
        return results


__all__ = ["MacroInsightDAO"]
