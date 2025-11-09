"""DAO for persisting concept constituent snapshots."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List

from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "concept_constituent_schema.sql"


class ConceptConstituentDAO(PostgresDAOBase):
    """Persist THS concept constituent snapshots."""

    def __init__(self, config: PostgresSettings) -> None:
        super().__init__(config=config)
        self._table = getattr(config, "concept_constituent_table", "concept_constituents")
        self._schema_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def _qualified_table(self) -> sql.Composed:
        return sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table),
        )

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_template,
            schema=self.config.schema,
            table=self._table,
            index_concept=f"{self._table}_concept_idx",
        )

    def replace_entries(self, concept_name: str, concept_code: str, items: Iterable[Dict[str, Any]]) -> None:
        """Upsert entries for the concept (keeps previously stored symbols)."""
        rows = list(items)
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                if not rows:
                    return
                insert = sql.SQL(
                    """
                    INSERT INTO {table} (
                        concept_name,
                        concept_code,
                        symbol,
                        stock_name,
                        rank,
                        last_price,
                        change_percent,
                        change_amount,
                        speed_percent,
                        turnover_rate,
                        volume_ratio,
                        amplitude_percent,
                        turnover_amount,
                        updated_at
                    )
                    VALUES (
                        %(concept_name)s,
                        %(concept_code)s,
                        %(symbol)s,
                        %(stock_name)s,
                        %(rank)s,
                        %(last_price)s,
                        %(change_percent)s,
                        %(change_amount)s,
                        %(speed_percent)s,
                        %(turnover_rate)s,
                        %(volume_ratio)s,
                        %(amplitude_percent)s,
                        %(turnover_amount)s,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (concept_name, symbol) DO UPDATE
                    SET concept_code = EXCLUDED.concept_code,
                        stock_name = EXCLUDED.stock_name,
                        rank = EXCLUDED.rank,
                        last_price = EXCLUDED.last_price,
                        change_percent = EXCLUDED.change_percent,
                        change_amount = EXCLUDED.change_amount,
                        speed_percent = EXCLUDED.speed_percent,
                        turnover_rate = EXCLUDED.turnover_rate,
                        volume_ratio = EXCLUDED.volume_ratio,
                        amplitude_percent = EXCLUDED.amplitude_percent,
                        turnover_amount = EXCLUDED.turnover_amount,
                        updated_at = CURRENT_TIMESTAMP
                    """
                ).format(table=self._qualified_table())
                payload = [
                    {
                        "concept_name": concept_name,
                        "concept_code": concept_code,
                        "symbol": item.get("symbol"),
                        "stock_name": item.get("name"),
                        "rank": item.get("rank"),
                        "last_price": item.get("lastPrice"),
                        "change_percent": item.get("changePercent"),
                        "change_amount": item.get("changeAmount"),
                        "speed_percent": item.get("speedPercent"),
                        "turnover_rate": item.get("turnoverRate"),
                        "volume_ratio": item.get("volumeRatio"),
                        "amplitude_percent": item.get("amplitudePercent"),
                        "turnover_amount": item.get("turnoverAmount"),
                    }
                    for item in rows
                ]
                cur.executemany(insert, payload)
            conn.commit()

    def list_entries(self, concept_name: str) -> List[Dict[str, Any]]:
        """Return the latest stored constituent snapshot for the concept."""
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT concept_name,
                               concept_code,
                               symbol,
                               stock_name,
                               rank,
                               last_price,
                               change_percent,
                               change_amount,
                               speed_percent,
                               turnover_rate,
                               volume_ratio,
                               amplitude_percent,
                               turnover_amount,
                               updated_at
                        FROM {table}
                        WHERE concept_name = %s
                        ORDER BY rank NULLS LAST, symbol
                        """
                    ).format(table=self._qualified_table()),
                    (concept_name,),
                )
                rows = cur.fetchall()
        return [
            {
                "concept": row[0],
                "conceptCode": row[1],
                "symbol": row[2],
                "name": row[3],
                "rank": row[4],
                "lastPrice": row[5],
                "changePercent": row[6],
                "changeAmount": row[7],
                "speedPercent": row[8],
                "turnoverRate": row[9],
                "volumeRatio": row[10],
                "amplitudePercent": row[11],
                "turnoverAmount": row[12],
                "updatedAt": row[13],
            }
            for row in rows
        ]


__all__ = ["ConceptConstituentDAO"]
