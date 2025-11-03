"""
DAO for global finance flash headlines sourced from Eastmoney.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from zoneinfo import ZoneInfo

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "global_flash_schema.sql"

GLOBAL_FLASH_FIELDS: Sequence[str] = (
    "url",
    "title",
    "summary",
    "published_at",
    "if_extract",
    "extract_checked_at",
    "extract_reason",
    "subject_level",
    "impact_scope",
    "event_type",
    "time_sensitivity",
    "quant_signal",
    "impact_levels",
    "impact_markets",
    "impact_industries",
    "impact_sectors",
    "impact_themes",
    "impact_stocks",
)


class GlobalFlashDAO(PostgresDAOBase):
    """Persistence helper for Eastmoney global finance flash data."""

    _conflict_keys: Sequence[str] = ("url",)

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "global_flash_table", "global_flash")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )
        index_name = f"{self._table_name}_published_at_idx"
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {index} "
                    "ON {schema}.{table} (published_at DESC)"
                ).format(
                    index=sql.Identifier(index_name),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {index} "
                    "ON {schema}.{table} (if_extract)"
                ).format(
                    index=sql.Identifier(f"{self._table_name}_if_extract_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS if_extract BOOLEAN"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS extract_checked_at TIMESTAMP WITHOUT TIME ZONE"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS extract_reason TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS subject_level TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS impact_scope TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS event_type TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS time_sensitivity TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS quant_signal TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS impact_levels TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS impact_markets TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS impact_industries TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS impact_sectors TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS impact_themes TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS impact_stocks TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )

    def upsert(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            available_columns = [column for column in GLOBAL_FLASH_FIELDS if column in dataframe.columns]
            affected = self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe,
                columns=available_columns,
                conflict_keys=self._conflict_keys,
                date_columns=(),
            )
        return affected

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
        return {
            "count": count or 0,
            "updated_at": self._convert_to_local(last_updated),
        }

    def latest_published_at(self) -> Optional[datetime]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT MAX(published_at) FROM {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                (latest,) = cur.fetchone()
        return latest

    def list_recent(self, *, limit: int = 200) -> List[Dict[str, object]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT url, title, summary, published_at, if_extract, extract_checked_at, extract_reason, subject_level, impact_scope, event_type, time_sensitivity, quant_signal, impact_levels, impact_markets, impact_industries, impact_sectors, impact_themes, impact_stocks "
                        "FROM {schema}.{table} "
                        "ORDER BY published_at DESC NULLS LAST, url ASC "
                        "LIMIT %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (limit,),
                )
                rows = cur.fetchall()
        results: List[Dict[str, object]] = []
        for (
            url,
            title,
            summary,
            published_at,
            impact_flag,
            checked_at,
            reason,
            subject_level,
            impact_scope,
            event_type,
            time_sensitivity,
            quant_signal,
            impact_levels,
            impact_markets,
            impact_industries,
            impact_sectors,
            impact_themes,
            impact_stocks,
        ) in rows:
            results.append(
                {
                    "url": url,
                    "title": title,
                    "summary": summary,
                    "published_at": published_at,
                    "if_extract": impact_flag,
                    "extract_checked_at": checked_at,
                    "extract_reason": reason,
                    "subject_level": subject_level,
                    "impact_scope": impact_scope,
                    "event_type": event_type,
                    "time_sensitivity": time_sensitivity,
                    "quant_signal": quant_signal,
                    "impact_levels": self._decode_text_array(impact_levels),
                    "impact_markets": self._decode_text_array(impact_markets),
                    "impact_industries": self._decode_text_array(impact_industries),
                    "impact_sectors": self._decode_text_array(impact_sectors),
                    "impact_themes": self._decode_text_array(impact_themes),
                    "impact_stocks": self._decode_text_array(impact_stocks),
                }
            )
        return results

    def list_unclassified(self, *, limit: int = 20) -> List[Dict[str, object]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT url, title, summary, published_at "
                        "FROM {schema}.{table} "
                        "WHERE if_extract IS NULL "
                        "ORDER BY published_at DESC NULLS LAST, url ASC "
                        "LIMIT %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (limit,),
                )
                rows = cur.fetchall()
        results: List[Dict[str, object]] = []
        for url, title, summary, published_at in rows:
            results.append(
                {
                    "url": url,
                    "title": title,
                    "summary": summary,
                    "published_at": published_at,
                }
            )
        return results

    @staticmethod
    def _decode_text_array(value: Optional[str]) -> List[str]:
        if not value:
            return []
        try:
            data = json.loads(value)
            if isinstance(data, list):
                return [str(item).strip() for item in data if str(item).strip()]
            if isinstance(data, str) and data.strip():
                return [part.strip() for part in data.split(",") if part.strip()]
        except (json.JSONDecodeError, TypeError):
            return [part.strip() for part in str(value).split(",") if part.strip()]
        return []

    @staticmethod
    def _convert_to_local(value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                try:
                    value = value.replace(tzinfo=ZoneInfo("UTC"))
                except Exception:
                    return value
            return value.astimezone(ZoneInfo("Asia/Shanghai"))
        return value

    def update_classification(self, entries: List[Dict[str, object]]) -> int:
        if not entries:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            values: List[tuple] = []
            for entry in entries:
                url = entry.get("url")
                if not url:
                    continue
                values.append(
                    (
                        url,
                        entry.get("if_extract"),
                        entry.get("extract_checked_at"),
                        entry.get("extract_reason"),
                        entry.get("subject_level"),
                        entry.get("impact_scope"),
                        entry.get("event_type"),
                        entry.get("time_sensitivity"),
                        entry.get("quant_signal"),
                    )
                )

            if not values:
                return 0

            from psycopg2.extras import execute_values

            columns_sql = sql.SQL(
                "UPDATE {schema}.{table} AS t SET "
                "if_extract = data.if_extract, "
                "extract_checked_at = data.extract_checked_at, "
                "extract_reason = data.extract_reason, "
                "subject_level = data.subject_level, "
                "impact_scope = data.impact_scope, "
                "event_type = data.event_type, "
                "time_sensitivity = data.time_sensitivity, "
                "quant_signal = data.quant_signal, "
                "updated_at = CURRENT_TIMESTAMP "
                "FROM (VALUES %s) AS data(" \
                "url, if_extract, extract_checked_at, extract_reason, subject_level, impact_scope, event_type, time_sensitivity, quant_signal" \
                ") WHERE t.url = data.url"
            ).format(
                schema=sql.Identifier(self.config.schema),
                table=sql.Identifier(self._table_name),
            )

            with conn.cursor() as cur:
                execute_values(cur, columns_sql.as_string(conn), values)
                affected = cur.rowcount
        return affected


__all__ = ["GlobalFlashDAO", "GLOBAL_FLASH_FIELDS"]
