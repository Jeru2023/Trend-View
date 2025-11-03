"""DAO for news insights and LLM results."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "news_insights_schema.sql"

NEWS_INSIGHT_FIELDS: tuple[str, ...] = (
    "article_id",
    "is_relevant",
    "relevance_confidence",
    "relevance_reason",
    "relevance_checked_at",
    "impact_levels",
    "impact_markets",
    "impact_industries",
    "impact_sectors",
    "impact_themes",
    "impact_stocks",
    "impact_summary",
    "impact_analysis",
    "impact_confidence",
    "impact_checked_at",
    "extra_metadata",
)

DATE_COLUMNS: tuple[str, ...] = (
    "relevance_checked_at",
    "impact_checked_at",
)


class NewsInsightDAO(PostgresDAOBase):
    """Persistence helper for LLM-driven news insights."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "news_insights_table", "news_insights")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            table_relevance_idx=f"{self._table_name}_relevance_idx",
        )

    def upsert(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            available_columns = [column for column in NEWS_INSIGHT_FIELDS if column in dataframe.columns]
            affected = self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe.loc[:, available_columns],
                columns=available_columns,
                conflict_keys=("article_id",),
                date_columns=DATE_COLUMNS,
            )
        return affected

    def fetch_many(self, article_ids: Iterable[str]) -> Dict[str, Dict[str, object]]:
        ids = [article_id for article_id in article_ids]
        if not ids:
            return {}
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT article_id, is_relevant, relevance_confidence, relevance_reason, relevance_checked_at, "
                        "impact_levels, impact_markets, impact_industries, impact_sectors, impact_themes, impact_stocks, impact_summary, impact_analysis, impact_confidence, impact_checked_at, extra_metadata "
                        "FROM {schema}.{table} WHERE article_id = ANY(%s)"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (ids,),
                )
                rows = cur.fetchall()
        result: Dict[str, Dict[str, object]] = {}
        for row in rows:
            (
                article_id,
                is_relevant,
                relevance_confidence,
                relevance_reason,
                relevance_checked_at,
                impact_levels,
                impact_markets,
                impact_industries,
                impact_sectors,
                impact_themes,
                impact_stocks,
                impact_summary,
                impact_analysis,
                impact_confidence,
                impact_checked_at,
                extra_metadata,
            ) = row
            result[article_id] = {
                "is_relevant": is_relevant,
                "relevance_confidence": relevance_confidence,
                "relevance_reason": relevance_reason,
                "relevance_checked_at": relevance_checked_at,
                "impact_levels": self._decode_array(impact_levels),
                "impact_markets": self._decode_array(impact_markets),
                "impact_industries": self._decode_array(impact_industries),
                "impact_sectors": self._decode_array(impact_sectors),
                "impact_themes": self._decode_array(impact_themes),
                "impact_stocks": self._decode_array(impact_stocks),
                "impact_summary": impact_summary,
                "impact_analysis": impact_analysis,
                "impact_confidence": impact_confidence,
                "impact_checked_at": impact_checked_at,
                "extra_metadata": extra_metadata,
            }
        return result

    @staticmethod
    def _decode_array(value: Optional[str]) -> List[str]:
        if not value:
            return []
        try:
            data = json.loads(value)
            if isinstance(data, list):
                return [str(item).strip() for item in data if str(item).strip()]
        except (json.JSONDecodeError, TypeError):
            pass
        return [part.strip() for part in str(value).split(",") if part.strip()]


__all__ = ["NewsInsightDAO"]
