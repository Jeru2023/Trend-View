"""DAO for concept index history sourced from Tushare."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence, Set, Tuple

import math
import pandas as pd
from threading import Lock
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "concept_index_history_schema.sql"

CONCEPT_INDEX_HISTORY_FIELDS: Sequence[str] = (
    "ts_code",
    "concept_name",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
)

_TABLE_INIT_LOCK = Lock()
_INITIALISED_TABLES: Set[Tuple[str, str]] = set()


class ConceptIndexHistoryDAO(PostgresDAOBase):
    """Persistence helper for concept index daily history."""

    _conflict_keys: Sequence[str] = ("ts_code", "trade_date")

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "concept_index_history_table", "concept_index_history")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        key = (self.config.schema, self._table_name)
        if key in _INITIALISED_TABLES:
            return
        with _TABLE_INIT_LOCK:
            if key in _INITIALISED_TABLES:
                return
            self._execute_schema_template(
                conn,
                self._schema_sql_template,
                schema=self.config.schema,
                table=self._table_name,
            )
            _INITIALISED_TABLES.add(key)

    def upsert(self, dataframe: pd.DataFrame, *, conn: Optional[PGConnection] = None) -> int:
        if dataframe.empty:
            return 0

        deduped = dataframe.drop_duplicates(subset=self._conflict_keys, keep="last")

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._upsert_dataframe(
                    owned_conn,
                    schema=self.config.schema,
                    table=self._table_name,
                    dataframe=deduped,
                    columns=CONCEPT_INDEX_HISTORY_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=("trade_date",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=deduped,
            columns=CONCEPT_INDEX_HISTORY_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=("trade_date",),
        )

    def list_entries(
        self,
        *,
        ts_code: Optional[str] = None,
        concept_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> dict[str, object]:
        conditions: List[sql.SQL] = []
        params: List[object] = []

        if ts_code:
            conditions.append(sql.SQL("h.ts_code = %s"))
            params.append(ts_code)
        if concept_name:
            conditions.append(sql.SQL("h.concept_name = %s"))
            params.append(concept_name)
        if start_date:
            conditions.append(sql.SQL("h.trade_date >= %s"))
            params.append(start_date)
        if end_date:
            conditions.append(sql.SQL("h.trade_date <= %s"))
            params.append(end_date)

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        limit = max(1, min(int(limit), 1000))
        offset = max(0, int(offset))

        base_query = sql.SQL(
            """
            SELECT h.ts_code,
                   h.concept_name,
                   h.trade_date,
                   h.open,
                   h.high,
                   h.low,
                   h.close,
                   h.pre_close,
                   h.change,
                   h.pct_chg,
                   h.vol,
                   h.amount,
                   h.updated_at
            FROM {schema}.{table} AS h
            {where_clause}
            ORDER BY h.trade_date DESC
            LIMIT %s OFFSET %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        count_query = sql.SQL(
            "SELECT COUNT(*) FROM {schema}.{table} AS h {where_clause}"
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        query_params = params + [limit, offset]

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query, params)
                total = cur.fetchone()[0] or 0
                cur.execute(base_query, query_params)
                rows = cur.fetchall()

        columns = [
            "ts_code",
            "concept_name",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
            "updated_at",
        ]

        def _to_float(value: object) -> Optional[float]:
            if value is None:
                return None
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(numeric):
                return None
            return numeric

        items: List[dict[str, object]] = []
        for row in rows:
            record = dict(zip(columns, row))
            for key in ("open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"):
                record[key] = _to_float(record.get(key))
            items.append(record)

        return {"total": int(total), "items": items}

    def stats(self) -> dict[str, Optional[datetime]]:
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


__all__ = ["CONCEPT_INDEX_HISTORY_FIELDS", "ConceptIndexHistoryDAO"]
