"""DAO for concept fund flow data sourced from AkShare."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence

import math
import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "concept_fund_flow_schema.sql"

CONCEPT_FUND_FLOW_FIELDS: Sequence[str] = (
    "symbol",
    "concept",
    "rank",
    "concept_index",
    "price_change_percent",
    "stage_change_percent",
    "inflow",
    "outflow",
    "net_amount",
    "company_count",
    "leading_stock",
    "leading_stock_change_percent",
    "current_price",
)


class ConceptFundFlowDAO(PostgresDAOBase):
    """Persistence helper for concept fund flow data."""

    _conflict_keys: Sequence[str] = ("symbol", "concept")

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "concept_fund_flow_table", "concept_fund_flow")
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

        deduped = dataframe.drop_duplicates(subset=self._conflict_keys, keep="last")

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._upsert_dataframe(
                    owned_conn,
                    schema=self.config.schema,
                    table=self._table_name,
                    dataframe=deduped,
                    columns=CONCEPT_FUND_FLOW_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=(),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=deduped,
            columns=CONCEPT_FUND_FLOW_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=(),
        )

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

    def list_entries(
        self,
        *,
        symbol: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, object]:
        conditions: List[sql.SQL] = []
        params: List[object] = []

        if symbol:
            conditions.append(sql.SQL("c.symbol = %s"))
            params.append(symbol)

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        base_query = sql.SQL(
            """
            SELECT c.symbol,
                   c.concept,
                   c.rank,
                   c.concept_index,
                   c.price_change_percent,
                   c.stage_change_percent,
                   c.inflow,
                   c.outflow,
                   c.net_amount,
                   c.company_count,
                   c.leading_stock,
                   c.leading_stock_change_percent,
                   c.current_price,
                   c.updated_at
            FROM {schema}.{table} AS c
            {where_clause}
            ORDER BY c.symbol ASC, c.rank ASC NULLS LAST, c.concept ASC
            LIMIT %s OFFSET %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        count_query = sql.SQL(
            "SELECT COUNT(*) FROM {schema}.{table} AS c {where_clause}"
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
            "symbol",
            "concept",
            "rank",
            "concept_index",
            "price_change_percent",
            "stage_change_percent",
            "inflow",
            "outflow",
            "net_amount",
            "company_count",
            "leading_stock",
            "leading_stock_change_percent",
            "current_price",
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

        def _to_int(value: object) -> Optional[int]:
            if value is None:
                return None
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return None

        items: List[dict[str, object]] = []
        for row in rows:
            record = dict(zip(columns, row))
            for key in (
                "concept_index",
                "price_change_percent",
                "stage_change_percent",
                "inflow",
                "outflow",
                "net_amount",
                "leading_stock_change_percent",
                "current_price",
            ):
                record[key] = _to_float(record.get(key))
            record["company_count"] = _to_int(record.get("company_count"))
            record["rank"] = _to_int(record.get("rank"))
            items.append(record)

        return {"total": int(total), "items": items}

    def truncate(self) -> None:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
            conn.commit()


__all__ = [
    "CONCEPT_FUND_FLOW_FIELDS",
    "ConceptFundFlowDAO",
]
