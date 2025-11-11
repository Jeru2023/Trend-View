"""DAO for big deal fund flow (large trade tracking) data sourced from AkShare."""

from __future__ import annotations

from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import List, Optional, Sequence

import math
import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import execute_batch

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "big_deal_fund_flow_schema.sql"

BIG_DEAL_FUND_FLOW_FIELDS: Sequence[str] = (
    "trade_time",
    "stock_code",
    "stock_name",
    "trade_price",
    "trade_volume",
    "trade_amount",
    "trade_side",
    "price_change_percent",
    "price_change",
)


class BigDealFundFlowDAO(PostgresDAOBase):
    """Persistence helper for big deal fund flow data."""

    _conflict_keys: Sequence[str] = ("trade_time", "stock_code", "trade_side", "trade_volume", "trade_amount")

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "big_deal_fund_flow_table", "big_deal_fund_flow")
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

        normalized = self._normalize_dataframe(deduped, ("trade_time",))

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._upsert_rows(owned_conn, normalized)

        self.ensure_table(conn)
        return self._upsert_rows(conn, normalized)

    def _upsert_rows(self, conn: PGConnection, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        columns = BIG_DEAL_FUND_FLOW_FIELDS
        conflict_sql = sql.SQL(", ").join(sql.Identifier(col) for col in self._conflict_keys)
        column_sql = sql.SQL(", ").join(sql.Identifier(col) for col in columns)

        update_sql = sql.SQL(", ").join(
            sql.Composed([sql.Identifier(col), sql.SQL(" = EXCLUDED."), sql.Identifier(col)])
            for col in columns
            if col not in self._conflict_keys
        )

        insert_stmt = sql.SQL(
            """
            INSERT INTO {schema}.{table} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_keys}) DO UPDATE SET
                {updates},
                updated_at = CURRENT_TIMESTAMP
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            columns=column_sql,
            placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            conflict_keys=conflict_sql,
            updates=update_sql if update_sql else sql.SQL(""),
        )

        values = [tuple(row.get(col) for col in columns) for row in dataframe.to_dict(orient="records")]

        with conn.cursor() as cur:
            execute_batch(cur, insert_stmt.as_string(conn), values, page_size=200)

        return len(values)

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
        limit: int = 100,
        offset: int = 0,
        side: Optional[str] = None,
        stock_codes: Optional[Sequence[str]] = None,
    ) -> dict[str, object]:
        conditions: List[sql.SQL] = []
        params: List[object] = []

        if side:
            conditions.append(sql.SQL("b.trade_side = %s"))
            params.append(side)

        if stock_codes:
            placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in stock_codes)
            conditions.append(sql.SQL("b.stock_code IN ({placeholders})").format(placeholders=placeholders))
            params.extend(stock_codes)

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        base_query = sql.SQL(
            """
            SELECT b.trade_time,
                   b.stock_code,
                   b.stock_name,
                   b.trade_price,
                   b.trade_volume,
                   b.trade_amount,
                   b.trade_side,
                   b.price_change_percent,
                   b.price_change,
                   b.updated_at
            FROM {schema}.{table} AS b
            {where_clause}
            ORDER BY b.trade_time DESC, b.trade_amount DESC
            LIMIT %s OFFSET %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        count_query = sql.SQL(
            "SELECT COUNT(*) FROM {schema}.{table} AS b {where_clause}"
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
            "trade_time",
            "stock_code",
            "stock_name",
            "trade_price",
            "trade_volume",
            "trade_amount",
            "trade_side",
            "price_change_percent",
            "price_change",
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
            record["trade_volume"] = _to_int(record.get("trade_volume"))
            for key in ("trade_price", "trade_amount", "price_change_percent", "price_change"):
                record[key] = _to_float(record.get(key))
            items.append(record)

        return {"total": int(total), "items": items}

    def fetch_buy_amount_map(
        self,
        stock_codes: Sequence[str],
        trade_date: Optional[date] = None,
    ) -> dict[str, float]:
        unique_codes = sorted({(code or "").strip() for code in stock_codes if code})
        if not unique_codes:
            return {}

        day = trade_date or datetime.now().date()
        start = datetime.combine(day, time.min)
        end = start + timedelta(days=1)

        code_placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in unique_codes)
        query = sql.SQL(
            """
            SELECT stock_code,
                   COALESCE(SUM(CASE WHEN trade_side LIKE %s THEN trade_amount ELSE 0 END), 0) AS buy_amount,
                   COALESCE(SUM(CASE WHEN trade_side LIKE %s THEN trade_amount ELSE 0 END), 0) AS sell_amount
            FROM {schema}.{table}
            WHERE stock_code IN ({codes})
              AND trade_time >= %s
              AND trade_time < %s
            GROUP BY stock_code
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            codes=code_placeholders,
        )

        params: List[object] = ["%买%", "%卖%"]
        params.extend(unique_codes)
        params.extend([start, end])

        results: dict[str, float] = {}
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, params)
                for stock_code, buy_amount, sell_amount in cur.fetchall():
                    try:
                        buy_numeric = float(buy_amount or 0.0)
                    except (TypeError, ValueError):
                        buy_numeric = 0.0
                    try:
                        sell_numeric = float(sell_amount or 0.0)
                    except (TypeError, ValueError):
                        sell_numeric = 0.0
                    results[str(stock_code).strip()] = buy_numeric - sell_numeric
        return results


__all__ = [
    "BIG_DEAL_FUND_FLOW_FIELDS",
    "BigDealFundFlowDAO",
]
