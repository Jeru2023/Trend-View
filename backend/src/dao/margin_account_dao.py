"""
DAO for margin (financing & securities lending) account statistics.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "margin_account_schema.sql"

MARGIN_ACCOUNT_FIELDS: Sequence[str] = (
    "trade_date",
    "financing_balance",
    "securities_lending_balance",
    "financing_purchase_amount",
    "securities_lending_sell_amount",
    "securities_company_count",
    "business_department_count",
    "individual_investor_count",
    "institutional_investor_count",
    "participating_investor_count",
    "liability_investor_count",
    "collateral_value",
    "average_collateral_ratio",
)

NUMERIC_FIELDS: Sequence[str] = tuple(
    field
    for field in MARGIN_ACCOUNT_FIELDS
    if field not in {"trade_date"}
)


class MarginAccountDAO(PostgresDAOBase):
    """Persistence helper for margin account statistics."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "margin_account_table", "margin_account")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            trade_date_idx=f"{self._table_name}_trade_date_idx",
        )

    def upsert(self, dataframe: pd.DataFrame, *, conn: Optional[PGConnection] = None) -> int:
        if dataframe.empty:
            return 0

        deduped = dataframe.drop_duplicates(subset=("trade_date",), keep="last")
        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._upsert_dataframe(
                    owned_conn,
                    schema=self.config.schema,
                    table=self._table_name,
                    dataframe=deduped,
                    columns=MARGIN_ACCOUNT_FIELDS,
                    conflict_keys=("trade_date",),
                    date_columns=("trade_date",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=deduped,
            columns=MARGIN_ACCOUNT_FIELDS,
            conflict_keys=("trade_date",),
            date_columns=("trade_date",),
        )

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

    def list_entries(
        self,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> Dict[str, object]:
        conditions: List[sql.SQL] = []
        params: List[object] = []

        if start_date:
            conditions.append(sql.SQL("m.trade_date >= %s"))
            params.append(start_date)

        if end_date:
            conditions.append(sql.SQL("m.trade_date <= %s"))
            params.append(end_date)

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        base_query = sql.SQL(
            """
            SELECT
                m.trade_date,
                m.financing_balance,
                m.securities_lending_balance,
                m.financing_purchase_amount,
                m.securities_lending_sell_amount,
                m.securities_company_count,
                m.business_department_count,
                m.individual_investor_count,
                m.institutional_investor_count,
                m.participating_investor_count,
                m.liability_investor_count,
                m.collateral_value,
                m.average_collateral_ratio,
                m.updated_at
            FROM {schema}.{table} AS m
            {where_clause}
            ORDER BY m.trade_date DESC
            LIMIT %s OFFSET %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        count_query = sql.SQL(
            "SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table} AS m {where_clause}"
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            where_clause=where_clause,
        )

        query_params = params + [limit, offset]
        available_years: List[int] = []

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query, params)
                total_row = cur.fetchone()
                total = total_row[0] if total_row else 0
                last_updated = total_row[1] if total_row else None
                cur.execute(base_query, query_params)
                rows = cur.fetchall()

            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT DISTINCT EXTRACT(YEAR FROM trade_date)::int
                        FROM {schema}.{table}
                        ORDER BY 1 DESC
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                available_years = [row[0] for row in cur.fetchall()]

        columns = [
            "trade_date",
            "financing_balance",
            "securities_lending_balance",
            "financing_purchase_amount",
            "securities_lending_sell_amount",
            "securities_company_count",
            "business_department_count",
            "individual_investor_count",
            "institutional_investor_count",
            "participating_investor_count",
            "liability_investor_count",
            "collateral_value",
            "average_collateral_ratio",
            "updated_at",
        ]

        items: List[Dict[str, object]] = []
        for row in rows:
            payload = dict(zip(columns, row))
            for field in NUMERIC_FIELDS:
                value = payload.get(field)
                if isinstance(value, Decimal):
                    payload[field] = float(value) if value.is_finite() else None
            items.append(payload)

        return {
            "total": total or 0,
            "items": items,
            "updated_at": last_updated,
            "available_years": available_years,
        }


__all__ = ["MarginAccountDAO", "MARGIN_ACCOUNT_FIELDS"]
