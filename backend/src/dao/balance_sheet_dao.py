"""DAO for balance sheet statements."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "balance_sheet_schema.sql"

BALANCE_SHEET_COLUMNS: Sequence[str] = (
    "ts_code",
    "ann_date",
    "end_date",
    "money_cap",
    "accounts_receiv",
    "inventories",
    "fix_assets",
    "total_cur_assets",
    "total_nca",
    "total_assets",
    "st_borr",
    "lt_borr",
    "acct_payable",
    "total_cur_liab",
    "total_ncl",
    "total_liab",
    "total_share",
    "cap_rese",
    "surplus_rese",
    "undistr_porfit",
    "total_hldr_eqy_exc_min_int",
    "total_liab_hldr_eqy",
)


class BalanceSheetDAO(PostgresDAOBase):
    """Persistence helper for balance sheet statements."""

    _conflict_keys: Sequence[str] = ("ts_code", "end_date", "ann_date")
    _date_columns: Sequence[str] = ("ann_date", "end_date")

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "balance_sheet_table", "balance_sheet_statements")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            index_ts_code=f"{self._table_name}_ts_code_idx",
        )

    def upsert(self, dataframe: pd.DataFrame, *, conn=None) -> int:
        if dataframe is None or dataframe.empty:
            return 0
        normalized = self._normalize_dataframe(dataframe, self._date_columns)
        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._write_dataframe(owned_conn, normalized)
        self.ensure_table(conn)
        return self._write_dataframe(conn, normalized)

    def _write_dataframe(self, conn, dataframe: pd.DataFrame) -> int:
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=BALANCE_SHEET_COLUMNS,
            conflict_keys=self._conflict_keys,
            date_columns=self._date_columns,
        )

    def list_entries(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        keyword: Optional[str] = None,
        ts_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))

        where_clauses = []
        params: list[object] = []

        if keyword:
            where_clauses.append(
                sql.SQL("(bs.ts_code ILIKE %s OR sb.name ILIKE %s OR sb.symbol ILIKE %s)")
            )
            like_value = f"%{keyword}%"
            params.extend([like_value, like_value, like_value])

        if ts_code:
            where_clauses.append(sql.SQL("bs.ts_code = %s"))
            params.append(ts_code)

        where_sql = sql.SQL("")
        if where_clauses:
            where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_clauses)

        base_query = sql.SQL(
            """
            FROM {schema}.{table} AS bs
            LEFT JOIN {schema}.{stock_table} AS sb
              ON sb.ts_code = bs.ts_code
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            stock_table=sql.Identifier(self.config.stock_table),
        )

        count_query = sql.SQL("SELECT COUNT(*) ").format() + base_query + where_sql
        data_query = (
            sql.SQL(
                """
                SELECT bs.ts_code,
                       bs.ann_date,
                       bs.end_date,
                       bs.money_cap,
                       bs.accounts_receiv,
                       bs.inventories,
                       bs.fix_assets,
                       bs.total_cur_assets,
                       bs.total_nca,
                       bs.total_assets,
                       bs.st_borr,
                       bs.lt_borr,
                       bs.acct_payable,
                       bs.total_cur_liab,
                       bs.total_ncl,
                       bs.total_liab,
                       bs.total_share,
                       bs.cap_rese,
                       bs.surplus_rese,
                       bs.undistr_porfit,
                       bs.total_hldr_eqy_exc_min_int,
                       bs.total_liab_hldr_eqy,
                       bs.created_at,
                       bs.updated_at,
                       sb.name,
                       sb.symbol,
                       sb.industry,
                       sb.market
                """
            )
            + base_query
            + where_sql
            + sql.SQL(
                " ORDER BY bs.end_date DESC NULLS LAST, bs.ann_date DESC NULLS LAST, bs.ts_code LIMIT %s OFFSET %s"
            )
        )

        params_with_pagination = params + [limit, offset]

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query, params)
                total = cur.fetchone()[0] or 0
                cur.execute(data_query, params_with_pagination)
                rows = cur.fetchall()

        columns = [
            "ts_code",
            "ann_date",
            "end_date",
            "money_cap",
            "accounts_receiv",
            "inventories",
            "fix_assets",
            "total_cur_assets",
            "total_nca",
            "total_assets",
            "st_borr",
            "lt_borr",
            "acct_payable",
            "total_cur_liab",
            "total_ncl",
            "total_liab",
            "total_share",
            "cap_rese",
            "surplus_rese",
            "undistr_porfit",
            "total_hldr_eqy_exc_min_int",
            "total_liab_hldr_eqy",
            "created_at",
            "updated_at",
            "name",
            "symbol",
            "industry",
            "market",
        ]

        items = [dict(zip(columns, row)) for row in rows]
        return {"total": int(total), "items": items}

    def stats(self) -> Dict[str, Any]:
        query = sql.SQL(
            """
            SELECT COUNT(*), MAX(updated_at)
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
                count, updated_at = cur.fetchone()
        return {
            "count": int(count or 0),
            "updated_at": updated_at,
        }


__all__ = ["BalanceSheetDAO", "BALANCE_SHEET_COLUMNS"]
