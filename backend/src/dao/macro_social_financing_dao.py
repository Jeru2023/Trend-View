"""
DAO for macro social financing incremental statistics.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "social_financing_schema.sql"

SOCIAL_FINANCING_FIELDS: Sequence[str] = (
    "period_date",
    "period_label",
    "total_financing",
    "renminbi_loans",
    "entrusted_and_fx_loans",
    "entrusted_loans",
    "trust_loans",
    "undiscounted_bankers_acceptance",
    "corporate_bonds",
    "domestic_equity_financing",
)


class MacroSocialFinancingDAO(PostgresDAOBase):
    """Persistence helper for social financing time series."""

    _conflict_keys: Sequence[str] = ("period_date",)

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "social_financing_table", "macro_social_financing")
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
                sql.SQL("CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (period_date DESC)").format(
                    index=sql.Identifier(f"{self._table_name}_period_date_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (updated_at DESC)").format(
                    index=sql.Identifier(f"{self._table_name}_updated_at_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )

    def upsert(self, dataframe: pd.DataFrame, *, conn: Optional[PGConnection] = None) -> int:
        if dataframe.empty:
            return 0

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._upsert_dataframe(
                    owned_conn,
                    schema=self.config.schema,
                    table=self._table_name,
                    dataframe=dataframe,
                    columns=SOCIAL_FINANCING_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=("period_date",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=SOCIAL_FINANCING_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=("period_date",),
        )

    def list_entries(self, *, limit: int = 200, offset: int = 0) -> Dict[str, object]:
        query = sql.SQL(
            """
            SELECT period_date,
                   period_label,
                   total_financing,
                   renminbi_loans,
                   entrusted_and_fx_loans,
                   entrusted_loans,
                   trust_loans,
                   undiscounted_bankers_acceptance,
                   corporate_bonds,
                   domestic_equity_financing,
                   updated_at
            FROM {schema}.{table}
            ORDER BY period_date DESC
            LIMIT %s OFFSET %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        count_query = sql.SQL(
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
                cur.execute(count_query)
                count_row = cur.fetchone()
                total = count_row[0] if count_row else 0
                last_updated = count_row[1] if count_row else None

                cur.execute(query, (limit, offset))
                rows = cur.fetchall()

        items: List[Dict[str, object]] = []
        for (
            period_date,
            period_label,
            total_financing,
            renminbi_loans,
            entrusted_and_fx_loans,
            entrusted_loans,
            trust_loans,
            undiscounted_bankers_acceptance,
            corporate_bonds,
            domestic_equity_financing,
            updated_at,
        ) in rows:
            items.append(
                {
                    "period_date": period_date,
                    "period_label": period_label,
                    "total_financing": total_financing,
                    "renminbi_loans": renminbi_loans,
                    "entrusted_and_fx_loans": entrusted_and_fx_loans,
                    "entrusted_loans": entrusted_loans,
                    "trust_loans": trust_loans,
                    "undiscounted_bankers_acceptance": undiscounted_bankers_acceptance,
                    "corporate_bonds": corporate_bonds,
                    "domestic_equity_financing": domestic_equity_financing,
                    "updated_at": updated_at,
                }
            )

        return {"total": total or 0, "items": items, "updated_at": last_updated}

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
            "updated_at": last_updated,
        }


__all__ = ["MacroSocialFinancingDAO", "SOCIAL_FINANCING_FIELDS"]
