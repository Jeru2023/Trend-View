"""
DAO for macro M2 money supply year-over-year statistics.
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

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "m2_schema.sql"

M2_FIELDS: Sequence[str] = (
    "period_label",
    "period_date",
    "m0",
    "m0_yoy",
    "m0_mom",
    "m1",
    "m1_yoy",
    "m1_mom",
    "m2",
    "m2_yoy",
    "m2_mom",
)

M2_COLUMN_DEFINITIONS: Sequence[tuple[str, str]] = (
    ("m0", "DOUBLE PRECISION"),
    ("m0_yoy", "DOUBLE PRECISION"),
    ("m0_mom", "DOUBLE PRECISION"),
    ("m1", "DOUBLE PRECISION"),
    ("m1_yoy", "DOUBLE PRECISION"),
    ("m1_mom", "DOUBLE PRECISION"),
    ("m2", "DOUBLE PRECISION"),
    ("m2_yoy", "DOUBLE PRECISION"),
    ("m2_mom", "DOUBLE PRECISION"),
    ("updated_at", "TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()"),
)


class MacroM2DAO(PostgresDAOBase):
    """Persistence helper for M2 money supply data."""

    _conflict_keys: Sequence[str] = ("period_label",)

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "m2_table", "macro_m2")
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
            cur.execute(
                """
                SELECT tc.constraint_name,
                       ARRAY_AGG(kc.column_name ORDER BY kc.ordinal_position)
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kc
                  ON kc.table_schema = tc.table_schema
                 AND kc.table_name = tc.table_name
                 AND kc.constraint_name = tc.constraint_name
                WHERE tc.table_schema = %s
                  AND tc.table_name = %s
                  AND tc.constraint_type = 'PRIMARY KEY'
                GROUP BY tc.constraint_name
                """,
                (self.config.schema, self._table_name),
            )
            pk_info = cur.fetchone()
            expected_pk_columns = ["period_label"]
            if pk_info is None:
                cur.execute(
                    sql.SQL("ALTER TABLE {schema}.{table} ADD CONSTRAINT {constraint} PRIMARY KEY (period_label)").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                        constraint=sql.Identifier(f"{self._table_name}_period_label_pkey"),
                    )
                )
            else:
                pk_name, columns = pk_info
                if list(columns or []) != expected_pk_columns:
                    cur.execute(
                        sql.SQL("ALTER TABLE {schema}.{table} DROP CONSTRAINT {constraint}").format(
                            schema=sql.Identifier(self.config.schema),
                            table=sql.Identifier(self._table_name),
                            constraint=sql.Identifier(pk_name),
                        )
                    )
                    cur.execute(
                        sql.SQL(
                            "ALTER TABLE {schema}.{table} ADD CONSTRAINT {constraint} PRIMARY KEY (period_label)"
                        ).format(
                            schema=sql.Identifier(self.config.schema),
                            table=sql.Identifier(self._table_name),
                            constraint=sql.Identifier(f"{self._table_name}_period_label_pkey"),
                        )
                    )

            for column_name, column_definition in M2_COLUMN_DEFINITIONS:
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS {column} {definition}"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                        column=sql.Identifier(column_name),
                        definition=sql.SQL(column_definition),
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
                    columns=M2_FIELDS,
                    conflict_keys=self._conflict_keys,
                    date_columns=("period_date",),
                )

        self.ensure_table(conn)
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=M2_FIELDS,
            conflict_keys=self._conflict_keys,
            date_columns=("period_date",),
        )

    def purge_empty_rows(self, *, conn: Optional[PGConnection] = None) -> int:
        """
        Remove legacy rows that lack all money-supply metrics (historical AkShare records).
        """
        delete_sql = sql.SQL(
            """
            DELETE FROM {schema}.{table}
            WHERE COALESCE(
                m0, m0_yoy, m0_mom,
                m1, m1_yoy, m1_mom,
                m2, m2_yoy, m2_mom
            ) IS NULL
            """
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                with owned_conn.cursor() as cur:
                    cur.execute(delete_sql)
                    return cur.rowcount or 0

        self.ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute(delete_sql)
            return cur.rowcount or 0

    def list_entries(self, *, limit: int = 200, offset: int = 0) -> Dict[str, object]:
        query = sql.SQL(
            """
            SELECT period_date,
                   period_label,
                   m0,
                   m0_yoy,
                   m0_mom,
                   m1,
                   m1_yoy,
                   m1_mom,
                   m2,
                   m2_yoy,
                   m2_mom,
                   updated_at
            FROM {schema}.{table}
            ORDER BY period_date DESC, period_label DESC
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
        for row in rows:
            (
                period_date,
                period_label,
                m0,
                m0_yoy,
                m0_mom,
                m1,
                m1_yoy,
                m1_mom,
                m2,
                m2_yoy,
                m2_mom,
                updated_at,
            ) = row
            items.append(
                {
                    "period_date": period_date,
                    "period_label": period_label,
                    "m0": m0,
                    "m0_yoy": m0_yoy,
                    "m0_mom": m0_mom,
                    "m1": m1,
                    "m1_yoy": m1_yoy,
                    "m1_mom": m1_mom,
                    "m2": m2,
                    "m2_yoy": m2_yoy,
                    "m2_mom": m2_mom,
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


__all__ = ["MacroM2DAO", "M2_FIELDS"]
