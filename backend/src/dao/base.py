"""
Shared PostgreSQL DAO utilities for the Trend View backend.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from ..config.settings import PostgresSettings


@dataclass(frozen=True)
class PostgresDAOBase:
    """Base class that provides convenience helpers for PostgreSQL operations."""

    config: PostgresSettings

    def connect(self) -> psycopg2.extensions.connection:
        """Create a new database connection using the configured credentials."""
        return psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            dbname=self.config.database,
            user=self.config.user,
            password=self.config.password,
        )

    @staticmethod
    def _normalize_dataframe(
        dataframe: pd.DataFrame,
        date_columns: Sequence[str],
    ) -> pd.DataFrame:
        """Convert date columns and replace NaN/NaT with None for database writes."""
        frame = dataframe.copy()
        for column in date_columns:
            if column in frame.columns:
                frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.date
        return frame.where(pd.notnull(frame), None)

    @staticmethod
    def _execute_schema_template(
        conn: psycopg2.extensions.connection,
        template_sql: str,
        *,
        schema: str,
        table: str,
    ) -> None:
        """Run a schema creation SQL template with formatted identifiers."""
        table_sql = sql.SQL(template_sql).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema_name}").format(
                    schema_name=sql.Identifier(schema)
                )
            )
            cur.execute(table_sql)

    @staticmethod
    def _upsert_dataframe(
        conn: psycopg2.extensions.connection,
        *,
        schema: str,
        table: str,
        dataframe: pd.DataFrame,
        columns: Sequence[str],
        conflict_keys: Sequence[str],
        date_columns: Sequence[str],
    ) -> int:
        """Perform an upsert of the provided DataFrame."""
        normalized = PostgresDAOBase._normalize_dataframe(
            dataframe,
            date_columns=date_columns,
        )
        records = normalized.to_dict(orient="records")
        if not records:
            return 0

        values: list[Iterable[object]] = [
            tuple(record.get(column) for column in columns) for record in records
        ]

        columns_sql = sql.SQL(", ").join(sql.Identifier(col) for col in columns)
        update_sql = sql.SQL(", ").join(
            sql.Composed(
                [sql.Identifier(col), sql.SQL(" = EXCLUDED."), sql.Identifier(col)]
            )
            for col in columns
            if col not in conflict_keys
        )
        conflict_sql = sql.SQL(", ").join(sql.Identifier(key) for key in conflict_keys)

        insert_stmt = sql.SQL(
            """
            INSERT INTO {schema}.{table} ({columns})
            VALUES %s
            ON CONFLICT ({conflict_keys}) DO UPDATE SET
                {updates},
                updated_at = CURRENT_TIMESTAMP
            """
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            columns=columns_sql,
            conflict_keys=conflict_sql,
            updates=update_sql,
        )

        with conn.cursor() as cur:
            execute_values(cur, insert_stmt.as_string(conn), values)

        return len(values)


__all__ = [
    "PostgresDAOBase",
]
