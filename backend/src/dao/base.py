"""
Shared PostgreSQL DAO utilities for the Trend View backend.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Iterator, Sequence

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from ..config.settings import PostgresSettings

DEFAULT_CONNECT_TIMEOUT = 3
DEFAULT_APPLICATION_NAME = "trend_view_backend"


@dataclass(frozen=True)
class PostgresDAOBase:
    """Base class that provides convenience helpers for PostgreSQL operations."""

    config: PostgresSettings

    def _build_connection_kwargs(self) -> dict[str, object]:
        """Compose connection keyword arguments based on settings."""
        timeout = getattr(self.config, "connect_timeout", DEFAULT_CONNECT_TIMEOUT) or DEFAULT_CONNECT_TIMEOUT
        application_name = getattr(self.config, "application_name", DEFAULT_APPLICATION_NAME) or DEFAULT_APPLICATION_NAME

        options_parts: list[str] = []
        statement_timeout = getattr(self.config, "statement_timeout_ms", None)
        if statement_timeout is not None:
            options_parts.append(f"-c statement_timeout={int(statement_timeout)}")

        idle_timeout = getattr(self.config, "idle_in_transaction_session_timeout_ms", None)
        if idle_timeout is not None:
            options_parts.append(f"-c idle_in_transaction_session_timeout={int(idle_timeout)}")

        kwargs: dict[str, object] = {
            "host": self.config.host,
            "port": self.config.port,
            "dbname": self.config.database,
            "user": self.config.user,
            "password": self.config.password,
            "connect_timeout": timeout,
            "application_name": application_name,
        }

        if options_parts:
            kwargs["options"] = " ".join(options_parts)

        return kwargs

    def _open_connection(self) -> psycopg2.extensions.connection:
        """Open a new psycopg2 connection using the configured parameters."""
        return psycopg2.connect(**self._build_connection_kwargs())

    @contextmanager
    def connect(self) -> Iterator[psycopg2.extensions.connection]:
        """Provide a managed connection that commits/rolls back and always closes."""
        conn = self._open_connection()
        try:
            yield conn
            if not conn.closed and not conn.autocommit:
                conn.commit()
        except Exception:
            if not conn.closed:
                try:
                    conn.rollback()
                except psycopg2.Error:
                    pass
            raise
        finally:
            if not conn.closed:
                conn.close()

    @staticmethod
    def _normalize_dataframe(
        dataframe: pd.DataFrame,
        date_columns: Sequence[str],
    ) -> pd.DataFrame:
        """Convert date columns and replace NaN/NaT with None for database writes."""
        frame = dataframe.copy()
        for column in date_columns:
            if column in frame.columns:
                converted = pd.to_datetime(frame[column], errors="coerce")
                frame[column] = converted.apply(
                    lambda val: val.date() if isinstance(val, (pd.Timestamp, datetime)) and not pd.isna(val) else None
                )
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
