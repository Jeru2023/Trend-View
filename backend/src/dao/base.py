"""
Shared PostgreSQL DAO utilities for the Trend View backend.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Iterator, Sequence, Optional

import math
import re

import pandas as pd
import psycopg2
from psycopg2 import sql, errors
from psycopg2.extras import execute_values

from ..config.settings import PostgresSettings

DEFAULT_CONNECT_TIMEOUT = 3
DEFAULT_APPLICATION_NAME = "trend_view_backend"
_ADD_COLUMN_PATTERN = re.compile(
    r"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+\"?([A-Za-z0-9_]+)\"?",
    re.IGNORECASE,
)


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
                frame[column] = converted
        return frame.where(pd.notnull(frame), None)

    @staticmethod
    def _execute_schema_template(
        conn: psycopg2.extensions.connection,
        template_sql: str,
        *,
        schema: str,
        table: str,
        **extra_identifiers: str,
    ) -> None:
        """Run a schema creation SQL template with formatted identifiers."""

        format_args: dict[str, sql.Composable] = {
            "schema": sql.Identifier(schema),
            "table": sql.Identifier(table),
        }

        for key, value in extra_identifiers.items():
            format_args[key] = sql.Identifier(value)

        table_sql = sql.SQL(template_sql).format(**format_args)

        schema_name = schema
        table_name = table

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema_name}").format(
                    schema_name=sql.Identifier(schema_name)
                )
            )

        def _load_existing_columns() -> set[str]:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    """,
                    (schema_name, table_name),
                )
                return {row[0].lower() for row in cur.fetchall()}

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema_name, table_name),
            )
            table_exists = cur.fetchone() is not None

        existing_columns: set[str] = _load_existing_columns() if table_exists else set()

        rendered_sql = table_sql.as_string(conn)
        statements = [stmt.strip() for stmt in rendered_sql.split(";") if stmt.strip()]

        for statement in statements:
            stripped_statement = statement.lstrip()
            upper_stmt = stripped_statement.upper()
            if table_exists and upper_stmt.startswith("CREATE TABLE"):
                continue

            pending_column_update: Optional[str] = None
            if table_exists and upper_stmt.startswith("ALTER TABLE"):
                match = _ADD_COLUMN_PATTERN.search(statement)
                if match:
                    column_name = match.group(1).strip('"').lower()
                    if column_name in existing_columns:
                        continue
                    pending_column_update = column_name

            try:
                with conn.cursor() as cur:
                    cur.execute(statement)
            except (errors.UniqueViolation, errors.DuplicateTable, errors.DuplicateObject):
                conn.rollback()
                table_exists = True
                existing_columns = _load_existing_columns()
                continue
            except Exception:
                conn.rollback()
                raise
            else:
                if upper_stmt.startswith("CREATE TABLE"):
                    table_exists = True
                    existing_columns = _load_existing_columns()
                elif pending_column_update:
                    existing_columns.add(pending_column_update)

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
        for record in records:
            for key, value in list(record.items()):
                if value is None:
                    continue
                if isinstance(value, float) and math.isnan(value):
                    record[key] = None
                    continue
                if pd.isna(value):
                    record[key] = None
                    continue
            for column in date_columns:
                if column not in record:
                    continue
                value = record[column]
                if value is None:
                    continue
                if isinstance(value, pd.Timestamp):
                    value = value.to_pydatetime()
                if isinstance(value, datetime):
                    record[column] = value.replace(tzinfo=None)
                else:
                    record[column] = value
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
