"""
DAO for managing A-share trading calendar records.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "trade_calendar_schema.sql"

TRADE_CALENDAR_FIELDS: Sequence[str] = ("cal_date", "exchange", "is_open")


class TradeCalendarDAO(PostgresDAOBase):
    """Persistence helper for the trading calendar."""

    _conflict_keys: Sequence[str] = ("cal_date",)

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "trade_calendar_table", "trade_calendar")
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
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (cal_date DESC)"
                ).format(
                    index=sql.Identifier(f"{self._table_name}_cal_date_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (is_open)"
                ).format(
                    index=sql.Identifier(f"{self._table_name}_is_open_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )

    def upsert(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            normalized = dataframe.copy()
            if "cal_date" in normalized.columns:
                normalized["cal_date"] = pd.to_datetime(normalized["cal_date"], errors="coerce").dt.date
            if "is_open" in normalized.columns:
                normalized["is_open"] = normalized["is_open"].map(lambda value: bool(int(value)) if value is not None else None)

            available_columns = [column for column in TRADE_CALENDAR_FIELDS if column in normalized.columns]
            return self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=normalized.loc[:, available_columns],
                columns=available_columns,
                conflict_keys=self._conflict_keys,
                date_columns=("cal_date",),
            )

    def is_trading_day(self, cal_date: date) -> Optional[bool]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT is_open FROM {schema}.{table} WHERE cal_date = %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (cal_date,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return bool(row[0])

    def list_between(self, start: date, end: date) -> Iterable[Dict[str, object]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT cal_date, exchange, is_open, updated_at "
                        "FROM {schema}.{table} "
                        "WHERE cal_date BETWEEN %s AND %s "
                        "ORDER BY cal_date ASC"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (start, end),
                )
                for cal_date_value, exchange, is_open, updated_at in cur.fetchall():
                    yield {
                        "cal_date": cal_date_value,
                        "exchange": exchange,
                        "is_open": bool(is_open),
                        "updated_at": updated_at,
                    }

    def stats(self) -> Dict[str, Optional[datetime]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table}"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                count, last_updated = cur.fetchone()
        return {"count": count or 0, "updated_at": last_updated}


__all__ = ["TradeCalendarDAO", "TRADE_CALENDAR_FIELDS"]
