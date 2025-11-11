"""DAO for per-stock intraday minute volume profiles (daily snapshots)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable, List, Sequence

from psycopg2 import sql
from psycopg2.extras import execute_values

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "intraday_volume_profile_daily_schema.sql"

DAILY_COLUMNS: Sequence[str] = (
    "stock_code",
    "trade_date",
    "minute_index",
    "volume_ratio",
    "cumulative_ratio",
    "minute_volume",
)


class IntradayVolumeProfileDailyDAO(PostgresDAOBase):
    """Persistence helper for daily minute-by-minute volume ratios."""

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "intraday_volume_profile_daily_table", "intraday_volume_profile_daily")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            table_trade_idx=f"{self._table_name}_trade_idx",
        )

    def replace_profile(self, stock_code: str, trade_date: date, entries: Iterable[dict[str, object]]) -> int:
        records: List[tuple] = []
        for entry in entries:
            records.append(
                (
                    stock_code,
                    trade_date,
                    int(entry["minute_index"]),
                    float(entry.get("volume_ratio") or 0.0),
                    float(entry.get("cumulative_ratio") or 0.0),
                    int(entry.get("minute_volume") or 0),
                )
            )
        if not records:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DELETE FROM {schema}.{table} WHERE stock_code = %s AND trade_date = %s").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (stock_code, trade_date),
                )
                execute_values(
                    cur,
                    sql.SQL(
                        "INSERT INTO {schema}.{table} (stock_code, trade_date, minute_index, volume_ratio, cumulative_ratio, minute_volume) "
                        "VALUES %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ).as_string(conn),
                    records,
                )
            conn.commit()
        return len(records)


__all__ = ["IntradayVolumeProfileDailyDAO"]
