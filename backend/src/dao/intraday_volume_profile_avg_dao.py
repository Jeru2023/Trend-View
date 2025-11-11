"""DAO for aggregated intraday minute volume profiles."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from psycopg2 import sql
from psycopg2.extras import execute_values

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "intraday_volume_profile_avg_schema.sql"

AVG_COLUMNS: Sequence[str] = (
    "stock_code",
    "minute_index",
    "ratio_sum",
    "cumulative_ratio_sum",
    "sample_count",
    "avg_ratio",
    "avg_cumulative_ratio",
    "is_frozen",
    "last_trade_date",
)


class IntradayVolumeProfileAverageDAO(PostgresDAOBase):
    """Aggregated per-minute ratios once sufficient history is collected."""

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "intraday_volume_profile_avg_table", "intraday_volume_profile_avg")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            table_stock_idx=f"{self._table_name}_stock_idx",
            table_frozen_idx=f"{self._table_name}_frozen_idx",
        )

    def upsert_running_average(
        self,
        stock_code: str,
        trade_date: date,
        entries: Iterable[dict[str, float]],
    ) -> int:
        records: List[tuple] = []
        for entry in entries:
            ratio = float(entry.get("volume_ratio") or 0.0)
            cumulative = float(entry.get("cumulative_ratio") or 0.0)
            records.append(
                (
                    stock_code,
                    int(entry["minute_index"]),
                    ratio,
                    cumulative,
                    1,
                    ratio,
                    cumulative,
                    False,
                    trade_date,
                )
            )
        if not records:
            return 0

        insert_sql = sql.SQL(
            """
            INSERT INTO {schema}.{table} (stock_code, minute_index, ratio_sum, cumulative_ratio_sum, sample_count, avg_ratio, avg_cumulative_ratio, is_frozen, last_trade_date)
            VALUES %s
            ON CONFLICT (stock_code, minute_index) DO UPDATE
            SET ratio_sum = {table}.ratio_sum + EXCLUDED.ratio_sum,
                cumulative_ratio_sum = {table}.cumulative_ratio_sum + EXCLUDED.cumulative_ratio_sum,
                sample_count = {table}.sample_count + EXCLUDED.sample_count,
                avg_ratio = ( {table}.ratio_sum + EXCLUDED.ratio_sum ) / ({table}.sample_count + EXCLUDED.sample_count),
                avg_cumulative_ratio = ( {table}.cumulative_ratio_sum + EXCLUDED.cumulative_ratio_sum ) / ({table}.sample_count + EXCLUDED.sample_count),
                last_trade_date = EXCLUDED.last_trade_date,
                updated_at = CURRENT_TIMESTAMP
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                execute_values(cur, insert_sql.as_string(conn), records)
            conn.commit()
        return len(records)

    def list_frozen_codes(self) -> List[str]:
        query = sql.SQL(
            "SELECT DISTINCT stock_code FROM {schema}.{table} WHERE is_frozen = TRUE"
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query)
                return [row[0] for row in cur.fetchall()]

    def get_sample_count(self, stock_code: str) -> int:
        query = sql.SQL(
            "SELECT sample_count FROM {schema}.{table} WHERE stock_code = %s ORDER BY minute_index LIMIT 1"
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (stock_code,))
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else 0

    def mark_frozen(self, stock_code: str) -> None:
        query = sql.SQL(
            "UPDATE {schema}.{table} SET is_frozen = TRUE, updated_at = CURRENT_TIMESTAMP WHERE stock_code = %s"
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (stock_code,))
            conn.commit()

    def fetch_profiles(self, stock_codes: Sequence[str]) -> Dict[str, Dict[int, float]]:
        if not stock_codes:
            return {}
        query = sql.SQL(
            """
            SELECT stock_code, minute_index, avg_cumulative_ratio
            FROM {schema}.{table}
            WHERE stock_code = ANY(%s)
            """
        ).format(schema=sql.Identifier(self.config.schema), table=sql.Identifier(self._table_name))

        profiles: Dict[str, Dict[int, float]] = {}
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (list(stock_codes),))
                for stock_code, minute_index, avg_cumulative_ratio in cur.fetchall():
                    if avg_cumulative_ratio is None:
                        continue
                    bucket = profiles.setdefault(stock_code, {})
                    bucket[int(minute_index)] = float(avg_cumulative_ratio)
        return profiles


__all__ = ["IntradayVolumeProfileAverageDAO"]
