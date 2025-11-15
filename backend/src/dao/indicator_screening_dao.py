"""DAO for indicator-based stock screening results."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "indicator_screening_schema.sql"
MAX_FETCH_LIMIT = 10000

INDICATOR_SCREENING_COLUMNS: Sequence[str] = (
    "indicator_code",
    "indicator_name",
    "captured_at",
    "rank",
    "stock_code",
    "stock_code_full",
    "stock_name",
    "price_change_percent",
    "stage_change_percent",
    "last_price",
    "volume_shares",
    "volume_text",
    "baseline_volume_shares",
    "baseline_volume_text",
    "volume_days",
    "turnover_percent",
    "turnover_rate",
    "turnover_amount",
    "turnover_amount_text",
    "industry",
    "high_price",
    "low_price",
)


class IndicatorScreeningDAO(PostgresDAOBase):
    """Persistence helper for indicator screening datasets."""

    _conflict_keys: Sequence[str] = ("indicator_code", "stock_code")
    _date_columns: Sequence[str] = ("captured_at",)
    _schema_initialized: bool = False
    _schema_lock: Lock = Lock()

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "indicator_screening_table", "indicator_screening")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        if type(self)._schema_initialized:
            return
        with type(self)._schema_lock:
            if type(self)._schema_initialized:
                return
            self._execute_schema_template(
                conn,
                self._schema_sql_template,
                schema=self.config.schema,
                table=self._table_name,
                indicator_rank_idx=f"{self._table_name}_indicator_rank_idx",
            )
            type(self)._schema_initialized = True
            return

    def upsert(self, dataframe: pd.DataFrame, *, conn: Optional[PGConnection] = None) -> int:
        if dataframe is None or dataframe.empty:
            return 0

        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._write_dataframe(owned_conn, dataframe)

        self.ensure_table(conn)
        return self._write_dataframe(conn, dataframe)

    def _write_dataframe(self, conn: PGConnection, dataframe: pd.DataFrame) -> int:
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=INDICATOR_SCREENING_COLUMNS,
            conflict_keys=self._conflict_keys,
            date_columns=self._date_columns,
        )

    def list_entries(
        self,
        *,
        indicator_code: str,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, object]:
        limit = max(1, min(int(limit), MAX_FETCH_LIMIT))
        offset = max(0, int(offset))

        columns = ["indicator_code", "indicator_name", "captured_at", *INDICATOR_SCREENING_COLUMNS[3:]]
        select_list = sql.SQL(", ").join(sql.SQL("s.") + sql.Identifier(col) for col in columns)

        stats_query = sql.SQL(
            """
            SELECT COUNT(*), MAX(captured_at)
            FROM {schema}.{table}
            WHERE indicator_code = %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(stats_query, (indicator_code,))
                total_count, latest_captured = cur.fetchone()
                if latest_captured is None:
                    return {
                        "total": 0,
                        "items": [],
                        "latest_captured_at": None,
                        "indicator_code": indicator_code,
                    }
                query = sql.SQL(
                    """
                    SELECT {columns}
                    FROM {schema}.{table}
                    WHERE indicator_code = %s AND captured_at = %s
                    ORDER BY rank NULLS LAST, stock_code
                    LIMIT %s OFFSET %s
                    """
                ).format(
                    columns=select_list,
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
                count_query = sql.SQL(
                    """
                    SELECT COUNT(*)
                    FROM {schema}.{table}
                    WHERE indicator_code = %s AND captured_at = %s
                    """
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
                cur.execute(count_query, (indicator_code, latest_captured))
                filtered_count = cur.fetchone()[0] or 0
                cur.execute(query, (indicator_code, latest_captured, limit, offset))
                rows = cur.fetchall()

        items: List[dict[str, object]] = []
        for row in rows:
            items.append({column: value for column, value in zip(columns, row)})

        return {
            "total": int(filtered_count),
            "items": items,
            "latest_captured_at": latest_captured,
            "indicator_code": indicator_code,
        }

    def list_recent_entries(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, object]:
        limit = max(1, min(int(limit), MAX_FETCH_LIMIT))
        offset = max(0, int(offset))

        columns = ["indicator_code", "indicator_name", "captured_at", *INDICATOR_SCREENING_COLUMNS[3:]]
        select_list = sql.SQL(", ".join(f's."{col}"' for col in columns))

        query = sql.SQL(
            """
            SELECT {columns}
            FROM {schema}.{table}
            ORDER BY captured_at DESC NULLS LAST, rank NULLS LAST, indicator_code, stock_code
            LIMIT %s OFFSET %s
            """
        ).format(
            columns=select_list,
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        stats_query = sql.SQL(
            "SELECT COUNT(*), MAX(captured_at) FROM {schema}.{table}"
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(stats_query)
                count, latest_captured = cur.fetchone()
                cur.execute(query, (limit, offset))
                rows = cur.fetchall()

        items: List[dict[str, object]] = []
        for row in rows:
            items.append({column: value for column, value in zip(columns, row)})

        return {
            "total": int(count or 0),
            "items": items,
            "latest_captured_at": latest_captured,
            "indicator_code": None,
        }

    def latest_captured(self) -> Optional[str]:
        query = sql.SQL(
            "SELECT MAX(captured_at) FROM {schema}.{table}"
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                return row[0] if row else None

    def fetch_latest_entries_for_codes(
        self,
        *,
        indicator_code: str,
        stock_codes: Sequence[str],
    ) -> Dict[str, dict[str, object]]:
        if not indicator_code or not stock_codes:
            return {}

        columns = ["indicator_code", "indicator_name", "captured_at", *INDICATOR_SCREENING_COLUMNS[3:]]
        select_list = sql.SQL(", ").join(sql.Identifier(col) for col in columns)

        latest_captured_sql = sql.SQL(
            "SELECT MAX(captured_at) FROM {schema}.{table} WHERE indicator_code = %s"
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        query = sql.SQL(
            """
            SELECT {columns}
            FROM {schema}.{table} AS s
            WHERE s.indicator_code = %s
              AND s.stock_code_full = ANY(%s)
              AND s.captured_at = ({latest_subquery})
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
            columns=select_list,
            latest_subquery=latest_captured_sql,
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (indicator_code, list(stock_codes), indicator_code))
                rows = cur.fetchall()

        results: Dict[str, dict[str, object]] = {}
        for row in rows:
            record = {column: value for column, value in zip(columns, row)}
            stock_code_full = record.get("stock_code_full")
            if stock_code_full:
                results[str(stock_code_full)] = record
        return results

    def query_full_universe(
        self,
        *,
        trade_date: date,
        indicator_codes: Sequence[str],
        primary_indicator_code: str | None,
        limit: int,
        offset: int,
        net_income_yoy_min: float | None,
        net_income_qoq_min: float | None,
        pe_min: float | None,
        pe_max: float | None,
        turnover_rate_min: float | None,
        turnover_rate_max: float | None,
        daily_change_min: float | None,
        daily_change_max: float | None,
        pct_change_1w_max: float | None,
        pct_change_1m_max: float | None,
        require_big_deal_inflow: bool,
        tables: dict[str, str],
    ) -> dict[str, object]:
        limit = max(1, min(int(limit), MAX_FETCH_LIMIT))
        offset = max(0, int(offset))
        indicator_list = list(dict.fromkeys(indicator_codes or []))

        schema_identifier = sql.Identifier(self.config.schema)
        daily_trade_table = sql.Identifier(tables["daily_trade"])
        stock_table = sql.Identifier(tables["stock_basic"])
        daily_indicator_table = sql.Identifier(tables["daily_indicator"])
        trade_metrics_table = sql.Identifier(tables["daily_trade_metrics"])
        fundamental_table = sql.Identifier(tables["fundamental_metrics"])
        big_deal_table = sql.Identifier(tables["big_deal"])

        indicator_cte = sql.SQL(
            """
            indicator_latest AS (
                SELECT s.stock_code_full,
                       s.stock_code,
                       s.stock_name,
                       s.indicator_code,
                       s.indicator_name,
                       s.captured_at,
                       s.rank,
                       s.price_change_percent,
                       s.stage_change_percent,
                       s.last_price,
                       s.volume_shares,
                       s.volume_text,
                       s.baseline_volume_shares,
                       s.baseline_volume_text,
                       s.volume_days,
                       s.turnover_percent,
                       s.turnover_rate,
                       s.turnover_amount,
                       s.turnover_amount_text,
                       s.high_price,
                       s.low_price
                FROM {schema}.{table} AS s
                JOIN (
                    SELECT indicator_code, MAX(captured_at) AS captured_at
                    FROM {schema}.{table}
                    GROUP BY indicator_code
                ) latest
                  ON latest.indicator_code = s.indicator_code
                 AND latest.captured_at = s.captured_at
            )
            """
        ).format(schema=schema_identifier, table=sql.Identifier(self._table_name))

        daily_indicator_cte = sql.SQL(
            """
            daily_indicator_snapshot AS (
                SELECT DISTINCT ON (ts_code)
                       ts_code,
                       turnover_rate,
                       pe
                FROM {schema}.{table}
                ORDER BY ts_code, trade_date DESC
            )
            """
        ).format(schema=schema_identifier, table=daily_indicator_table)

        metrics_cte = sql.SQL(
            """
            trade_metrics_snapshot AS (
                SELECT DISTINCT ON (ts_code)
                       ts_code,
                       pct_change_1w,
                       pct_change_1m
                FROM {schema}.{table}
                ORDER BY ts_code, trade_date DESC
            )
            """
        ).format(schema=schema_identifier, table=trade_metrics_table)

        start_dt = datetime.combine(trade_date, time.min)
        end_dt = start_dt + timedelta(days=1)
        buy_pattern = "买%"
        sell_pattern = "卖%"

        big_deal_cte = sql.SQL(
            """
            big_deal_summary AS (
                SELECT stock_code,
                       COALESCE(SUM(CASE WHEN trade_side LIKE %s THEN trade_amount ELSE 0 END), 0) AS buy_amount,
                       COALESCE(SUM(CASE WHEN trade_side LIKE %s THEN trade_amount ELSE 0 END), 0) AS sell_amount,
                       COALESCE(SUM(CASE WHEN trade_side LIKE %s THEN trade_amount ELSE 0 END), 0)
                       - COALESCE(SUM(CASE WHEN trade_side LIKE %s THEN trade_amount ELSE 0 END), 0) AS net_amount,
                       COUNT(*) AS trade_count
                FROM {schema}.{table}
                WHERE trade_time >= %s
                  AND trade_time < %s
                GROUP BY stock_code
            )
            """
        ).format(schema=schema_identifier, table=big_deal_table)

        cte_sql = sql.SQL("WITH ") + sql.SQL(", ").join(
            (indicator_cte, daily_indicator_cte, metrics_cte, big_deal_cte)
        )

        if primary_indicator_code:
            primary_join = sql.SQL(
                """
                LEFT JOIN indicator_latest AS primary_ind
                       ON primary_ind.stock_code_full = dt.ts_code
                      AND primary_ind.indicator_code = %s
                """
            )
            primary_params: list[object] = [primary_indicator_code]
        else:
            primary_join = sql.SQL(
                """
                LEFT JOIN LATERAL (
                    SELECT NULL::TEXT AS indicator_code,
                           NULL::TEXT AS indicator_name,
                           NULL::TIMESTAMP WITHOUT TIME ZONE AS captured_at,
                           NULL::NUMERIC AS rank,
                           NULL::NUMERIC AS price_change_percent,
                           NULL::NUMERIC AS stage_change_percent,
                           NULL::NUMERIC AS last_price,
                           NULL::NUMERIC AS volume_shares,
                           NULL::TEXT AS volume_text,
                           NULL::NUMERIC AS baseline_volume_shares,
                           NULL::TEXT AS baseline_volume_text,
                           NULL::INTEGER AS volume_days,
                           NULL::NUMERIC AS turnover_percent,
                           NULL::NUMERIC AS turnover_rate,
                           NULL::NUMERIC AS turnover_amount,
                           NULL::TEXT AS turnover_amount_text,
                           NULL::NUMERIC AS high_price,
                           NULL::NUMERIC AS low_price
                ) AS primary_ind ON TRUE
                """
            )
            primary_params = []

        select_columns = [
            sql.SQL("dt.ts_code"),
            sql.SQL("COALESCE(sb.symbol, split_part(dt.ts_code, '.', 1)) AS stock_code"),
            sql.SQL("sb.name AS stock_name"),
            sql.SQL("sb.industry"),
            sql.SQL("dt.trade_date"),
            sql.SQL("dt.close"),
            sql.SQL("dt.pct_chg"),
            sql.SQL("di.turnover_rate"),
            sql.SQL("di.pe AS pe_ratio"),
            sql.SQL("fm.net_income_yoy_latest"),
            sql.SQL("fm.net_income_qoq_latest"),
            sql.SQL("mt.pct_change_1w"),
            sql.SQL("mt.pct_change_1m"),
            sql.SQL("bds.net_amount AS big_deal_net_amount"),
            sql.SQL("bds.buy_amount AS big_deal_buy_amount"),
            sql.SQL("bds.sell_amount AS big_deal_sell_amount"),
            sql.SQL("bds.trade_count AS big_deal_trade_count"),
            sql.SQL("primary_ind.indicator_code"),
            sql.SQL("primary_ind.indicator_name"),
            sql.SQL("primary_ind.captured_at AS indicator_captured_at"),
            sql.SQL("primary_ind.rank AS indicator_rank"),
            sql.SQL("primary_ind.price_change_percent AS indicator_price_change_percent"),
            sql.SQL("primary_ind.stage_change_percent AS indicator_stage_change_percent"),
            sql.SQL("primary_ind.last_price AS indicator_last_price"),
            sql.SQL("primary_ind.volume_shares AS indicator_volume_shares"),
            sql.SQL("primary_ind.volume_text AS indicator_volume_text"),
            sql.SQL("primary_ind.baseline_volume_shares AS indicator_baseline_volume_shares"),
            sql.SQL("primary_ind.baseline_volume_text AS indicator_baseline_volume_text"),
            sql.SQL("primary_ind.volume_days AS indicator_volume_days"),
            sql.SQL("primary_ind.turnover_percent AS indicator_turnover_percent"),
            sql.SQL("primary_ind.turnover_rate AS indicator_turnover_rate"),
            sql.SQL("primary_ind.turnover_amount AS indicator_turnover_amount"),
            sql.SQL("primary_ind.turnover_amount_text AS indicator_turnover_amount_text"),
            sql.SQL("primary_ind.high_price AS indicator_high_price"),
            sql.SQL("primary_ind.low_price AS indicator_low_price"),
        ]

        base_from = sql.SQL(
            """
            FROM {schema}.{daily_trade} AS dt
            LEFT JOIN {schema}.{stock_table} AS sb ON sb.ts_code = dt.ts_code
            LEFT JOIN daily_indicator_snapshot AS di ON di.ts_code = dt.ts_code
            LEFT JOIN trade_metrics_snapshot AS mt ON mt.ts_code = dt.ts_code
            LEFT JOIN {schema}.{fundamental_table} AS fm ON fm.ts_code = dt.ts_code
            LEFT JOIN big_deal_summary AS bds ON bds.stock_code = sb.symbol
            {primary_join}
            """
        ).format(
            schema=schema_identifier,
            daily_trade=daily_trade_table,
            stock_table=stock_table,
            fundamental_table=fundamental_table,
            primary_join=primary_join,
        )

        where_clauses: list[sql.SQL] = [sql.SQL("dt.trade_date = %s"), sql.SQL("dt.is_intraday = FALSE")]
        where_params: list[object] = [trade_date]

        if net_income_yoy_min is not None:
            where_clauses.append(sql.SQL("fm.net_income_yoy_latest >= %s"))
            where_params.append(net_income_yoy_min)
        if net_income_qoq_min is not None:
            where_clauses.append(sql.SQL("fm.net_income_qoq_latest >= %s"))
            where_params.append(net_income_qoq_min)
        if pe_min is not None:
            where_clauses.append(sql.SQL("di.pe >= %s"))
            where_params.append(pe_min)
        if pe_max is not None:
            where_clauses.append(sql.SQL("di.pe <= %s"))
            where_params.append(pe_max)
        if turnover_rate_min is not None:
            where_clauses.append(sql.SQL("di.turnover_rate >= %s"))
            where_params.append(turnover_rate_min)
        if turnover_rate_max is not None:
            where_clauses.append(sql.SQL("di.turnover_rate <= %s"))
            where_params.append(turnover_rate_max)
        if daily_change_min is not None:
            where_clauses.append(sql.SQL("dt.pct_chg >= %s"))
            where_params.append(daily_change_min)
        if daily_change_max is not None:
            where_clauses.append(sql.SQL("dt.pct_chg <= %s"))
            where_params.append(daily_change_max)
        if pct_change_1w_max is not None:
            where_clauses.append(sql.SQL("mt.pct_change_1w <= %s"))
            where_params.append(pct_change_1w_max)
        if pct_change_1m_max is not None:
            where_clauses.append(sql.SQL("mt.pct_change_1m <= %s"))
            where_params.append(pct_change_1m_max)

        for code in indicator_list:
            where_clauses.append(
                sql.SQL(
                    "EXISTS (SELECT 1 FROM indicator_latest AS li WHERE li.stock_code_full = dt.ts_code AND li.indicator_code = %s)"
                )
            )
            where_params.append(code)

        if require_big_deal_inflow:
            where_clauses.append(sql.SQL("(bds.net_amount IS NOT NULL AND bds.net_amount > 0)"))

        where_sql = sql.SQL("")
        if where_clauses:
            where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_clauses)

        order_sql = (
            sql.SQL("ORDER BY primary_ind.rank NULLS LAST, dt.ts_code")
            if primary_indicator_code
            else sql.SQL("ORDER BY dt.pct_chg DESC NULLS LAST, dt.ts_code")
        )

        count_query = cte_sql + sql.SQL(" SELECT COUNT(*) ") + base_from + where_sql
        data_query = (
            cte_sql
            + sql.SQL(" SELECT ")
            + sql.SQL(", ").join(select_columns)
            + base_from
            + where_sql
            + sql.SQL(" ")
            + order_sql
            + sql.SQL(" LIMIT %s OFFSET %s")
        )

        cte_params = [buy_pattern, sell_pattern, buy_pattern, sell_pattern, start_dt, end_dt]
        count_params = cte_params + primary_params + where_params
        data_params = count_params + [limit, offset]

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query, count_params)
                total = cur.fetchone()[0] or 0
                cur.execute(data_query, data_params)
                rows = cur.fetchall()

        column_names = [
            "ts_code",
            "stock_code",
            "stock_name",
            "industry",
            "trade_date",
            "close",
            "pct_chg",
            "turnover_rate",
            "pe_ratio",
            "net_income_yoy_latest",
            "net_income_qoq_latest",
            "pct_change_1w",
            "pct_change_1m",
            "big_deal_net_amount",
            "big_deal_buy_amount",
            "big_deal_sell_amount",
            "big_deal_trade_count",
            "indicator_code",
            "indicator_name",
            "indicator_captured_at",
            "indicator_rank",
            "indicator_price_change_percent",
            "indicator_stage_change_percent",
            "indicator_last_price",
            "indicator_volume_shares",
            "indicator_volume_text",
            "indicator_baseline_volume_shares",
            "indicator_baseline_volume_text",
            "indicator_volume_days",
            "indicator_turnover_percent",
            "indicator_turnover_rate",
            "indicator_turnover_amount",
            "indicator_turnover_amount_text",
            "indicator_high_price",
            "indicator_low_price",
        ]

        items: list[dict[str, object]] = [
            {column: value for column, value in zip(column_names, row)} for row in rows
        ]

        return {
            "total": int(total),
            "items": items,
            "trade_date": trade_date,
        }
