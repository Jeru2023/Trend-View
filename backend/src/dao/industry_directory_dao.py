"""DAO for caching Eastmoney industry directory."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "industry_directory_schema.sql"


class IndustryDirectoryDAO(PostgresDAOBase):
    """Persist industry name/code mappings."""

    def __init__(self, config: PostgresSettings) -> None:
        super().__init__(config=config)
        self._table = getattr(config, "industry_directory_table", "industry_directory")
        self._schema_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def _qualified_table(self) -> sql.Composed:
        return sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table),
        )

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_template,
            schema=self.config.schema,
            table=self._table,
            index_code=f"{self._table}_code_idx",
        )

    def replace_all(self, mapping: Dict[str, str]) -> None:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(sql.SQL("DELETE FROM {table}").format(table=self._qualified_table()))
                if mapping:
                    insert = sql.SQL(
                        """
                        INSERT INTO {table} (industry_name, industry_code, updated_at)
                        VALUES (%s, %s, CURRENT_TIMESTAMP)
                        """
                    ).format(table=self._qualified_table())
                    cur.executemany(insert, [(name, code) for name, code in mapping.items()])
            conn.commit()

    def list_entries(self) -> List[Dict[str, str]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT industry_name, industry_code
                        FROM {table}
                        ORDER BY industry_name
                        """
                    ).format(table=self._qualified_table())
                )
                rows = cur.fetchall()
        return [{"industry_name": row[0], "industry_code": row[1]} for row in rows]

    def stats(self) -> Dict[str, object]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT COUNT(*), MAX(updated_at)
                        FROM {table}
                        """
                    ).format(table=self._qualified_table())
                )
                row = cur.fetchone()
        count = row[0] if row else 0
        updated_at = row[1] if row else None
        return {"count": count, "updated_at": updated_at}


__all__ = ["IndustryDirectoryDAO"]
