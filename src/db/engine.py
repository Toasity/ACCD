"""Database engine helpers using psycopg2.

This module provides a minimal connection helper and an `execute` helper
used by the ETL placeholder.
"""
import psycopg2
import psycopg2.extras
from typing import Any, Optional, Sequence

from src.config import get_db_dsn


def get_conn():
    """Return a new psycopg2 connection (caller should close it).

    Connection has autocommit disabled so callers can commit/rollback.
    """
    dsn = get_db_dsn()
    conn = psycopg2.connect(dsn)
    return conn


def execute(conn, sql: str, params: Optional[Sequence[Any]] = None, fetch: bool = False):
    """Execute SQL using given connection. Optionally fetch one row.

    Returns cursor.fetchall() if fetch=True, otherwise None.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        if fetch:
            return cur.fetchall()
        return None
