# db/connection.py
"""
DB connection helper for Supabase Postgres.

- Assumes STREAMDASH_DB=postgres and DATABASE_URL is set.
- Returns a psycopg2 connection (DB-API).
- Raises clear errors if psycopg2 is missing or DATABASE_URL is not provided.
"""

import os
from typing import Any

DB_TYPE = os.getenv("STREAMDASH_DB", "postgres").lower()

if DB_TYPE != "postgres":
    raise RuntimeError("This application is configured to use Postgres. Set STREAMDASH_DB=postgres.")

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import RealDictCursor  # type: ignore
except Exception as e:
    raise RuntimeError("psycopg2 is required for Postgres. Install with: pip install psycopg[binary]") from e


def get_connection() -> Any:
    """
    Return a psycopg2 connection using DATABASE_URL or individual PG* env vars.

    Usage:
      export STREAMDASH_DB=postgres
      export DATABASE_URL="postgresql://user:pass@host:5432/dbname"

    Returns:
      psycopg2 connection object
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url)

    # Fallback to individual env vars if DATABASE_URL is not set
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASS", "")
    dbname = os.getenv("DB_NAME", "postgres")

    return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)