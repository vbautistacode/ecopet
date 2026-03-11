# db/connection.py
"""
DB connection helper for Supabase Postgres.

- Supports psycopg (psycopg3) and psycopg2 (psycopg2-binary).
- Uses DATABASE_URL if present, otherwise falls back to DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME.
- Exposes get_connection() which returns a DB-API connection object.
- Exposes get_dict_cursor(conn) to obtain a dict-like cursor for convenience.
- Raises clear, actionable errors when drivers or configuration are missing.
"""

from __future__ import annotations

import os
from typing import Any, Optional, Tuple

DB_TYPE = os.getenv("STREAMDASH_DB", "postgres").lower()
if DB_TYPE != "postgres":
    raise RuntimeError("This application is configured to use Postgres. Set STREAMDASH_DB=postgres.")

# Try psycopg (psycopg3) first, then psycopg2
_DRIVER: Optional[str] = None
_psycopg = None
_psycopg2 = None
_RealDictCursor = None

try:
    import psycopg  # type: ignore
    _psycopg = psycopg
    _DRIVER = "psycopg"
except Exception:
    try:
        import psycopg2  # type: ignore
        from psycopg2.extras import RealDictCursor  # type: ignore
        _psycopg2 = psycopg2
        _RealDictCursor = RealDictCursor
        _DRIVER = "psycopg2"
    except Exception:
        raise RuntimeError(
            "No Postgres driver found. Install one of:\n"
            "  pip install 'psycopg[binary]'   # psycopg (psycopg3)\n"
            "  pip install psycopg2-binary     # psycopg2\n"
        )


def _build_dsn_from_env() -> str:
    """Construct a libpq-style DSN from individual env vars."""
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASS", "")
    dbname = os.getenv("DB_NAME", "postgres")
    # prefer DATABASE_URL if present; this function only used as fallback
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


def get_connection(dsn: Optional[str] = None, connect_kwargs: Optional[dict] = None) -> Any:
    """
    Return a Postgres connection object.

    Priority for connection string:
      1. explicit dsn argument
      2. DATABASE_URL env var
      3. constructed from DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME

    Parameters:
      dsn: optional connection string (overrides env)
      connect_kwargs: optional dict passed to driver connect call

    Returns:
      A DB-API connection object (psycopg or psycopg2 connection).
    """
    connect_kwargs = connect_kwargs or {}
    database_url = dsn or os.getenv("DATABASE_URL") or _build_dsn_from_env()

    if _DRIVER == "psycopg":
        # psycopg3: returns a connection object; use autocommit if desired via kwargs
        try:
            # psycopg.connect accepts a URL string
            return _psycopg.connect(database_url, **connect_kwargs)
        except Exception as e:
            raise RuntimeError(f"Failed to connect using psycopg: {e}") from e

    if _DRIVER == "psycopg2":
        try:
            # psycopg2.connect accepts a dsn string or keyword args
            # If DATABASE_URL is provided, pass it as dsn
            if os.getenv("DATABASE_URL") or dsn:
                return _psycopg2.connect(database_url, **connect_kwargs)
            # otherwise pass explicit kwargs
            host = os.getenv("DB_HOST", "localhost")
            port = int(os.getenv("DB_PORT", "5432"))
            user = os.getenv("DB_USER", "postgres")
            password = os.getenv("DB_PASS", "")
            dbname = os.getenv("DB_NAME", "postgres")
            return _psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname, **connect_kwargs)
        except Exception as e:
            raise RuntimeError(f"Failed to connect using psycopg2: {e}") from e

    # Should not reach here
    raise RuntimeError("No supported Postgres driver available.")


def get_dict_cursor(conn: Any) -> Any:
    """
    Return a cursor that yields rows as dictionaries.

    - For psycopg3: use conn.cursor(row_factory=psycopg.rows.dict_row)
    - For psycopg2: use cursor(cursor_factory=RealDictCursor)

    Usage:
      conn = get_connection()
      cur = get_dict_cursor(conn)
      cur.execute("SELECT * FROM my_table")
      rows = cur.fetchall()
    """
    if _DRIVER == "psycopg":
        try:
            # psycopg3 row factory
            from psycopg.rows import dict_row  # type: ignore
            return conn.cursor(row_factory=dict_row)
        except Exception as e:
            raise RuntimeError(f"Failed to create dict cursor for psycopg: {e}") from e

    if _DRIVER == "psycopg2":
        if _RealDictCursor is None:
            raise RuntimeError("RealDictCursor not available for psycopg2.")
        try:
            return conn.cursor(cursor_factory=_RealDictCursor)
        except Exception as e:
            raise RuntimeError(f"Failed to create dict cursor for psycopg2: {e}") from e

    raise RuntimeError("No supported Postgres driver available for creating a dict cursor.")


# Optional convenience: test connection (returns True/False)
def test_connection(timeout_seconds: int = 5) -> Tuple[bool, Optional[str]]:
    """
    Quick health check: attempt to open and close a connection.
    Returns (ok, error_message).
    """
    try:
        conn = get_connection(connect_kwargs={"connect_timeout": timeout_seconds})
        try:
            conn.close()
        except Exception:
            pass
        return True, None
    except Exception as e:
        return False, str(e)