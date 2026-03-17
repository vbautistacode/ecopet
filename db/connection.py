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
from typing import Any
from sqlalchemy import create_engine

DATABASE_URL = os.getenv("DATABASE_URL")


def get_engine():
    """
    Retorna um SQLAlchemy Engine usando DATABASE_URL.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurado")
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def get_connection() -> Any:
    """
    Backwards-compatible helper: retorna um objeto de conexão.
    Por padrão retorna um SQLAlchemy Engine (recomendado).
    """
    return get_engine()