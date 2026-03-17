# db/connection.py
"""
DB connection helper for Supabase Postgres.

- Supports psycopg (psycopg3) and psycopg2 (psycopg2-binary).
- Uses DATABASE_URL if present, otherwise falls back to DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME.
- Exposes get_connection() which returns a DB-API connection object.
- Exposes get_dict_cursor(conn) to obtain a dict-like cursor for convenience.
- Raises clear, actionable errors when drivers or configuration are missing.
"""

import os
from typing import Any, Optional, Tuple, Iterator, ContextManager
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

DATABASE_URL = os.getenv("DATABASE_URL")


def get_engine() -> Engine:
    """
    Retorna um SQLAlchemy Engine usando DATABASE_URL.
    Use Engine para operações com pandas (read_sql/to_sql) e para compatibilidade geral.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurado")
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def get_connection() -> Any:
    """
    Retorna uma conexão DB-API (psycopg.Connection) se psycopg estiver disponível.
    Se psycopg não estiver instalado, retorna um SQLAlchemy Engine (compatibilidade).
    Preferência: use get_engine() quando precisar de Engine.
    """
    if psycopg is None:
        # fallback: return engine so callers that expect SQLAlchemy still work
        return get_engine()
    # psycopg.connect retorna psycopg.Connection
    return psycopg.connect(DATABASE_URL)


def get_dict_cursor(conn: Optional[Any] = None) -> Tuple[Any, Any]:
    """
    Retorna (conn, cursor) onde cursor produz dicionários por linha (row factory).
    - Se conn for psycopg.Connection: cria cursor com row_factory=dict_row e retorna (conn, cursor).
    - Se conn for None: abre nova psycopg.Connection e cursor (caller deve fechar conn).
    - Se conn for SQLAlchemy Engine/Connection: obtém raw DBAPI connection e cria cursor dict-like.
    Uso típico:
        conn, cur = get_dict_cursor()
        try:
            cur.execute("SELECT ...")
            for row in cur:
                ...
        finally:
            cur.close()
            conn.close()
    Retorna tupla (conn, cursor).
    """
    # psycopg path (recommended)
    if psycopg is not None:
        if conn is None:
            conn = psycopg.connect(DATABASE_URL)
            cur = conn.cursor(row_factory=dict_row)
            return conn, cur

        # if user passed a psycopg.Connection
        if isinstance(conn, psycopg.Connection):
            cur = conn.cursor(row_factory=dict_row)
            return conn, cur

    # Fallback for SQLAlchemy Engine/Connection
    # If conn is a SQLAlchemy Engine, get raw connection and cursor
    try:
        from sqlalchemy.engine import Connection as SAConnection, Engine as SAEngine
    except Exception:
        SAConnection = SAEngine = None

    if SAEngine is not None and isinstance(conn, SAEngine):
        # get raw DBAPI connection from engine
        raw = conn.raw_connection()
        cur = raw.cursor()
        # wrap rows into dicts on fetch if needed (simple wrapper)
        return raw, _DictCursorWrapper(cur)

    if SAConnection is not None and isinstance(conn, SAConnection):
        raw = conn.connection
        cur = raw.cursor()
        return raw, _DictCursorWrapper(cur)

    # Last resort: if conn has cursor method, return cursor (no dict row factory)
    if conn is not None and hasattr(conn, "cursor"):
        cur = conn.cursor()
        # try to set row factory if psycopg-like
        try:
            cur.row_factory = dict_row  # may fail silently
        except Exception:
            pass
        return conn, cur

    raise RuntimeError("Não foi possível criar cursor dict. Forneça psycopg or SQLAlchemy Engine.")


class _DictCursorWrapper:
    """
    Wrapper simples que converte fetches do cursor DBAPI em dicts usando cursor.description.
    Usado quando só temos um cursor DBAPI sem suporte a row_factory.
    """
    def __init__(self, cur):
        self._cur = cur

    def execute(self, *args, **kwargs):
        return self._cur.execute(*args, **kwargs)

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    def fetchall(self):
        rows = self._cur.fetchall()
        return [self._row_to_dict(r) for r in rows]

    def __iter__(self):
        for r in self._cur:
            yield self._row_to_dict(r)

    def close(self):
        try:
            self._cur.close()
        except Exception:
            pass

    @property
    def description(self):
        return self._cur.description

    def _row_to_dict(self, row):
        desc = self._cur.description or []
        return {desc[i].name if hasattr(desc[i], "name") else desc[i][0]: row[i] for i in range(len(row))}