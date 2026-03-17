import re
import contextlib
import os
from typing import Any
from sqlalchemy import create_engine

_identifier_re = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _safe_ident(name: str) -> str:
    """
    Valida e retorna o identificador SQL entre aspas duplas.
    Lança ValueError se o nome for inválido.
    """
    if not isinstance(name, str) or not _identifier_re.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'


def _qualify(schema: str, name: str) -> str:
    return f'{_safe_ident(schema)}.{_safe_ident(name)}'


@contextlib.contextmanager
def connection_context(engine_or_conn: Any):
    """
    Context manager que aceita:
      - SQLAlchemy Engine (tem .begin())
      - SQLAlchemy Connection-like (tem .execute)
      - psycopg.Connection (psycopg v3) ou raw DBAPI connection (cursor/commit)
    Garante que o objeto yieldado seja compatível com pandas.to_sql (SQLAlchemy Connection)
    quando necessário, criando temporariamente um Engine a partir de DATABASE_URL.
    """
    # 1) SQLAlchemy Engine (has begin)
    if hasattr(engine_or_conn, "begin") and callable(getattr(engine_or_conn, "begin")):
        with engine_or_conn.begin() as conn:
            yield conn
        return

    # 2) SQLAlchemy Connection-like (has execute and not a DBAPI cursor)
    if hasattr(engine_or_conn, "execute") and not hasattr(engine_or_conn, "cursor"):
        yield engine_or_conn
        return

    # 3) psycopg.Connection (psycopg v3) or other DBAPI connection
    try:
        import psycopg
        is_psycopg_conn = isinstance(engine_or_conn, psycopg.Connection)
    except Exception:
        is_psycopg_conn = False

    DATABASE_URL = os.getenv("DATABASE_URL")
    if is_psycopg_conn:
        url = DATABASE_URL
        if not url:
            try:
                url = engine_or_conn.dsn  # best-effort fallback
            except Exception:
                url = None
        if not url:
            raise ValueError("DATABASE_URL not set; cannot create Engine from psycopg.Connection")
        temp_engine = create_engine(url, pool_pre_ping=True)
        try:
            with temp_engine.begin() as conn:
                yield conn
        finally:
            try:
                temp_engine.dispose()
            except Exception:
                pass
        return

    # 4) raw DBAPI connection (has cursor/commit)
    if hasattr(engine_or_conn, "cursor") and hasattr(engine_or_conn, "commit"):
        try:
            yield engine_or_conn
            try:
                engine_or_conn.commit()
            except Exception:
                pass
        except Exception:
            try:
                engine_or_conn.rollback()
            except Exception:
                pass
        return

    raise ValueError("Unsupported connection/engine object")