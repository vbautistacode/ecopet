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
import contextlib
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

@contextlib.contextmanager
def get_dict_cursor(conn: Optional[Any] = None):
    """
    Context manager que fornece um cursor que retorna dicionários por linha.
    Uso:
        with get_dict_cursor() as cur:
            cur.execute(...)
            for row in cur: ...
    Se conn for fornecido e for psycopg.Connection, usa-o (não fecha conn).
    Se conn for None, abre uma nova conexão (psycopg) e fecha ao sair.
    Também suporta SQLAlchemy Engine/Connection como fallback.
    """
    created_conn = False
    raw_conn = None
    cur = None

    # psycopg path (preferido)
    try:
        import psycopg
        from psycopg.rows import dict_row
    except Exception:
        psycopg = None
        dict_row = None

    try:
        if psycopg is not None:
            # se não passou conn, cria uma nova psycopg.Connection
            if conn is None:
                raw_conn = psycopg.connect(DATABASE_URL)
                created_conn = True
                cur = raw_conn.cursor(row_factory=dict_row)
                yield cur
                return

            # se passou um psycopg.Connection
            if isinstance(conn, psycopg.Connection):
                cur = conn.cursor(row_factory=dict_row)
                yield cur
                return
    except Exception:
        # se psycopg estiver presente mas algo falhar, vamos tentar fallback abaixo
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if created_conn and raw_conn:
            try:
                raw_conn.close()
            except Exception:
                pass
        raise

    # Fallback para SQLAlchemy Engine/Connection ou DBAPI genérico
    try:
        from sqlalchemy.engine import Engine as SAEngine, Connection as SAConnection
    except Exception:
        SAEngine = SAConnection = None

    try:
        if SAEngine is not None and isinstance(conn, SAEngine):
            # abrir raw DBAPI connection a partir do engine
            raw_conn = conn.raw_connection()
            cur = raw_conn.cursor()
            yield _DictCursorWrapper(cur)
            return

        if SAConnection is not None and isinstance(conn, SAConnection):
            raw_conn = conn.connection
            cur = raw_conn.cursor()
            yield _DictCursorWrapper(cur)
            return

        # se recebeu um raw DBAPI connection (psycopg2, etc.)
        if conn is not None and hasattr(conn, "cursor"):
            # tentar usar row factory se disponível (psycopg-like)
            try:
                cur = conn.cursor()
                yield _DictCursorWrapper(cur)
                return
            except Exception:
                # fallback para wrapper
                cur = conn.cursor()
                yield _DictCursorWrapper(cur)
                return

        # se chegou aqui e não abriu nada, tente abrir psycopg se disponível
        if psycopg is not None and raw_conn is None:
            raw_conn = psycopg.connect(DATABASE_URL)
            created_conn = True
            cur = raw_conn.cursor(row_factory=dict_row)
            yield cur
            return

        raise RuntimeError("Não foi possível criar cursor dict. Forneça psycopg ou SQLAlchemy Engine.")
    finally:
        # cleanup: fechar cursor e conexão criada internamente
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if created_conn and raw_conn is not None:
                raw_conn.close()
        except Exception:
            pass

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