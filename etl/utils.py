# etl/utils.py
import uuid
import hashlib
import logging
import os
import io
import csv
import re
import contextlib
from datetime import datetime
from typing import Tuple, Dict, List, Optional, Any
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine as SAEngine

DATABASE_URL = os.getenv("DATABASE_URL")

# -------------------------
# Identificadores SQL seguros
# -------------------------
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
    """
    Retorna schema e nome qualificados e escapados: "schema"."name"
    """
    return f'{_safe_ident(schema)}.{_safe_ident(name)}'

# -------------------------
# Utilitários gerais
# -------------------------
def generate_batch_id() -> str:
    return str(uuid.uuid4())

def file_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

def setup_logger(name: str = __name__):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    return logger

# -------------------------
# Load mapping (robusto a encodings)
# -------------------------
def load_mapping(path: str, sep: str = ';') -> Tuple[list, dict]:
    """
    Carrega mapping CSV e retorna (rows_list, mapping_dict).
    Tenta utf-8, depois latin-1; em último caso lê com errors='replace'.
    """
    def _read_with_encoding(enc: str):
        with open(path, 'r', encoding=enc, errors='strict') as f:
            reader = csv.DictReader(f, delimiter=sep)
            rows = [r for r in reader]
            return rows

    # 1) try utf-8
    try:
        rows = _read_with_encoding('utf-8')
    except UnicodeDecodeError:
        # 2) try latin-1 / cp1252
        try:
            rows = _read_with_encoding('latin-1')
        except UnicodeDecodeError:
            # 3) fallback: replace invalid chars to avoid crash
            with open(path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f, delimiter=sep)
                rows = [r for r in reader]

    # build mapping dict coluna_origem -> nome_destino (ignore empty)
    mapping: Dict[str, str] = {}
    for r in rows:
        src = (r.get('coluna_origem') or '').strip()
        tgt = (r.get('nome_destino') or '').strip()
        if src and tgt:
            mapping[src] = tgt

    return rows, mapping

# -------------------------
# Pequenos helpers adicionais (opcionais)
# -------------------------
def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"

@contextlib.contextmanager
def connection_context(engine_or_conn: Any):
    """
    Context manager que aceita:
      - SQLAlchemy Engine (tem .begin())
      - SQLAlchemy Connection (objeto com .execute)
      - psycopg.Connection (psycopg v3) ou raw DBAPI
    Retorna um objeto compatível com pandas.to_sql (SQLAlchemy Connection) ou com .execute.
    """
    # 1) SQLAlchemy Engine (has begin)
    if hasattr(engine_or_conn, "begin") and callable(getattr(engine_or_conn, "begin")):
        with engine_or_conn.begin() as conn:
            yield conn
        return

    # 2) SQLAlchemy Connection-like (already a Connection object)
    if hasattr(engine_or_conn, "execute") and hasattr(engine_or_conn, "closed") if hasattr(engine_or_conn, "closed") else True:
        # yield as-is (no transaction control)
        yield engine_or_conn
        return

    # 3) psycopg.Connection (psycopg v3) or other DBAPI connection
    # pandas.to_sql does NOT accept raw DBAPI connections, so create a temporary SQLAlchemy Engine
    try:
        import psycopg
        is_psycopg_conn = isinstance(engine_or_conn, psycopg.Connection)
    except Exception:
        is_psycopg_conn = False

    if is_psycopg_conn:
        # prefer DATABASE_URL env var; se não existir, tentar extrair dsn do objeto
        url = DATABASE_URL
        if not url:
            try:
                # psycopg.Connection.dsn may contain connection string; fallback best-effort
                url = engine_or_conn.dsn
            except Exception:
                url = None
        if not url:
            raise ValueError("DATABASE_URL não configurado; não é possível criar Engine a partir de psycopg.Connection")

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

    raise ValueError("Objeto passado não é um Engine/Connection/DBAPI/psycopg válido")