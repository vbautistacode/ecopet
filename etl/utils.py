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
from sqlalchemy.engine import Engine, Connection
from typing import Tuple, Dict, List, Optional, Any

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
    Context manager que aceita Engine, SQLAlchemy Connection ou raw DBAPI connection.
    Retorna um objeto que pode ser usado para executar SQL (conn.execute(...)).
    Garante commit/rollback quando aplicável.
    """
    # SQLAlchemy Engine (has begin)
    if hasattr(engine_or_conn, "begin") and callable(getattr(engine_or_conn, "begin")):
        with engine_or_conn.begin() as conn:
            yield conn
        return

    # SQLAlchemy Connection (some environments expose Connection object with begin)
    if hasattr(engine_or_conn, "connection") and callable(getattr(engine_or_conn, "connection")):
        # treat as Engine-like
        with engine_or_conn.connection() as conn:
            yield conn
        return

    # Raw DBAPI connection (has cursor, commit, rollback)
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

    # Fallback: object that supports execute but no transaction control
    if hasattr(engine_or_conn, "execute"):
        # no transaction semantics available; yield as-is
        yield engine_or_conn
        return

    raise ValueError("Objeto passado não é um Engine/Connection válido")