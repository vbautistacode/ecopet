# etl/utils.py
import re
import os
import uuid
import hashlib
import csv
import contextlib
import logging
from typing import Any, Dict, Tuple
from datetime import datetime
from sqlalchemy import create_engine
from typing import Optional

# -------------------------
# Identifiers / helpers
# -------------------------
_identifier_re = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

def _safe_ident(name: str) -> str:
    if not isinstance(name, str) or not _identifier_re.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'

def _qualify(schema: str, name: str) -> str:
    return f'{_safe_ident(schema)}.{_safe_ident(name)}'

# -------------------------
# Logging helper
# -------------------------
def setup_logger(name: str = __name__, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a configured logger for the ETL modules.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger

# -------------------------
# Batch id / hashing / mapping
# -------------------------
def generate_batch_id(prefix: str = "") -> str:
    """
    Generate a short unique batch id: prefix-YYYYmmdd-HHMMSS-uuid4short
    """
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    short = uuid.uuid4().hex[:8]
    return f"{prefix}{ts}-{short}"

def file_hash(path: str, algo: str = "sha256") -> str:
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def load_mapping(path: str) -> Tuple[list, Dict[str, str]]:
    """
    Load a simple CSV mapping file with columns: source,target
    Returns (rows, mapping_dict)
    """
    rows = []
    mapping = {}
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            rows.append(r)
            src = r.get("source") or r.get("orig") or r.get("from")
            tgt = r.get("target") or r.get("dest") or r.get("to")
            if src and tgt:
                mapping[src.strip()] = tgt.strip()
    return rows, mapping

# -------------------------
# Connection context (Engine / Connection / psycopg)
# -------------------------
@contextlib.contextmanager
def connection_context(engine_or_conn: Any):
    """
    Yields an object usable for .execute(...) and accepted by pandas.to_sql/read_sql.
    Accepts:
      - SQLAlchemy Engine (has .begin())
      - SQLAlchemy Connection-like (has .execute)
      - psycopg.Connection (psycopg v3) or raw DBAPI connection (cursor/commit)
    If a psycopg.Connection is provided and DATABASE_URL is set, a temporary SQLAlchemy Engine
    is created from DATABASE_URL to provide a SQLAlchemy Connection for pandas.to_sql/read_sql.
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
                url = engine_or_conn.dsn
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

# -------------------------
# Convenience: get_engine from env
# -------------------------
def get_engine_from_env():
    """
    Create SQLAlchemy engine from DATABASE_URL env var.
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not configured")
    return create_engine(url, pool_pre_ping=True)