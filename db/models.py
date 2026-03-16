# db/models.py
import os
from urllib.parse import urlparse

from sqlalchemy import (
    MetaData, Table, Column, BigInteger, Text, Numeric, Date,
    TIMESTAMP, create_engine, func
)
from sqlalchemy.engine import Engine, Connection as SAConnection

# Detect DB type from DATABASE_URL if present, fallback to STREAMDASH_DB
def detect_db_type_from_url():
    db_url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_URL".upper())
    if not db_url:
        return os.getenv("STREAMDASH_DB", "sqlite").lower()
    scheme = urlparse(db_url).scheme
    if scheme.startswith("postgres"):
        return "postgres"
    if scheme.startswith("sqlite"):
        return "sqlite"
    return os.getenv("STREAMDASH_DB", "sqlite").lower()

DB_TYPE = detect_db_type_from_url()

# SQLAlchemy metadata and table definitions (portable)
metadata = MetaData()

indicadores_financeiros = Table(
    "indicadores_financeiros",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("nome", Text),
    Column("valor", Numeric),
    Column("periodo", Date)
)

users = Table(
    "users",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False),
    Column("username", Text, nullable=False, unique=True),
    Column("password_hash", Text, nullable=False),
    Column("role", Text, nullable=False, server_default="viewer"),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
)

# Adicione outras tabelas aqui...

def _is_sqlalchemy_engine(obj):
    return isinstance(obj, Engine)

def _is_sqlalchemy_connection(obj):
    return isinstance(obj, SAConnection)

def _is_dbapi_connection(obj):
    # DB-API connections normally expose .cursor()
    return hasattr(obj, "cursor") and not _is_sqlalchemy_connection(obj) and not _is_sqlalchemy_engine(obj)

def create_tables(engine_or_conn):
    """
    Create tables using SQLAlchemy metadata.
    Accepts:
      - SQLAlchemy Engine (recommended) OR
      - SQLAlchemy Connection (engine.connect()) OR
      - DB-API connection (psycopg2, psycopg). If DB-API is passed, DATABASE_URL env var must be set.
    """
    # 1) If SQLAlchemy Engine -> use directly
    if _is_sqlalchemy_engine(engine_or_conn):
        metadata.create_all(bind=engine_or_conn)
        return

    # 2) If SQLAlchemy Connection -> use directly
    if _is_sqlalchemy_connection(engine_or_conn):
        metadata.create_all(bind=engine_or_conn)
        return

    # 3) If DB-API connection (psycopg2, sqlite3, etc.) -> create Engine from DATABASE_URL
    if _is_dbapi_connection(engine_or_conn):
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL not set; cannot create SQLAlchemy engine from DB-API connection")
        engine = create_engine(db_url, pool_pre_ping=True)
        metadata.create_all(bind=engine)
        return

    # Otherwise, unsupported object
    raise RuntimeError(
        "Unsupported connection object passed to create_tables. "
        "Provide a SQLAlchemy Engine/Connection or a DB-API connection."
    )