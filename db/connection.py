# db/connection.py
# Conexão segura com Supabase (db/connection.py, .env.example)	Base de dados central; necessário para persistência e testes.

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

# Use DATABASE_URL no formato:
# postgresql://user:password@host:5432/dbname
DATABASE_URL = os.getenv("postgresql://postgres:CGUh0NTdOuv9spKp@db.jxamzozlonqsvmqmipym.supabase.co:5432/postgres")

def get_engine() -> Engine:
    """
    Retorna um SQLAlchemy Engine configurado para o DATABASE_URL.
    Usa NullPool para evitar conexões persistentes em ambientes serverless.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurado. Defina a variável de ambiente.")
    return create_engine(DATABASE_URL, poolclass=NullPool, future=True)