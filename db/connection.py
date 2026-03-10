# db/connection.py
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

# Espera-se que DATABASE_URL esteja definida nas variáveis de ambiente
# Exemplo: postgres://user:password@host:5432/dbname
DATABASE_URL = os.getenv("DATABASE_URL")

def get_engine() -> Engine:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurado. Defina a variável de ambiente.")
    # NullPool evita conexões persistentes em ambientes serverless/Streamlit Cloud
    return create_engine(DATABASE_URL, poolclass=NullPool, future=True)