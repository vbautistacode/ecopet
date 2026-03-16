# db/init_db.py
from sqlalchemy import create_engine
from db.models import create_tables
import os

def init_db():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    engine = create_engine(db_url, pool_pre_ping=True)
    create_tables(engine)
    print("✅ Banco inicializado com sucesso!")

if __name__ == "__main__":
    init_db()