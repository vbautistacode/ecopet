# check_db.py
"""
Verificador de esquema e amostras — compatível com Postgres / Supabase.

Uso esperado:
  export STREAMDASH_DB=postgres
  export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
  python check_db.py
"""

from typing import List
import pandas as pd
import logging
from db.connection import get_engine
from etl.utils import connection_context, _safe_ident

logger = logging.getLogger(__name__)

def list_tables(engine) -> List[str]:
    try:
        with connection_context(engine) as conn:
            res = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name;")
            return [row[0] for row in res]
    except Exception as e:
        logger.exception("Erro ao listar tabelas")
        return []

def sample_and_nulls(engine, table: str, limit: int = 5):
    # validar identificador
    _safe_ident(table)
    try:
        with connection_context(engine) as conn:
            df_sample = pd.read_sql(f"SELECT * FROM public.{table} LIMIT {int(limit)}", con=conn)
            print(f"\n== {table} (rows sample {len(df_sample)})")
            if not df_sample.empty:
                print(df_sample.head(limit).to_string(index=False))
            else:
                print("(tabela vazia ou sem permissões de leitura)")

            total_df = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM public.{table}", con=conn)
            total = int(total_df.iloc[0, 0]) if not total_df.empty else 0
            print("total rows:", total)

            full = pd.read_sql(f"SELECT * FROM public.{table} LIMIT 1000", con=conn)
            print("null counts (sample up to 1000 rows):")
            print(full.isna().sum().to_string())
    except Exception as e:
        logger.exception("Erro ao ler %s", table)

def main():
    try:
        engine = get_engine()
    except Exception as e:
        print("Erro ao obter engine:", e)
        return

    try:
        tables = list_tables(engine)
        print("Tables:", tables)
    except Exception as e:
        logger.exception("Erro ao listar tabelas")
        tables = []

    expected = [ ... ]

    for t in expected:
        if t in tables:
            sample_and_nulls(engine, t)
        else:
            print(f"\nTabela esperada ausente: {t}")

    try:
        engine.dispose()
    except Exception:
        pass