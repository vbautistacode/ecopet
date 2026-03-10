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
from db.connection import get_connection


def list_tables(conn) -> List[str]:
    """
    Lista tabelas visíveis no schema public (Postgres).
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name;
            """
        )
        rows = cur.fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        print("Erro ao listar tabelas (information_schema):", e)
        return []


def sample_and_nulls(conn, table: str, limit: int = 5):
    """
    Mostra amostra, contagem total e contagem de nulos (amostra até 1000 linhas).
    """
    try:
        # sample rows
        df_sample = pd.read_sql(f"SELECT * FROM public.{table} LIMIT %s", conn, params=(limit,))
        print(f"\n== {table} (rows sample {len(df_sample)})")
        if not df_sample.empty:
            print(df_sample.head(limit).to_string(index=False))
        else:
            print("(tabela vazia ou sem permissões de leitura)")

        # total rows
        total_df = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM public.{table}", conn)
        total = int(total_df.iloc[0, 0]) if not total_df.empty else 0
        print("total rows:", total)

        # null counts using a safe sample (limit 1000)
        full = pd.read_sql(f"SELECT * FROM public.{table} LIMIT %s", conn, params=(1000,))
        print("null counts (sample up to 1000 rows):")
        print(full.isna().sum().to_string())
    except Exception as e:
        print(f"Erro ao ler {table}: {e}")


def main():
    try:
        conn = get_connection()
    except Exception as e:
        print("Erro ao obter conexão com o banco:", e)
        return

    try:
        tables = list_tables(conn)
        print("Tables:", tables)
    except Exception as e:
        print("Erro ao listar tabelas:", e)
        tables = []

    expected = [
        "dre_financeiro",
        "dados_contabeis",
        "indicadores_vendas",
        "indicadores_financeiros",
        "indicadores_marketing",
        "indicadores_operacionais",
        "indicadores_clientes",
    ]

    for t in expected:
        if t in tables:
            sample_and_nulls(conn, t)
        else:
            print(f"\nTabela esperada ausente: {t}")

    try:
        conn.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()