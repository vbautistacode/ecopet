# apply_sql.py
"""
Aplica create_tables.sql no banco Postgres de forma robusta,
executando cada statement separadamente.
Uso:
  export DATABASE_URL="postgresql+psycopg2://USER:PASS@HOST:5432/DBNAME"
  python apply_sql.py
"""

"""
Executa create_tables.sql statement por statement, confirmando cada um separadamente.
Uso:
  export DATABASE_URL="postgresql+psycopg2://USER:PASS@HOST:5432/DBNAME"
  python apply_sql_individual.py
No Windows PowerShell:
  $env:DATABASE_URL="postgresql+psycopg2://USER:PASSWORD@HOST:5432/DBNAME"
  python apply_sql_individual.py
"""

import os
import sys
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("Defina DATABASE_URL no ambiente (ex: postgresql+psycopg2://user:pass@host:5432/dbname)")
    sys.exit(1)

SQL_FILE = "create_tables.sql"

def split_sql(sql_text: str):
    try:
        import sqlparse
        statements = [s.strip() for s in sqlparse.split(sql_text) if s.strip()]
        return statements
    except Exception:
        parts = []
        for part in sql_text.split(";"):
            p = part.strip()
            if not p:
                continue
            lines = [ln for ln in p.splitlines() if not ln.strip().startswith("--")]
            joined = "\n".join(lines).strip()
            if joined:
                parts.append(joined)
        return parts

def main():
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql_text = f.read()

    statements = split_sql(sql_text)
    if not statements:
        print("Nenhum statement SQL encontrado em", SQL_FILE)
        return

    engine = create_engine(DATABASE_URL)
    print(f"Conectando a {DATABASE_URL.split('@')[-1]} ...")

    # Executa cada statement em sua própria transação (commit por statement)
    for i, stmt in enumerate(statements, start=1):
        try:
            print(f"[{i}/{len(statements)}] Executando statement (primeiros 120 chars):\n{stmt[:120]!r}\n")
            # cada with engine.begin() cria e commita uma transação separada
            with engine.begin() as conn:
                conn.execute(text(stmt))
            print(f"[{i}] OK\n")
        except Exception as e:
            print(f"[{i}] ERRO ao executar statement: {e}\n")
            # não abortar automaticamente: interrompe para você inspecionar
            raise

    print("Todos os statements executados (ou até o primeiro erro).")

if __name__ == "__main__":
    main()