#!/usr/bin/env python3
"""
scripts/create_user_postgres.py

Cria as tabelas mínimas (users, uploads_log) em um banco Postgres e insere/atualiza um usuário.

Uso:
  export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
  python scripts/create_user_postgres.py --username test_user --password secret123 --name "Test User" --role admin

Ou forneça as variáveis PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE.

Observações:
- Recomendado: usar DATABASE_URL apontando para o Supabase Postgres.
- Para produção, instale passlib + argon2-cffi para usar Argon2.
"""
import os
import argparse
import sys
from typing import Optional

# hashing: try passlib first
_HAS_PASSLIB = False
try:
    from passlib.hash import argon2
    _HAS_PASSLIB = True
except Exception:
    try:
        from werkzeug.security import generate_password_hash
    except Exception:
        raise RuntimeError("Nenhum backend de hash disponível. Instale 'passlib' ou 'werkzeug'.")

def hash_password(password: str) -> str:
    pw = "" if password is None else str(password)
    if _HAS_PASSLIB:
        return argon2.hash(pw)
    return generate_password_hash(pw, method="pbkdf2:sha256", salt_length=16)

# psycopg2 import
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception as e:
    raise RuntimeError("psycopg2 não encontrado. Instale com: pip install psycopg[binary]") from e

def get_connection():
    """
    Retorna uma conexão psycopg2.
    Usa DATABASE_URL se definido, caso contrário usa PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE.
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url)
    # fallback para variáveis separadas
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        dbname=os.getenv("PGDATABASE", "postgres"),
    )

# DDL para Postgres
DDL_USERS = """
CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'viewer',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

DDL_UPLOADS_LOG = """
CREATE TABLE IF NOT EXISTS uploads_log (
  id BIGSERIAL PRIMARY KEY,
  uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  uploaded_by TEXT,
  filename TEXT,
  file_hash TEXT,
  target_table TEXT,
  rows_count INTEGER,
  status TEXT,
  message TEXT,
  metadata JSONB
);
"""

def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL_USERS)
        cur.execute(DDL_UPLOADS_LOG)
        conn.commit()

def upsert_user(conn, name: str, username: str, password: str, role: str):
    hashed = hash_password(password)
    with conn.cursor() as cur:
        # tenta atualizar; se não existir, insere
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE users
                SET name = %s, password_hash = %s, role = %s, updated_at = now()
                WHERE username = %s
                """,
                (name, hashed, role, username),
            )
            print(f"Usuário existente atualizado: {username}")
        else:
            cur.execute(
                """
                INSERT INTO users (name, username, password_hash, role)
                VALUES (%s, %s, %s, %s)
                """,
                (name, username, hashed, role),
            )
            print(f"Usuário criado: {username}")
        conn.commit()

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--username", required=True, help="Nome de usuário (login)")
    p.add_argument("--password", required=True, help="Senha em texto plano")
    p.add_argument("--name", default="Admin", help="Nome completo do usuário")
    p.add_argument("--role", default="admin", help="Role do usuário (admin/viewer)")
    p.add_argument("--db-url", default=None, help="Opcional: DATABASE_URL para usar apenas neste comando")
    return p.parse_args()

def main():
    args = parse_args()
    # opcionalmente sobrescreve DATABASE_URL apenas para esta execução
    if args.db_url:
        os.environ["DATABASE_URL"] = args.db_url

    try:
        conn = get_connection()
    except Exception as e:
        print("Falha ao conectar ao banco:", e, file=sys.stderr)
        sys.exit(1)

    try:
        ensure_tables(conn)
        upsert_user(conn, args.name, args.username, args.password, args.role)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
