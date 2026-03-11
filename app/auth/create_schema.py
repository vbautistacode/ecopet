# app/auth/create_schema.py
"""
Create users table in Supabase/Postgres.

Usage:
  export STREAMDASH_DB=postgres
  export DATABASE_URL="postgresql://postgres:CGUh0NTdOuv9spKp@db.jxamzozlonqsvmqmipym.supabase.co:5432/postgres"
  python app/auth/create_schema.py
"""

from db.connection import get_connection

def create_users_table():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.users (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'viewer' CHECK (role IN ('admin','viewer')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        # optional: index on username for faster lookups (unique already creates index)
        conn.commit()
        print("Tabela 'public.users' criada/confirmada com sucesso.")
    except Exception as e:
        print("Erro ao criar tabela 'users':", e)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    create_users_table()