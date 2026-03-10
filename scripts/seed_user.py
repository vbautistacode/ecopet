#!/usr/bin/env python3
"""
Seed or update a user in the active database (Postgres via Supabase or local SQLite fallback).

Usage examples:
  # Postgres (preferred)
  export STREAMDASH_DB=postgres
  export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
  python scripts/seed_user.py --username test_user --password 'Test@1234' --name "Usuário de Teste" --role admin

  # SQLite fallback (dev)
  export STREAMDASH_DB=sqlite
  python scripts/seed_user.py --db streamdash.db --username test_user --password 'Test@1234'
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

from passlib.hash import argon2

# Centralized DB connection helper
from db.connection import get_connection
import os
import sqlite3


DEFAULT_DB = "streamdash.db"


USERS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  role TEXT CHECK (role IN ('admin','viewer')) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

USERS_TABLE_POSTGRES = """
CREATE TABLE IF NOT EXISTS public.users (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'viewer' CHECK (role IN ('admin','viewer')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

UPSERT_SQLITE = """
INSERT INTO users (name, username, password_hash, role)
VALUES (?, ?, ?, ?)
ON CONFLICT(username) DO UPDATE SET
  password_hash = excluded.password_hash,
  name = excluded.name,
  role = excluded.role;
"""

UPSERT_POSTGRES = """
INSERT INTO public.users (name, username, password_hash, role)
VALUES (%s, %s, %s, %s)
ON CONFLICT (username) DO UPDATE
  SET password_hash = EXCLUDED.password_hash,
      name = EXCLUDED.name,
      role = EXCLUDED.role;
"""


def parse_args():
    p = argparse.ArgumentParser(description="Seed or update a user in DB (Argon2)")
    p.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite DB file (only used if STREAMDASH_DB=sqlite)")
    p.add_argument("--username", required=True, help="Username to create or update")
    p.add_argument("--password", required=True, help="Plaintext password for the user")
    p.add_argument("--name", default=None, help="Full name for the user")
    p.add_argument("--role", choices=["admin", "viewer"], default="viewer", help="User role")
    return p.parse_args()


def hash_password(plain: str) -> str:
    pw = "" if plain is None else str(plain).strip()
    return argon2.hash(pw)


def ensure_table_sqlite(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.executescript(USERS_TABLE_SQLITE)
    conn.commit()


def ensure_table_postgres(conn):
    cur = conn.cursor()
    cur.execute(USERS_TABLE_POSTGRES)
    conn.commit()


def seed_user_sqlite(conn: sqlite3.Connection, name: str, username: str, password_hash: str, role: str):
    cur = conn.cursor()
    cur.execute(UPSERT_SQLITE, (name, username, password_hash, role))
    conn.commit()


def seed_user_postgres(conn, name: str, username: str, password_hash: str, role: str):
    cur = conn.cursor()
    cur.execute(UPSERT_POSTGRES, (name, username, password_hash, role))
    conn.commit()


def main():
    args = parse_args()
    db_type = os.getenv("STREAMDASH_DB", "postgres").lower()

    name = args.name if args.name else args.username
    try:
        password_hash = hash_password(args.password)
    except Exception as e:
        print(f"Erro ao gerar hash da senha: {e}", file=sys.stderr)
        sys.exit(1)

    if db_type == "sqlite":
        db_path = Path(args.db)
        # ensure parent dir exists
        if not db_path.parent.exists():
            try:
                db_path.parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                print(f"Erro ao criar diretório para DB: {e}", file=sys.stderr)
                sys.exit(1)
        try:
            conn = sqlite3.connect(str(db_path))
            ensure_table_sqlite(conn)
            seed_user_sqlite(conn, name, args.username, password_hash, args.role)
            conn.close()
            print(f"Usuário seedado/atualizado no SQLite: username='{args.username}', role='{args.role}', db='{db_path}'")
        except Exception as e:
            print(f"Erro ao seedar usuário no SQLite: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        # Postgres path (Supabase)
        try:
            conn = get_connection()
        except Exception as e:
            print(f"Erro ao obter conexão com o banco Postgres: {e}", file=sys.stderr)
            sys.exit(1)

        try:
            ensure_table_postgres(conn)
            seed_user_postgres(conn, name, args.username, password_hash, args.role)
            conn.close()
            print(f"Usuário seedado/atualizado no Postgres: username='{args.username}', role='{args.role}'")
        except Exception as e:
            print(f"Erro ao seedar usuário no Postgres: {e}", file=sys.stderr)
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            sys.exit(1)


if __name__ == "__main__":
    main()