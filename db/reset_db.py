# db/reset_db.py
"""
Reset database script adapted for Supabase/Postgres-first architecture.

Behavior:
- If STREAMDASH_DB=sqlite (development), removes the local SQLite file if present.
- Always (re)creates tables using db.models.create_tables(conn) against the active DB.
- Attempts to run seed_db(conn) if seed_db accepts a connection, otherwise falls back to seed_db().
- Safe and idempotent for Postgres/Supabase (does not drop production DB).
"""

import os
import sys
from typing import Optional

# Centralized connection helper and schema creator
from db.connection import get_connection
from db.models import create_tables

# seed_db may accept a connection or not; import and call defensively
try:
    from db.seed_db import seed_db  # type: ignore
except Exception:
    seed_db = None  # type: ignore


def _remove_sqlite_file_if_requested() -> None:
    db_type = os.getenv("STREAMDASH_DB", "postgres").lower()
    if db_type != "sqlite":
        # In Postgres/Supabase mode we do not touch local sqlite files.
        print("STREAMDASH_DB != sqlite; skipping removal of local SQLite file.")
        return

    sqlite_path = os.getenv("SQLITE_PATH", "streamdash.db")
    if os.path.exists(sqlite_path):
        try:
            os.remove(sqlite_path)
            print(f"🗑️ Removed local SQLite file: {sqlite_path}")
        except Exception as e:
            print(f"Warning: failed to remove {sqlite_path}: {e}")
    else:
        print(f"No local SQLite file found at {sqlite_path}; nothing to remove.")


def reset_db() -> None:
    """
    Recreate schema and optionally seed data.

    Important: this does NOT drop or truncate production Postgres tables.
    It calls create_tables(conn) which should be implemented to run CREATE TABLE IF NOT EXISTS.
    """
    # Only remove local sqlite file when explicitly configured to use sqlite
    _remove_sqlite_file_if_requested()

    conn = None
    try:
        conn = get_connection()
    except Exception as e:
        print("Erro ao obter conexão com o banco:", e)
        sys.exit(1)

    try:
        # Create or migrate tables (create_tables must be idempotent)
        create_tables(conn)
        print("📦 Schema criado/confirmado com sucesso no banco ativo.")

        # Seed data if seed_db is available
        if seed_db is None:
            print("🌱 seed_db não encontrado; pulando etapa de seed.")
            return

        # Try calling seed_db with connection first, then fallback to no-arg call
        try:
            seed_db(conn)  # type: ignore
            print("🌱 Banco populado com seed_db(conn).")
        except TypeError:
            try:
                seed_db()  # type: ignore
                print("🌱 Banco populado com seed_db().")
            except Exception as e:
                print("Warning: seed_db() falhou:", e)
        except Exception as e:
            print("Warning: seed_db(conn) falhou:", e)

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    reset_db()