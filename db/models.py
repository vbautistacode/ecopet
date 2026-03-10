# db/models.py
import os

DB_TYPE = os.getenv("STREAMDASH_DB", "sqlite").lower()

def create_tables(conn):
    cursor = conn.cursor()

    if DB_TYPE == "postgres":
        # Postgres-compatible DDL
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS indicadores_financeiros (
            id BIGSERIAL PRIMARY KEY,
            nome TEXT,
            valor NUMERIC,
            periodo DATE
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'viewer',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)
        # outros CREATE TABLEs para Postgres...
    else:
        # SQLite-compatible DDL
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS indicadores_financeiros (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT,
            valor REAL,
            periodo TEXT
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'viewer'
        );
        """)
        # outros CREATE TABLEs para SQLite...

    conn.commit()
