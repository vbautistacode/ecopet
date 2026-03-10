# app/auth/auth_utils.py
"""
Authentication utilities (robust, with fallbacks).

- Primary hasher: Argon2 (passlib) when available.
- Fallback: werkzeug PBKDF2-SHA256 for development if passlib is missing.
- Supports SQLite and PostgreSQL based on STREAMDASH_DB env var.
- Exposes: get_connection, get_user_by_username, create_user, hash_password, verify_password, is_admin.
"""

import os
from typing import Optional, Dict, Any

# Try to import passlib (preferred). If not available, use werkzeug as a dev fallback.
_HAS_PASSLIB = False
try:
    from passlib.hash import argon2, bcrypt, bcrypt_sha256  # type: ignore
    _HAS_PASSLIB = True
except Exception:
    _HAS_PASSLIB = False
    try:
        from werkzeug.security import generate_password_hash, check_password_hash  # type: ignore
    except Exception:
        # If neither passlib nor werkzeug are available, raise a clear error at import time.
        raise RuntimeError("Nenhum backend de hash disponível. Instale 'passlib' (recomendado) ou 'werkzeug'.")

DB_TYPE = os.getenv("STREAMDASH_DB", "sqlite").lower()  # "sqlite" or "postgres"

if DB_TYPE == "postgres":
    try:
        import psycopg2  # type: ignore
        from psycopg2.extras import RealDictCursor  # type: ignore
    except Exception as e:
        raise RuntimeError("psycopg2 não encontrado. Instale com: pip install psycopg[binary]") from e
elif DB_TYPE == "sqlite":
    import sqlite3  # type: ignore
else:
    raise RuntimeError(f"Unsupported DB_TYPE: {DB_TYPE}")


# -------------------------
# Database connection
# -------------------------
def get_connection():
    """
    Return a DB connection object.
    For Postgres: uses DATABASE_URL if present, otherwise PG* env vars.
    For SQLite: sqlite3 connection to SQLITE_PATH (default streamdash.db).
    """
    if DB_TYPE == "postgres":
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            # psycopg2 accepts a DSN string
            return psycopg2.connect(database_url)
        # fallback to individual env vars
        return psycopg2.connect(
            dbname=os.getenv("DB_NAME", "streamdash"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "postgres"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
        )
    else:
        path = os.getenv("SQLITE_PATH", "streamdash.db")
        return sqlite3.connect(path)


# -------------------------
# User retrieval / creation
# -------------------------
def get_user_by_username(conn, username: str) -> Optional[Dict[str, Any]]:
    """
    Return a user dict or None.
    Expected columns: id, name, username, password_hash, role
    """
    if DB_TYPE == "postgres":
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, name, username, password_hash, role FROM users WHERE username = %s", (username,))
            row = cur.fetchone()
            return dict(row) if row else None
    else:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT id, name, username, password_hash, role FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
        return dict(row) if row else None


def create_user(conn, name: str, username: str, password: str, role: str = "viewer") -> None:
    """
    Create a new user (hashes password with Argon2 if available).
    """
    hashed = hash_password(password)
    if DB_TYPE == "postgres":
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (name, username, password_hash, role) VALUES (%s, %s, %s, %s)",
                (name, username, hashed, role),
            )
        conn.commit()
    else:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (name, username, password_hash, role) VALUES (?, ?, ?, ?)",
            (name, username, hashed, role),
        )
        conn.commit()


# -------------------------
# Hashing helpers
# -------------------------
def hash_password(password: str) -> str:
    pw = "" if password is None else str(password).strip()
    if _HAS_PASSLIB:
        return argon2.hash(pw)
    # fallback: PBKDF2-SHA256 via werkzeug (para desenvolvimento)
    return generate_password_hash(pw, method="pbkdf2:sha256", salt_length=16)


def _is_argon2_hash(h: str) -> bool:
    return isinstance(h, str) and h.startswith("$argon2")


def _is_bcrypt_hash(h: str) -> bool:
    return isinstance(h, str) and (h.startswith("$2a$") or h.startswith("$2b$") or h.startswith("$2y$"))


def _is_bcrypt_sha256_hash(h: str) -> bool:
    return isinstance(h, str) and h.startswith("$bcrypt-sha256$")


def _rehash_to_argon2(conn, user_id: int, plain: str) -> None:
    """
    Re-hash the plain password with Argon2 and update the DB.
    Non-fatal: swallow exceptions to avoid blocking login.
    Only runs if passlib is available.
    """
    if not _HAS_PASSLIB:
        return
    try:
        new_hash = argon2.hash(plain)
        if DB_TYPE == "postgres":
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET password_hash = %s WHERE id = %s", (new_hash, user_id))
            conn.commit()
        else:
            cur = conn.cursor()
            cur.execute("UPDATE users SET password_hash = ? WHERE id = ?", (new_hash, user_id))
            conn.commit()
    except Exception:
        # swallow to avoid blocking login flow
        pass


# -------------------------
# Verification
# -------------------------
def verify_password(plain: str, hashed: str, conn=None, user_id: Optional[int] = None) -> bool:
    """
    Verify a plaintext password against a stored hash.
    - Supports Argon2 (preferred), bcrypt, bcrypt_sha256 when passlib is available.
    - If an old hash (bcrypt / bcrypt_sha256) is verified and conn+user_id are provided,
      re-hashes the password to Argon2 and updates the DB (only if passlib is available).
    Returns True if password matches, False otherwise.
    """
    if not isinstance(plain, str) or not isinstance(hashed, str):
        return False
    try:
        if _HAS_PASSLIB:
            # comportamento original: suporta argon2, bcrypt_sha256, bcrypt
            if _is_argon2_hash(hashed):
                return argon2.verify(plain, hashed)
            if _is_bcrypt_sha256_hash(hashed):
                ok = bcrypt_sha256.verify(plain, hashed)
                if ok and conn is not None and user_id is not None:
                    _rehash_to_argon2(conn, user_id, plain)
                return ok
            if _is_bcrypt_hash(hashed):
                ok = bcrypt.verify(plain, hashed)
                if ok and conn is not None and user_id is not None:
                    _rehash_to_argon2(conn, user_id, plain)
                return ok
            # fallback: try argon2 verify
            try:
                return argon2.verify(plain, hashed)
            except Exception:
                return False
        else:
            # fallback: werkzeug check (only for dev)
            return check_password_hash(hashed, plain)
    except Exception:
        return False


# -------------------------
# Utilities
# -------------------------
def is_admin(user: Optional[Dict[str, Any]]) -> bool:
    """
    Return True if user has role 'admin'.
    """
    if not user:
        return False
    # user may be sqlite3.Row (mapping) or dict
    role = user.get("role") if isinstance(user, dict) else user["role"]
    return role == "admin"