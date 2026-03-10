# app/auth/auth_utils.py
"""
Authentication utilities (refactored).

- Primary hasher: Argon2 (passlib).
- Supports legacy bcrypt / bcrypt_sha256 verification and optional rehash-to-Argon2 on successful login.
- Supports SQLite and PostgreSQL based on STREAMDASH_DB env var.
- Exposes: get_connection, get_user_by_username, create_user, hash_password, verify_password, is_admin.
"""

import os
from typing import Optional, Dict, Any

from passlib.hash import argon2, bcrypt, bcrypt_sha256

DB_TYPE = os.getenv("STREAMDASH_DB", "sqlite").lower()  # "sqlite" or "postgres"

if DB_TYPE == "postgres":
    import psycopg2
    from psycopg2.extras import RealDictCursor
elif DB_TYPE == "sqlite":
    import sqlite3
else:
    raise RuntimeError(f"Unsupported DB_TYPE: {DB_TYPE}")


# -------------------------
# Database connection
# -------------------------
def get_connection():
    """
    Return a DB connection object.
    For Postgres: psycopg2 connection.
    For SQLite: sqlite3 connection.
    """
    if DB_TYPE == "postgres":
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
    Create a new user (hashes password with Argon2).
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
    """
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
        pass


# -------------------------
# Verification
# -------------------------
def verify_password(plain: str, hashed: str, conn=None, user_id: Optional[int] = None) -> bool:
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