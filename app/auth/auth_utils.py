# app/auth/auth_utils.py
"""
Authentication utilities for Supabase/Postgres.

- Primary hasher: Argon2 (passlib) when available.
- Fallback: werkzeug PBKDF2-SHA256 for development if passlib is missing.
- Assumes Postgres (STREAMDASH_DB=postgres) and uses db.connection.get_connection()
- Exposes: get_user_by_username, create_user, hash_password, verify_password, is_admin.
"""

import os
from typing import Optional, Dict, Any

# Hashing backends
_HAS_PASSLIB = False
try:
    from passlib.hash import argon2, bcrypt, bcrypt_sha256  # type: ignore
    _HAS_PASSLIB = True
except Exception:
    _HAS_PASSLIB = False
    try:
        from werkzeug.security import generate_password_hash, check_password_hash  # type: ignore
    except Exception:
        raise RuntimeError("Nenhum backend de hash disponível. Instale 'passlib' (recomendado) ou 'werkzeug'.")

# Use centralized connection helper (expects psycopg2-based connection)
from db.connection import get_connection

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import RealDictCursor  # type: ignore
except Exception:
    # get_connection will raise a clearer error if psycopg2 is missing; keep import error explicit here
    raise RuntimeError("psycopg2 é necessário. Instale com: pip install psycopg[binary]")


# -------------------------
# User retrieval / creation
# -------------------------
def get_user_by_username(conn, username: str) -> Optional[Dict[str, Any]]:
    """
    Return a user dict or None.
    Expected columns: id, name, username, password_hash, role
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT id, name, username, password_hash, role FROM public.users WHERE username = %s",
            (username,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def create_user(conn, name: str, username: str, password: str, role: str = "viewer") -> None:
    """
    Create a new user (hashes password with Argon2 if available).
    """
    hashed = hash_password(password)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public.users (name, username, password_hash, role) VALUES (%s, %s, %s, %s)",
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
        with conn.cursor() as cur:
            cur.execute("UPDATE public.users SET password_hash = %s WHERE id = %s", (new_hash, user_id))
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
    role = user.get("role") if isinstance(user, dict) else user["role"]
    return role == "admin"