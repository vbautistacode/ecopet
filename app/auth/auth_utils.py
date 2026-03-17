# app/auth/auth_utils.py
"""
Authentication utilities for Supabase/Postgres.

- Primary hasher: Argon2 (passlib) when available.
- Fallback: werkzeug PBKDF2-SHA256 for development if passlib is missing.
- Assumes Postgres (STREAMDASH_DB=postgres) and uses db.connection.get_connection()
- Exposes: get_user_by_username, create_user, hash_password, verify_password, is_admin.
"""

import logging
import os
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Hashing backends: prefer passlib, fallback para werkzeug se necessário
_HAS_PASSLIB = False
_argon2 = None
_bcrypt = None
_bcrypt_sha256 = None
_generate_password_hash = None
_check_password_hash = None

try:
    from passlib.hash import argon2, bcrypt, bcrypt_sha256  # type: ignore
    _HAS_PASSLIB = True
    _argon2 = argon2
    _bcrypt = bcrypt
    _bcrypt_sha256 = bcrypt_sha256
except Exception:
    _HAS_PASSLIB = False
    try:
        from werkzeug.security import generate_password_hash, check_password_hash  # type: ignore
        _generate_password_hash = generate_password_hash
        _check_password_hash = check_password_hash
    except Exception:
        # Não lançar na importação; registrar e deixar o app inicializar.
        logger.error("Nenhum backend de hash disponível. Instale 'passlib' (recomendado) ou 'werkzeug'.")

# abstrações de DB
from db.connection import get_connection, get_dict_cursor
from etl.utils import connection_context

# -------------------------
# User retrieval / creation
# -------------------------
def get_user_by_username(conn, username: str) -> Optional[Dict[str, Any]]:
    """
    Retorna o usuário como dict ou None.
    conn pode ser psycopg.Connection, SQLAlchemy Engine/Connection ou None (get_connection será usado).
    """
    if conn is None:
        conn = get_connection()
    # get_dict_cursor é um context manager que retorna cursor com rows como dicts
    with get_dict_cursor(conn) as cur:
        cur.execute("SELECT * FROM public.users WHERE username = %s", (username,))
        return cur.fetchone()

def create_user(conn, name: str, username: str, password: str, role: str = "viewer") -> None:
    """
    Cria usuário. Usa connection_context para suportar Engine/Connection/psycopg.
    """
    hashed = hash_password(password)
    # usar connection_context para garantir compatibilidade com pandas/sqlalchemy/psycopg
    with connection_context(conn) as c:
        # usar parâmetros posicionais para psycopg / SQLAlchemy text binding
        c.execute(
            "INSERT INTO public.users (name, username, password_hash, role) VALUES (:name, :username, :password_hash, :role)",
            {"name": name, "username": username, "password_hash": hashed, "role": role}
        )

# -------------------------
# Hashing helpers
# -------------------------
def hash_password(password: Optional[str]) -> str:
    pw = "" if password is None else str(password).strip()
    if _HAS_PASSLIB and _argon2 is not None:
        return _argon2.hash(pw)
    if _generate_password_hash is not None:
        return _generate_password_hash(pw, method="pbkdf2:sha256", salt_length=16)
    raise RuntimeError("Nenhum backend de hash disponível. Instale 'passlib' ou 'werkzeug'.")

def _is_argon2_hash(h: str) -> bool:
    return isinstance(h, str) and h.startswith("$argon2")

def _is_bcrypt_hash(h: str) -> bool:
    return isinstance(h, str) and (h.startswith("$2a$") or h.startswith("$2b$") or h.startswith("$2y$"))

def _is_bcrypt_sha256_hash(h: str) -> bool:
    return isinstance(h, str) and h.startswith("$bcrypt-sha256$")

def _rehash_to_argon2(conn, user_id: int, plain: str) -> None:
    """
    Re-hash password to Argon2 if passlib available. Non-fatal.
    """
    if not _HAS_PASSLIB or _argon2 is None:
        return
    try:
        new_hash = _argon2.hash(plain)
        with connection_context(conn) as c:
            c.execute(
                "UPDATE public.users SET password_hash = :hash WHERE id = :id",
                {"hash": new_hash, "id": user_id}
            )
    except Exception:
        logger.exception("Falha ao re-hash para user_id=%s", user_id)
        # swallow

# -------------------------
# Verification
# -------------------------
def verify_password(plain: str, hashed: str, conn=None, user_id: Optional[int] = None) -> bool:
    if not isinstance(plain, str) or not isinstance(hashed, str):
        return False
    try:
        if _HAS_PASSLIB and _argon2 is not None:
            # Argon2 preferred
            if _is_argon2_hash(hashed):
                return _argon2.verify(plain, hashed)
            if _is_bcrypt_sha256_hash(hashed) and _bcrypt_sha256 is not None:
                ok = _bcrypt_sha256.verify(plain, hashed)
                if ok and conn is not None and user_id is not None:
                    _rehash_to_argon2(conn, user_id, plain)
                return ok
            if _is_bcrypt_hash(hashed) and _bcrypt is not None:
                ok = _bcrypt.verify(plain, hashed)
                if ok and conn is not None and user_id is not None:
                    _rehash_to_argon2(conn, user_id, plain)
                return ok
            # fallback attempt
            try:
                return _argon2.verify(plain, hashed)
            except Exception:
                return False
        else:
            if _check_password_hash is None:
                return False
            return _check_password_hash(hashed, plain)
    except Exception:
        logger.exception("Erro ao verificar senha")
        return False

# -------------------------
# Utilities
# -------------------------
def is_admin(user: Optional[Dict[str, Any]]) -> bool:
    if not user:
        return False
    role = user.get("role") if isinstance(user, dict) else user["role"]
    return role == "admin"