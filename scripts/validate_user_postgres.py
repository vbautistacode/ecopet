#!/usr/bin/env python3
"""
scripts/validate_user_postgres.py

Valida username+password contra a tabela users no Postgres (Supabase).
Retorna exit code 0 se válido, 1 se inválido, 2 em erro.
Imprime JSON com resultado.
"""
import os
import sys
import json
import argparse

# hashing: passlib preferred, fallback werkzeug for dev
_HAS_PASSLIB = False
try:
    from passlib.hash import argon2, bcrypt, bcrypt_sha256
    _HAS_PASSLIB = True
except Exception:
    try:
        from werkzeug.security import check_password_hash, generate_password_hash
    except Exception:
        raise RuntimeError("Instale passlib ou werkzeug: pip install passlib argon2-cffi werkzeug")

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception as e:
    raise RuntimeError("Instale psycopg[binary]: pip install psycopg[binary]") from e

def _is_argon2_hash(h: str) -> bool:
    return isinstance(h, str) and h.startswith("$argon2")

def _is_bcrypt_hash(h: str) -> bool:
    return isinstance(h, str) and (h.startswith("$2a$") or h.startswith("$2b$") or h.startswith("$2y$"))

def _is_bcrypt_sha256_hash(h: str) -> bool:
    return isinstance(h, str) and h.startswith("$bcrypt-sha256$")

def get_conn():
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url)
    return psycopg2.connect(
        host=os.getenv("PGHOST","localhost"),
        port=os.getenv("PGPORT","5432"),
        user=os.getenv("PGUSER","postgres"),
        password=os.getenv("PGPASSWORD",""),
        dbname=os.getenv("PGDATABASE","postgres"),
    )

def verify_and_maybe_rehash(conn, user_row, plain_password):
    stored = user_row["password_hash"]
    user_id = user_row["id"]
    try:
        if _HAS_PASSLIB:
            # Argon2
            if _is_argon2_hash(stored):
                return argon2.verify(plain_password, stored)
            # bcrypt-sha256
            if _is_bcrypt_sha256_hash(stored):
                ok = bcrypt_sha256.verify(plain_password, stored)
                if ok:
                    # rehash to argon2
                    new_hash = argon2.hash(plain_password)
                    with conn.cursor() as cur:
                        cur.execute("UPDATE users SET password_hash = %s, updated_at = now() WHERE id = %s", (new_hash, user_id))
                        conn.commit()
                return ok
            # bcrypt
            if _is_bcrypt_hash(stored):
                ok = bcrypt.verify(plain_password, stored)
                if ok:
                    new_hash = argon2.hash(plain_password)
                    with conn.cursor() as cur:
                        cur.execute("UPDATE users SET password_hash = %s, updated_at = now() WHERE id = %s", (new_hash, user_id))
                        conn.commit()
                return ok
            # fallback try argon2 verify
            try:
                return argon2.verify(plain_password, stored)
            except Exception:
                return False
        else:
            # werkzeug fallback
            return check_password_hash(stored, plain_password)
    except Exception:
        return False

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--username", required=True)
    p.add_argument("--password", required=True)
    args = p.parse_args()

    try:
        conn = get_conn()
    except Exception as e:
        print(json.dumps({"ok": False, "error": f"DB connection failed: {e}"}))
        sys.exit(2)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, name, username, password_hash, role FROM users WHERE username = %s", (args.username,))
            row = cur.fetchone()
            if not row:
                print(json.dumps({"ok": False, "reason": "not_found"}))
                sys.exit(1)
            valid = verify_and_maybe_rehash(conn, row, args.password)
            if valid:
                print(json.dumps({"ok": True, "user": {"id": row["id"], "username": row["username"], "name": row["name"], "role": row["role"]}}))
                sys.exit(0)
            else:
                print(json.dumps({"ok": False, "reason": "invalid_password"}))
                sys.exit(1)
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}))
        sys.exit(2)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
