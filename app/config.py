# app/config.py
"""
Configurações globais de banco para o projeto.
Compatível com arquitetura atual (Supabase / Postgres).
"""

import os
from typing import Optional

# Tipo de banco esperado: "postgres" (produção) ou "sqlite" (fallback local)
DB_TYPE: str = os.getenv("STREAMDASH_DB", "postgres").lower()

# Preferência: DATABASE_URL (DSN). Se não existir, o código pode usar variáveis PG_* individuais.
DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")

# Variáveis individuais como fallback (úteis em alguns ambientes)
DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: str = os.getenv("DB_PORT", "5432")
DB_USER: str = os.getenv("DB_USER", "postgres")
DB_PASS: str = os.getenv("DB_PASS", "")
DB_NAME: str = os.getenv("DB_NAME", "postgres")

# Schema público por padrão (ajuste se usar schemas customizados)
DB_SCHEMA: str = os.getenv("DB_SCHEMA", "public")

def require_postgres_config() -> None:
    """
    Validação mínima: chama-se antes de abrir conexão em runtime para garantir que
    as variáveis necessárias estejam definidas quando DB_TYPE=postgres.
    """
    if DB_TYPE != "postgres":
        return
    if not DATABASE_URL and not (DB_HOST and DB_USER and DB_NAME):
        raise RuntimeError(
            "STREAMDASH_DB=postgres mas DATABASE_URL ou variáveis PG_* não estão configuradas."
        )