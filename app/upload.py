# app/upload.py
# Página de Upload com validação e upsert — fluxo operacional do usuário único.

import logging
from typing import List
import pandas as pd
from sqlalchemy import text

from etl.utils import connection_context, _safe_ident

logger = logging.getLogger(__name__)


def upsert_dataframe(engine, df: pd.DataFrame, table_name: str, key_cols: List[str]):
    """
    Upsert DataFrame into public."{table_name}" using a temporary table.
    engine may be Engine, Connection, or psycopg.Connection; connection_context handles compatibility.
    """
    if df is None or df.empty:
        logger.info("No rows to upsert for %s", table_name)
        return

    # sanitize identifiers
    _safe_ident(table_name)
    for c in df.columns:
        _safe_ident(c)
    for k in key_cols:
        _safe_ident(k)

    tmp_table = f"tmp_{table_name}_{int(pd.Timestamp.utcnow().timestamp())}"
    df_columns = list(df.columns)
    cols_sql = ", ".join([f'"{c}"' for c in df_columns])
    conflict_cols = ", ".join([f'"{c}"' for c in key_cols]) if key_cols else ""
    non_key_cols = [c for c in df_columns if c not in key_cols]
    set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols]) if non_key_cols else ""

    qualified_table = f'public."{table_name}"'
    qualified_tmp = f'public."{tmp_table}"'

    try:
        with connection_context(engine) as conn:
            # create/replace temp table in public schema
            df.to_sql(tmp_table, conn, if_exists="replace", index=False, schema="public", method="multi")
            if conflict_cols:
                upsert_sql = f"""
                INSERT INTO {qualified_table} ({cols_sql})
                SELECT {cols_sql} FROM {qualified_tmp}
                ON CONFLICT ({conflict_cols}) DO UPDATE
                SET {set_sql};
                """
            else:
                upsert_sql = f"""
                INSERT INTO {qualified_table} ({cols_sql})
                SELECT {cols_sql} FROM {qualified_tmp};
                """
            conn.execute(text(upsert_sql))
            # try to drop tmp table (some drivers may auto-drop temp tables)
            try:
                conn.execute(text(f'DROP TABLE IF EXISTS {qualified_tmp};'))
            except Exception:
                logger.debug("Could not drop tmp table %s (may be temp)", tmp_table)
        logger.info("Upserted %d rows into %s", len(df), table_name)
    except Exception:
        logger.exception("Erro no upsert para %s", table_name)
        raise