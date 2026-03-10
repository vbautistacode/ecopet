# etl/writer.py
"""
ETL Writer — funções de escrita compatíveis com Postgres (psycopg2) e SQLAlchemy engines.

- tenant_id é opcional; se fornecido e a coluna não existir, será adicionada.
- Suporta psycopg2 DB-API (execute_values) e SQLAlchemy engine (pandas.to_sql).
- Insere em public.<table> por padrão; ajuste se usar outro schema.
"""

from typing import Optional
import pandas as pd

# optional imports
try:
    import psycopg2  # type: ignore
    import psycopg2.extras as pg_extras  # type: ignore
except Exception:
    psycopg2 = None
    pg_extras = None


def _ensure_tenant_column(df: pd.DataFrame, tenant_id: Optional[str]) -> pd.DataFrame:
    if tenant_id is None:
        return df
    if "tenant_id" in df.columns:
        return df
    df = df.copy()
    df["tenant_id"] = tenant_id
    return df


def _bulk_insert_psycopg2(conn, table: str, df: pd.DataFrame):
    if df.empty:
        return
    cols = list(df.columns)
    values = df.values.tolist()
    placeholders = "(" + ",".join(["%s"] * len(cols)) + ")"
    insert_sql = f"INSERT INTO public.{table} ({', '.join(cols)}) VALUES %s"
    cur = conn.cursor()
    try:
        if pg_extras is None:
            executemany_sql = f"INSERT INTO public.{table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
            cur.executemany(executemany_sql, values)
        else:
            pg_extras.execute_values(cur, insert_sql, values, template=placeholders)
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _to_sql_via_pandas(conn, table: str, df: pd.DataFrame):
    if df.empty:
        return
    try:
        df.to_sql(table, conn, if_exists="append", index=False)
    except Exception as e:
        raise RuntimeError(f"pandas.to_sql failed for table {table}: {e}") from e


def _write_table(conn, table: str, df: pd.DataFrame):
    if df.empty:
        return
    # DB-API (psycopg2) path
    if hasattr(conn, "cursor"):
        if psycopg2 is None:
            raise RuntimeError("psycopg2 não disponível no ambiente para usar conexão DB-API.")
        _bulk_insert_psycopg2(conn, table, df)
        return
    # SQLAlchemy / pandas path
    _to_sql_via_pandas(conn, table, df)


def write_finance(conn, df: pd.DataFrame, tenant_id: Optional[str] = None):
    df2 = _ensure_tenant_column(df, tenant_id)
    _write_table(conn, "indicadores_financeiros", df2)


def write_sales(conn, df: pd.DataFrame, tenant_id: Optional[str] = None):
    df2 = _ensure_tenant_column(df, tenant_id)
    _write_table(conn, "indicadores_vendas", df2)


def write_ops(conn, df: pd.DataFrame, tenant_id: Optional[str] = None):
    df2 = _ensure_tenant_column(df, tenant_id)
    _write_table(conn, "indicadores_operacionais", df2)


def write_marketing(conn, df: pd.DataFrame, tenant_id: Optional[str] = None):
    df2 = _ensure_tenant_column(df, tenant_id)
    _write_table(conn, "indicadores_marketing", df2)


def write_clients(conn, df: pd.DataFrame, tenant_id: Optional[str] = None):
    df2 = _ensure_tenant_column(df, tenant_id)
    _write_table(conn, "indicadores_clientes", df2)
