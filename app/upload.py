# app/upload.py
# Página de Upload com validação e upsert — fluxo operacional do usuário único.

import streamlit as st
import pandas as pd
import io
import hashlib
from datetime import datetime
from sqlalchemy import text
from db.connection import get_engine
from etl.utils import _safe_ident, _qualify

st.set_page_config(page_title="Upload de Dados - EcoPet", layout="wide")

# ---------- Helpers ----------
def compute_file_hash(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()

def preview_dataframe(df: pd.DataFrame):
    st.dataframe(df.head(50))

def validate_and_normalize(df: pd.DataFrame, expected_cols=None):
    """
    Validação básica:
    - verifica colunas obrigatórias (se fornecidas)
    - normaliza 'mes' como string e tenta parsear datas
    Retorna (ok: bool, df_normalized: DataFrame, issues: list[str])
    """
    issues = []
    df2 = df.copy()
    expected = expected_cols or []
    for c in expected:
        if c not in df2.columns:
            issues.append(f"Coluna obrigatória ausente: {c}")
    if "mes" in df2.columns:
        df2["mes"] = df2["mes"].astype(str).str.strip()
        parsed = pd.to_datetime(df2["mes"].astype(str), errors="coerce")
        if parsed.isna().any():
            issues.append("Algumas linhas têm 'mes' inválido; use YYYY-MM ou YYYY-MM-DD")
    # remover linhas totalmente vazias
    df2 = df2.dropna(how="all")
    return (len(issues) == 0), df2, issues

def upsert_dataframe(engine, df: pd.DataFrame, table_name: str, key_cols: list):
    tmp_table = f"tmp_{table_name}_{int(datetime.utcnow().timestamp())}"
    df_columns = [c for c in df.columns]
    if not df_columns:
        raise ValueError("DataFrame sem colunas")

    # sanitize column names
    for c in df_columns:
        _ = _safe_ident(c)
    for k in key_cols:
        _ = _safe_ident(k)

    qualified_table = f'public."{table_name}"'
    qualified_tmp = f'public."{tmp_table}"'

    cols_sql = ", ".join([_safe_ident(c) for c in df_columns])
    conflict_cols = ", ".join([_safe_ident(c) for c in key_cols]) if key_cols else ""
    non_key_cols = [c for c in df_columns if c not in key_cols]
    set_sql = ", ".join([f'{_safe_ident(c)} = EXCLUDED.{_safe_ident(c)}' for c in non_key_cols]) if non_key_cols else ""

    with engine.begin() as conn:
        # grava temp table no schema public explicitamente
        df.to_sql(tmp_table, conn, if_exists="replace", index=False, schema="public")
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
        conn.execute(text(f'DROP TABLE IF EXISTS {qualified_tmp};'))

def log_upload(engine, user: str, filename: str, file_hash: str, table: str, rows: int, status: str, message: str = None):
    sql = text("""
    INSERT INTO public.uploads_log (uploaded_at, uploaded_by, filename, file_hash, target_table, rows_count, status, message)
    VALUES (:uploaded_at, :uploaded_by, :filename, :file_hash, :target_table, :rows_count, :status, :message);
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "uploaded_at": datetime.utcnow(),
            "uploaded_by": user,
            "filename": filename,
            "file_hash": file_hash,
            "target_table": table,
            "rows_count": rows,
            "status": status,
            "message": message
        })

# ---------- UI ----------
st.title("Upload de dados - EcoPet")
st.markdown("Envie um arquivo CSV ou XLSX. O arquivo será validado e persistido no banco (Supabase/Postgres).")

uploaded = st.file_uploader("Selecione CSV ou XLSX", type=["csv", "xlsx"], accept_multiple_files=False)
target_table = st.selectbox("Tabela destino", ["indicadores_financeiros", "dre_financeiro", "dados_contabeis", "indicadores_vendas"], index=0)

# key_cols_map agora não exige tenant_id; ajuste conforme sua chave única real
key_cols_map = {
    "indicadores_financeiros": ["mes"],
    "dre_financeiro": ["mes"],
    "dados_contabeis": ["mes"],
    "indicadores_vendas": ["mes"]
}

if uploaded:
    raw = uploaded.read()
    file_hash = compute_file_hash(raw)
    try:
        if uploaded.name.lower().endswith(".csv"):
            df = pd.read_csv(io.BytesIO(raw))
        else:
            df = pd.read_excel(io.BytesIO(raw))
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")
        st.stop()

    st.subheader("Preview")
    preview_dataframe(df)

    expected_cols = key_cols_map.get(target_table, [])
    ok, df_norm, issues = validate_and_normalize(df, expected_cols=expected_cols)
    if not ok:
        st.warning("Problemas detectados:")
        for i in issues:
            st.write("-", i)
    else:
        st.success("Validação básica OK")
        if st.button("Confirmar upload e persistir"):
            engine = get_engine()
            try:
                with st.spinner("Gravando no banco..."):
                    upsert_dataframe(engine, df_norm, table_name=target_table, key_cols=key_cols_map.get(target_table, []))
                    log_upload(engine, user="admin", filename=uploaded.name, file_hash=file_hash, table=target_table, rows=len(df_norm), status="success")
                st.success("Dados gravados com sucesso")
            except Exception as e:
                try:
                    log_upload(engine, user="admin", filename=uploaded.name, file_hash=file_hash, table=target_table, rows=len(df_norm), status="failed", message=str(e))
                except Exception:
                    pass
                st.error(f"Falha ao gravar dados: {e}")