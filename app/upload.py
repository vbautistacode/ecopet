# app/pages/upload.py
# Página de Upload (app/pages/upload.py) com validação e upsert	Fluxo operacional do usuário único; garante dados limpos.
#  permitir que o usuário admin envie CSV/XLSX, valide, veja preview e persista com ON CONFLICT (idempotente).

import streamlit as st
import pandas as pd
import io
import hashlib
from datetime import datetime
from sqlalchemy import text
from db.connection import get_engine

st.set_page_config(page_title="Upload de Dados - EcoPet", layout="wide")

# ---------- Helpers ----------
def compute_file_hash(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()

def preview_dataframe(df: pd.DataFrame):
    st.dataframe(df.head(50))

def validate_and_normalize(df: pd.DataFrame, expected_cols=None):
    """
    Validação básica:
    - verifica colunas obrigatórias
    - normaliza 'mes' como string e tenta parsear datas
    Retorna (ok: bool, df_normalized: DataFrame, issues: list[str])
    """
    issues = []
    df2 = df.copy()
    expected = expected_cols or ["tenant_id", "mes"]
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
    """
    Upsert via temp table + INSERT ... ON CONFLICT DO UPDATE.
    - df: DataFrame pronto para persistir
    - key_cols: lista de colunas que compõem a chave única (ex: ['tenant_id','mes'])
    """
    tmp_table = f"tmp_{table_name}_{int(datetime.utcnow().timestamp())}"
    df_columns = [c for c in df.columns]
    if not df_columns:
        raise ValueError("DataFrame sem colunas")
    with engine.begin() as conn:
        # grava temp table
        df.to_sql(tmp_table, conn, if_exists="replace", index=False)
        cols_sql = ", ".join([f'"{c}"' for c in df_columns])
        conflict_cols = ", ".join([f'"{c}"' for c in key_cols])
        # montar SET para update (exceto keys)
        set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df_columns if c not in key_cols])
        upsert_sql = f"""
        INSERT INTO {table_name} ({cols_sql})
        SELECT {cols_sql} FROM {tmp_table}
        ON CONFLICT ({conflict_cols}) DO UPDATE
        SET {set_sql};
        """
        conn.execute(text(upsert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp_table};"))

def log_upload(engine, user: str, filename: str, file_hash: str, table: str, rows: int, status: str, message: str = None):
    sql = text("""
    INSERT INTO uploads_log (uploaded_at, uploaded_by, filename, file_hash, target_table, rows_count, status, message)
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
key_cols_map = {
    "indicadores_financeiros": ["tenant_id", "mes"],
    "dre_financeiro": ["tenant_id", "mes"],
    "dados_contabeis": ["tenant_id", "mes"],
    "indicadores_vendas": ["tenant_id", "mes"]
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

    ok, df_norm, issues = validate_and_normalize(df, expected_cols=key_cols_map[target_table])
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
                    upsert_dataframe(engine, df_norm, table_name=target_table, key_cols=key_cols_map[target_table])
                    log_upload(engine, user="admin", filename=uploaded.name, file_hash=file_hash, table=target_table, rows=len(df_norm), status="success")
                st.success("Dados gravados com sucesso")
            except Exception as e:
                try:
                    log_upload(engine, user="admin", filename=uploaded.name, file_hash=file_hash, table=target_table, rows=len(df_norm), status="failed", message=str(e))
                except Exception:
                    pass
                st.error(f"Falha ao gravar dados: {e}")