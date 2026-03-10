# app/main.py

import sys
import os
from typing import Dict, Optional

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import streamlit as st
import pandas as pd

from app.auth.login import show_login
from app.auth.manage_users import show_manage_users
from app.dashboards.dashboards import show_dashboard

from db.connection import get_connection
from db.init_db import init_db
from db.models import create_tables

# -----------------------------
# Configuração da aplicação
# -----------------------------
st.set_page_config(page_title="Streamdash BI", layout="wide")
st.title("📊 EcoPet - BI")


def main():
    # Garantir estado inicial
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
        st.session_state["role"] = None

    # Se não autenticado, o show_login vai interromper com st.stop()
    show_login()

    # Daqui para baixo, só roda se estiver autenticado
    st.sidebar.title("📂 Menu")

    if st.session_state["role"] == "admin":
        choice = st.sidebar.radio("Navegação", ["Dashboards", "Gestão de Usuários"])
    else:
        choice = st.sidebar.radio("Navegação", ["Dashboards"])

    if choice == "Dashboards":
        pass
    elif choice == "Gestão de Usuários":
        show_manage_users()


if __name__ == "__main__":
    main()


# -----------------------------
# Inicialização do banco (idempotente)
# -----------------------------
init_db()
conn = get_connection()
create_tables(conn)
try:
    conn.close()
except Exception:
    pass


# -----------------------------
# Sidebar — Filtros e Upload
# -----------------------------
st.sidebar.title("⚙️ Configurações")

# tenant_id removed (no longer used). Keep as optional internal variable if needed.
periodo = st.sidebar.selectbox(
    "📅 Período",
    ["(Todos)", "(Acumulado)", "2025-01", "2025-02", "2025-03"],
    index=0
)

modo = st.sidebar.radio("🔍 Modo de visualização", ["Resumido", "Detalhado"])

uploaded_files = st.sidebar.file_uploader(
    "📁 Upload de dados (.csv ou .xlsx)", type=["csv", "xlsx"], accept_multiple_files=True
)

if st.sidebar.button("🔄 Atualizar dados"):
    st.experimental_rerun()


# -----------------------------
# Upload handling (transform + write)
# -----------------------------
if uploaded_files:
    from etl.loaders import load_csv, load_excel
    from etl.transformers import (
        transform_finance, transform_sales, transform_ops, transform_marketing, transform_clients
    )
    from etl.writer import (
        write_finance, write_sales, write_ops, write_marketing, write_clients
    )

    conn = get_connection()
    try:
        for file in uploaded_files:
            df_raw = load_csv(file) if file.name.lower().endswith(".csv") else load_excel(file)

            # Transformar (no tenant concept anymore)
            fin = transform_finance(df_raw)
            sales = transform_sales(df_raw)
            ops = transform_ops(df_raw)
            mkt = transform_marketing(df_raw)
            clients = transform_clients(df_raw)

            # write_* now accept optional tenant_id; call without tenant_id
            if fin is not None and not fin.empty:
                write_finance(conn, fin)
            if sales is not None and not sales.empty:
                write_sales(conn, sales)
            if ops is not None and not ops.empty:
                write_ops(conn, ops)
            if mkt is not None and not mkt.empty:
                write_marketing(conn, mkt)
            if clients is not None and not clients.empty:
                write_clients(conn, clients)

        st.sidebar.success("✅ Dados carregados com sucesso!")
    except Exception as e:
        st.sidebar.error(f"Erro ao processar uploads: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# -----------------------------
# Função utilitária para leitura segura das tabelas (cacheada)
# -----------------------------
@st.cache_data(ttl=60)
def fetch_all_tables() -> Dict[str, pd.DataFrame]:
    conn = get_connection()
    try:
        # Read full tables (no tenant filter). Qualify schema for Postgres.
        finance = pd.read_sql("SELECT * FROM public.indicadores_financeiros", conn)
        dre = pd.read_sql("SELECT * FROM public.dre_financeiro", conn)
        vendas = pd.read_sql("SELECT * FROM public.indicadores_vendas", conn)
        oper = pd.read_sql("SELECT * FROM public.indicadores_operacionais", conn)
        mkt = pd.read_sql("SELECT * FROM public.indicadores_marketing", conn)
        clientes = pd.read_sql("SELECT * FROM public.indicadores_clientes", conn)
        cont = pd.read_sql("SELECT * FROM public.dados_contabeis", conn)

        return {
            "financeiros": finance,
            "dre": dre,
            "vendas": vendas,
            "operacionais": oper,
            "marketing": mkt,
            "clientes": clientes,
            "contabeis": cont
        }
    except Exception as e:
        # Surface error in Streamlit UI and return empty frames to keep app running
        st.error(f"Erro ao buscar dados do banco: {e}")
        return {
            "financeiros": pd.DataFrame(),
            "dre": pd.DataFrame(),
            "vendas": pd.DataFrame(),
            "operacionais": pd.DataFrame(),
            "marketing": pd.DataFrame(),
            "clientes": pd.DataFrame(),
            "contabeis": pd.DataFrame()
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass

# -----------------------------
# Carregar dados e exibir dashboard
# -----------------------------
dfs = fetch_all_tables()

# Chamar o dashboard sem tenant_id (pass None). show_dashboard é defensivo e só filtrará se a coluna existir.
show_dashboard(dfs, tenant_id=None, periodo=periodo, modo=modo)