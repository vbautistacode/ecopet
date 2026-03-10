# app/main.py

import sys, os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import streamlit as st
# -----------------------------
# Configuração da aplicação
# -----------------------------
st.set_page_config(page_title="Streamdash BI", layout="wide")
st.title("📊 EcoPet - BI")

from app.auth.login import show_login
from app.auth.manage_users import show_manage_users

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
        # Chame funções de dashboards aqui (nunca em top-level de outros arquivos)
        # ex.: render_financeiro(), render_vendas()
    elif choice == "Gestão de Usuários":
        show_manage_users()

if __name__ == "__main__":
    main()

import os
import sys
from typing import Dict, Optional

# tornar o package importável quando executado via `streamlit run app/main.py`
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

import pandas as pd

from db.connection import get_connection
from db.init_db import init_db
from db.models import create_tables
from app.dashboards.dashboards import show_dashboard

# -----------------------------
# Inicialização do banco (idempotente)
# -----------------------------
init_db()
conn = get_connection()
create_tables(conn)
conn.close()

# -----------------------------
# Sidebar — Filtros e Upload
# -----------------------------
st.sidebar.title("⚙️ Configurações")

tenant_id = st.sidebar.text_input("Cliente (tenant_id)", "clienteA")
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

            # Transformar e gravar com tenant_id
            fin = transform_finance(df_raw)
            sales = transform_sales(df_raw)
            ops = transform_ops(df_raw)
            mkt = transform_marketing(df_raw)
            clients = transform_clients(df_raw)

            if fin is not None and not fin.empty:
                write_finance(conn, fin, tenant_id)
            if sales is not None and not sales.empty:
                write_sales(conn, sales, tenant_id)
            if ops is not None and not ops.empty:
                write_ops(conn, ops, tenant_id)
            if mkt is not None and not mkt.empty:
                write_marketing(conn, mkt, tenant_id)
            if clients is not None and not clients.empty:
                write_clients(conn, clients, tenant_id)

        st.sidebar.success("✅ Dados carregados com sucesso!")
    except Exception as e:
        st.sidebar.error(f"Erro ao processar uploads: {e}")
    finally:
        conn.close()

# -----------------------------
# Função utilitária para leitura segura das tabelas (cacheada)
# -----------------------------
@st.cache_data(ttl=60)
def fetch_tables_for_tenant(tenant: str) -> Dict[str, pd.DataFrame]:
    conn = get_connection()
    try:
        q = "SELECT * FROM indicadores_financeiros WHERE tenant_id = ?"
        finance = pd.read_sql(q, conn, params=(tenant,))

        q = "SELECT * FROM dre_financeiro WHERE tenant_id = ?"
        dre = pd.read_sql(q, conn, params=(tenant,))

        q = "SELECT * FROM indicadores_vendas WHERE tenant_id = ?"
        vendas = pd.read_sql(q, conn, params=(tenant,))

        q = "SELECT * FROM indicadores_operacionais WHERE tenant_id = ?"
        oper = pd.read_sql(q, conn, params=(tenant,))

        q = "SELECT * FROM indicadores_marketing WHERE tenant_id = ?"
        mkt = pd.read_sql(q, conn, params=(tenant,))

        q = "SELECT * FROM indicadores_clientes WHERE tenant_id = ?"
        clientes = pd.read_sql(q, conn, params=(tenant,))

        q = "SELECT * FROM dados_contabeis WHERE tenant_id = ?"
        cont = pd.read_sql(q, conn, params=(tenant,))

        return {
            "financeiros": finance,
            "dre": dre,
            "vendas": vendas,
            "operacionais": oper,
            "marketing": mkt,
            "clientes": clientes,
            "contabeis": cont
        }
    finally:
        conn.close()

# -----------------------------
# Carregar dados e exibir dashboard
# -----------------------------
dfs = fetch_tables_for_tenant(tenant_id)

# Integração com inputs manuais (se houver) e derived_metrics pode ser feita
# no show_dashboard. Aqui apenas encaminhamos os dataframes.
show_dashboard(dfs, tenant_id=tenant_id, periodo=periodo, modo=modo)

# Observação: executar com: streamlit run app/main.py