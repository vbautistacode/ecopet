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
from app.dashboards.dashboards import show_dashboard

from db.connection import get_connection
from db.init_db import init_db
from db.models import create_tables

# -----------------------------
# Configuração da aplicação
# -----------------------------
st.set_page_config(page_title="EcoPet - BI", layout="wide")
st.title("📊 Painel de Business Intelligence | EcoPet")

def main():
    # Garantir estado inicial
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
        st.session_state["role"] = None

    # Se não autenticado, o show_login vai interromper com st.stop()
    show_login()

    # Daqui para baixo, só roda se estiver autenticado
    st.sidebar.title("📂 Menu")

    # Removida a opção "Gestão de Usuários" — sempre apenas Dashboards
    choice = st.sidebar.radio("Navegação", ["Dashboards"])

    if choice == "Dashboards":
        pass

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

from app.utils.streamlit_compat import force_rerun
if st.sidebar.button("🔄 Atualizar dados"):
    
    force_rerun()


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
        def _is_dbapi(c):
            return hasattr(c, "cursor")

        def _table_exists(c, table_name: str) -> bool:
            # verifica information_schema para saber se a tabela existe no schema public
            try:
                if _is_dbapi(c):
                    cur = c.cursor()
                    cur.execute(
                        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s",
                        (table_name,)
                    )
                    exists = cur.fetchone() is not None
                    try:
                        cur.close()
                    except Exception:
                        pass
                    return exists
                else:
                    # engine path: usar read_sql para checar
                    q = "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s"
                    df = pd.read_sql(q, c, params=(table_name,))
                    return not df.empty
            except Exception:
                return False

        def _safe_read(table_name: str) -> pd.DataFrame:
            # retorna DataFrame vazio se a tabela não existir ou leitura falhar
            if not _table_exists(conn, table_name):
                st.warning(f"Table public.{table_name} not found; returning empty DataFrame.")
                return pd.DataFrame()
            try:
                # preferir read_sql_table quando conn for engine
                try:
                    return pd.read_sql_table(table_name, conn, schema="public")
                except Exception:
                    # fallback para SELECT qualificado (funciona com DB-API e engines)
                    return pd.read_sql(f"SELECT * FROM public.{table_name}", conn)
            except Exception as e:
                st.warning(f"Failed to read public.{table_name}: {e}")
                return pd.DataFrame()

        finance = _safe_read("indicadores_financeiros")
        dre = _safe_read("dre_financeiro")
        vendas = _safe_read("indicadores_vendas")
        oper = _safe_read("indicadores_operacionais")
        mkt = _safe_read("indicadores_marketing")
        clientes = _safe_read("indicadores_clientes")
        cont = _safe_read("dados_contabeis")

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
        st.error(f"Erro ao buscar dados do banco: {e}")
        return {k: pd.DataFrame() for k in ["financeiros","dre","vendas","operacionais","marketing","clientes","contabeis"]}
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
# python -m streamlit run app/main.py