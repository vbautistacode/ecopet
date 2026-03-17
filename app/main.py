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

st.title("🖥️ Painel de Business Intelligence | EcoPet")
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
    ["(Todos)", "(Acumulado)"],
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
    from etl.loaders import load_to_staging, read_chunks, load_csv, load_excel
    from etl.transformers import (
        transform_finance, transform_sales, transform_ops, transform_marketing, transform_clients
    )
    import etl.writer as writer_mod
    write_finance = getattr(writer_mod, "write_finance", None)
    write_sales = getattr(writer_mod, "write_sales", None)
    write_ops = getattr(writer_mod, "write_ops", None)
    write_marketing = getattr(writer_mod, "write_marketing", None)
    write_clients = getattr(writer_mod, "write_clients", None)

    # garantir logger local (se não existir no topo do arquivo)
    try:
        logger  # se já existir, usa
    except NameError:
        from etl.utils import setup_logger
        logger = setup_logger(__name__)

    import hashlib, time, os

    def _file_hash_bytes(file_obj):
        try:
            file_obj.seek(0)
            h = hashlib.sha256(file_obj.read()).hexdigest()
            file_obj.seek(0)
            return h
        except Exception:
            return None

    conn = get_connection()
    import_batch_root = int(time.time())
    try:
        for idx, file in enumerate(uploaded_files, start=1):
            file_name = file.name
            try:
                st.sidebar.info(f"Processando {file_name}...")
                import_batch_id = f"{import_batch_root}_{idx}"
                file_hash = _file_hash_bytes(file)

                # leitura robusta: tenta utf-8, se falhar tenta latin-1
                name_lower = file_name.lower()
                df_raw = None
                if name_lower.endswith(".csv"):
                    try:
                        df_raw = load_csv(file, sep=';')  # tenta padrão (detect/utf-8)
                    except Exception as e:
                        # se for erro de encoding, tenta latin-1
                        import traceback
                        tb = traceback.format_exc()
                        logger.warning("load_csv falhou com utf-8 para %s: %s. Tentando latin-1", file_name, e)
                        try:
                            # reabrir/rewind e tentar com latin-1
                            df_raw = load_csv(file, sep=';', encoding='latin-1')
                        except Exception as e2:
                            logger.exception("Falha ao ler %s com latin-1: %s", file_name, e2)
                            raise
                else:
                    # excel
                    try:
                        df_raw = load_excel(file)
                    except Exception:
                        # algumas vezes excel com encoding estranho; log e rethrow
                        logger.exception("Falha ao ler Excel %s", file_name)
                        raise

                # se leitura não produziu DataFrame, pular
                if df_raw is None:
                    st.sidebar.warning(f"Nenhum dado lido de {file_name}; pulando.")
                    continue

                # Transformações
                fin = transform_finance(df_raw)
                sales = transform_sales(df_raw)
                ops = transform_ops(df_raw)
                mkt = transform_marketing(df_raw)
                clients = transform_clients(df_raw)

                # Escrita: cheque se writer existe antes de chamar
                if fin is not None and not fin.empty:
                    if write_finance:
                        write_finance(conn, fin, import_batch_id=import_batch_id, file_hash=file_hash, file_name=file_name)
                    else:
                        logger.warning("write_finance não disponível; pulando gravação de %s", file_name)

                if sales is not None and not sales.empty:
                    if write_sales:
                        write_sales(conn, sales, import_batch_id=import_batch_id, file_hash=file_hash, file_name=file_name)
                    else:
                        logger.warning("write_sales não disponível; pulando gravação de %s", file_name)

                if ops is not None and not ops.empty:
                    if write_ops:
                        write_ops(conn, ops, import_batch_id=import_batch_id, file_hash=file_hash, file_name=file_name)
                    else:
                        logger.warning("write_ops não disponível; pulando gravação de %s", file_name)

                if mkt is not None and not mkt.empty:
                    if write_marketing:
                        write_marketing(conn, mkt, import_batch_id=import_batch_id, file_hash=file_hash, file_name=file_name)
                    else:
                        logger.warning("write_marketing não disponível; pulando gravação de %s", file_name)

                if clients is not None and not clients.empty:
                    if write_clients:
                        write_clients(conn, clients, import_batch_id=import_batch_id, file_hash=file_hash, file_name=file_name)
                    else:
                        logger.warning("write_clients não disponível; pulando gravação de %s", file_name)

                st.sidebar.success(f"✅ {file_name} processado")
            except Exception as file_err:
                st.sidebar.error(f"Erro ao processar {file_name}: {file_err}")
                logger.exception("Erro ao processar arquivo %s", file_name)
                # opcional: gravar em upload_errors via helper se disponível
                try:
                    if hasattr(writer_mod, "_write_upload_error"):
                        writer_mod._write_upload_error(conn, import_batch_id, file_name, None, "unknown", "FILE_PROCESS_ERROR", str(file_err))
                except Exception:
                    logger.exception("Falha ao registrar upload_error para %s", file_name)
                continue
        st.sidebar.success("✅ Processamento dos uploads finalizado")
    except Exception as e:
        st.sidebar.error(f"Erro inesperado ao processar uploads: {e}")
        logger.exception("Erro no processamento de uploads")
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