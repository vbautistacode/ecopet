# app/dashboards/dashboards.py
"""
Painel principal que organiza abas e orquestra módulos:
financeiro, dre, vendas, operacional, marketing, clientes, contabeis e estratégicos.
"""

from typing import Optional, Dict, Any
import streamlit as st
import pandas as pd

from app.dashboards.financeiro import show_finance
from app.dashboards.dre import show_dre
from app.dashboards.vendas import show_sales
from app.dashboards.operacional import show_ops
from app.dashboards.marketing import show_marketing
from app.dashboards.clientes import show_clients
from app.dashboards.estrategicos import show_estrategicos
from app.dashboards.utils import format_brl, cached_quality_tag, pct
from app.dashboards.thresholds import get_thresholds_for, get_prefer_high

# -----------------------------
# Utilitários defensivos locais
# -----------------------------
def _ensure_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

def _safe_mean(df: pd.DataFrame, col: str) -> Optional[float]:
    if df is None or df.empty or col not in df.columns:
        return None
    try:
        vals = pd.to_numeric(df[col], errors="coerce").dropna()
        return float(vals.mean()) if not vals.empty else None
    except Exception:
        return None

def _safe_sum(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0.0
    try:
        return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
    except Exception:
        return 0.0

def _safe_first(df: pd.DataFrame, col: str):
    if df is None or df.empty or col not in df.columns:
        return None
    try:
        return df[col].dropna().iloc[0]
    except Exception:
        return None

# -----------------------------
# Insights simples baseados em thresholds centrais
# -----------------------------
def insights_text(ctx: Dict[str, Any]) -> list:
    notes = []
    # ROI
    if ctx.get("roi") is not None:
        thr = get_thresholds_for("roi")
        if thr and ctx["roi"] < thr.get("bom", 0):
            notes.append("ROI abaixo do alvo. Reavalie alocação de capital.")
    # Liquidez corrente
    if ctx.get("liquidez_corrente") is not None:
        thr = get_thresholds_for("liquidez_corrente")
        if thr and ctx["liquidez_corrente"] < thr.get("bom", 1.0):
            notes.append("Liquidez corrente abaixo do recomendado; verifique posição de curto prazo.")
    # Dívida/EBITDA
    if ctx.get("divida_ebitda") is not None:
        thr = get_thresholds_for("divida_ebitda")
        if thr and ctx["divida_ebitda"] > thr.get("alerta", float("inf")):
            notes.append("Dívida / EBITDA acima do nível de alerta; avalie desalavancagem.")
    # CAGR
    if ctx.get("cagr") is not None:
        thr = get_thresholds_for("cagr")
        if thr and ctx["cagr"] < thr.get("alerta", 0):
            notes.append("Crescimento abaixo do esperado; revise estratégias de mercado.")
    return notes

def render_insights_section(ctx: Dict[str, Any]):
    st.subheader("🧠 Análise automática")
    notes = insights_text(ctx)
    if not notes:
        st.info("Sem alertas relevantes com os dados atuais.")
    else:
        for n in notes:
            st.markdown(f"- {n}")

# -----------------------------
# Painel central (orquestra abas e delega)
# -----------------------------
def show_dashboard(dfs: Dict[str, pd.DataFrame], tenant_id: Optional[str], periodo: str, modo: str = "Resumido"):
    """
    dfs: dict com keys:
      'financeiros','dre','vendas','operacionais','marketing','clientes','contabeis'
    tenant_id: opcional; se None ou não existir nas tabelas, nenhum filtro por tenant é aplicado
    periodo: usado para filtrar os dataframes quando aplicável
    modo: "Resumido" | "Detalhado"
    """

    # Normaliza DFs
    df_fin = _ensure_df(dfs.get("financeiros"))
    df_dre = _ensure_df(dfs.get("dre"))
    df_vendas = _ensure_df(dfs.get("vendas"))
    df_ops = _ensure_df(dfs.get("operacionais"))
    df_mkt = _ensure_df(dfs.get("marketing"))
    df_clientes = _ensure_df(dfs.get("clientes"))
    df_cont = _ensure_df(dfs.get("contabeis"))

    # Filtra por tenant e período quando possível (defensivo, com suporte a "(Todos)" e "(Acumulado)")
    def _normalize_mes(df: pd.DataFrame, col: str = "mes") -> pd.DataFrame:
        """Garante coluna mes_norm no formato YYYY-MM quando possível."""
        if df is None or df.empty:
            return df
        df = df.copy()
        if col in df.columns:
            try:
                df["mes_norm"] = pd.to_datetime(df[col].astype(str), errors="coerce").dt.to_period("M").astype(str)
            except Exception:
                df["mes_norm"] = df[col].astype(str).str.strip().str.slice(0, 7)
            # fallback para valores não parseáveis
            mask_na = df["mes_norm"].isna()
            if mask_na.any():
                df.loc[mask_na, "mes_norm"] = df.loc[mask_na, col].astype(str).str.strip().str.slice(0, 7)
        else:
            df["mes_norm"] = None
        return df

    def _apply_period_filter(df: pd.DataFrame, periodo: str) -> pd.DataFrame:
        """
        Aplica filtro de período:
          - "(Todos)" ou None => retorna df sem filtro
          - "(Acumulado)" => retorna df inteiro (acumulado)
          - "YYYY-MM" => filtra por mes_norm
        """
        if df is None or df.empty:
            return df
        if periodo in (None, "(Todos)"):
            return df
        if periodo == "(Acumulado)":
            return df
        mes_norm = str(periodo).strip()[:7]
        if "mes_norm" in df.columns:
            return df[df["mes_norm"] == mes_norm]
        if "mes" in df.columns:
            return df[df["mes"].astype(str).str.startswith(mes_norm)]
        return df

    # aplicar normalização a todos os DFs que podem ter 'mes'
    df_fin = _normalize_mes(df_fin)
    df_dre = _normalize_mes(df_dre)
    df_vendas = _normalize_mes(df_vendas)
    df_ops = _normalize_mes(df_ops)
    df_mkt = _normalize_mes(df_mkt)
    df_clientes = _normalize_mes(df_clientes)
    df_cont = _normalize_mes(df_cont)

    # filtrar por tenant quando aplicável (defensivo)
    try:
        if tenant_id is not None:
            if isinstance(df_fin, pd.DataFrame) and "tenant_id" in df_fin.columns:
                df_fin = df_fin[df_fin["tenant_id"] == tenant_id]
            # também filtrar os outros DFs por tenant se tiverem tenant_id
            for _df_name in ("df_dre", "df_vendas", "df_ops", "df_mkt", "df_clientes", "df_cont"):
                _df = locals().get(_df_name)
                if isinstance(_df, pd.DataFrame) and "tenant_id" in _df.columns:
                    locals()[_df_name] = _df[_df["tenant_id"] == tenant_id]
    except Exception:
        # não falhar a execução do dashboard por causa de filtro
        pass

    # aplicar filtro de período a todos os DFs
    df_fin = _apply_period_filter(df_fin, periodo)
    df_dre = _apply_period_filter(df_dre, periodo)
    df_vendas = _apply_period_filter(df_vendas, periodo)
    df_ops = _apply_period_filter(df_ops, periodo)
    df_mkt = _apply_period_filter(df_mkt, periodo)
    df_clientes = _apply_period_filter(df_clientes, periodo)
    df_cont = _apply_period_filter(df_cont, periodo)

    tabs = st.tabs([
        "💰 Financeiro", "📑 DRE", "🛒 Vendas", "⚙️ Operacional",
        "📣 Marketing", "👥 Clientes", "📘 Setores", "📊 Indicadores Estratégicos"
    ])

    tab_fin, tab_dre, tab_sales, tab_ops, tab_mkt, tab_cli, tab_cont, tab_estrat = tabs

    # -----------------------------
    # Financeiro
    # -----------------------------
    with tab_fin:
        if not df_fin.empty:
            show_finance(df_fin, modo)
            if modo == "Detalhado" and "mes" in df_fin.columns:
                try:
                    chart_df = df_fin.set_index("mes")[["entradas", "saidas", "saldo", "caixa"]].apply(pd.to_numeric, errors="coerce")
                    st.line_chart(chart_df)
                except Exception:
                    st.warning("Não foi possível gerar o gráfico financeiro.")
        else:
            st.info("Nenhum dado financeiro disponível.")

    # -----------------------------
    # DRE
    # -----------------------------
    with tab_dre:
        # preferir df_dre; se vazio e o usuário pediu acumulado/todos, usar df_fin como fallback acumulado
        dre_df = df_dre.copy() if not df_dre.empty else pd.DataFrame()
        if dre_df.empty and periodo in ("(Todos)", "(Acumulado)"):
            # usar df_fin inteiro (acumulado) como fallback
            dre_df = df_fin.copy()
        if not dre_df.empty:
            show_dre(dre_df, modo, mes=(None if periodo in ("(Todos)", "(Acumulado)") else periodo))
        else:
            st.info("Nenhum dado disponível para DRE.")

    # -----------------------------
    # Vendas
    # -----------------------------
    with tab_sales:
        if not df_vendas.empty:
            show_sales(df_vendas, modo)
            if modo == "Detalhado" and "mes" in df_vendas.columns:
                try:
                    chart_df = df_vendas.set_index("mes")[["ticket_medio", "volume_vendas"]].apply(pd.to_numeric, errors="coerce")
                    st.bar_chart(chart_df)
                except Exception:
                    st.warning("Não foi possível gerar o gráfico de vendas.")
        else:
            st.info("Nenhum dado de vendas disponível.")

    # -----------------------------
    # Operacional
    # -----------------------------
    with tab_ops:
        if not df_ops.empty:
            show_ops(df_ops, modo)
        else:
            st.info("Nenhum dado operacional disponível.")

    # -----------------------------
    # Marketing
    # -----------------------------
    with tab_mkt:
        if not df_mkt.empty:
            show_marketing(df_mkt, modo)
        else:
            st.info("Nenhum dado de marketing disponível.")

    # -----------------------------
    # Clientes
    # -----------------------------
    with tab_cli:
        if not df_clientes.empty:
            show_clients(df_clientes, modo)
        else:
            st.info("Nenhum dado de clientes disponível.")

    # -----------------------------
    # Setores
    # -----------------------------
    from app.dashboards.setor import show_setor
    with tab_cont:
        show_setor(tab_cont, df_setores=None, df_fin=df_fin, df_ops=df_ops, setor_col='setor', valor_col='valor', titulo='Receita por Setor', modo=modo)

    # -----------------------------
    # Indicadores Estratégicos
    # -----------------------------
    with tab_estrat:
        roi = _safe_mean(df_fin, "roi")
        roe = _safe_mean(df_fin, "roe")
        margem_liq = _safe_mean(df_fin, "margem_liquida")
        ebitda = _safe_sum(df_fin, "ebitda")
        liquidez_corrente = _safe_mean(df_fin, "liquidez_corrente")
        divida_ebitda = _safe_mean(df_fin, "divida_ebitda")
        cagr_receitas = _safe_mean(df_fin, "cagr_receitas")

        patrimonio_liq = _safe_first(df_cont, "patrimonio_liquido")
        valor_merc = _safe_first(df_cont, "valor_mercado")
        valor_firm = _safe_first(df_cont, "valor_firma")
        tipo_empresa = _safe_first(df_cont, "tipo_empresa")

        def _safe_div(num, den):
            try:
                if num is None or den is None:
                    return None
                den = float(den)
                if den == 0:
                    return None
                return float(num) / den
            except Exception:
                return None

        roe_calc = _safe_div(ebitda, patrimonio_liq)
        pl_calc = _safe_div(valor_merc, ebitda)
        ev_ebitda_calc = _safe_div(valor_firm, ebitda)

        # delega para o módulo especializado (mostra resumo ou detalhado)
        show_estrategicos(df_fin, df_cont, modo=modo)

        data_ctx = {
            "roi": roi,
            "roe": roe_calc or roe,
            "margem_liquida": margem_liq,
            "liquidez_corrente": liquidez_corrente,
            "divida_ebitda": divida_ebitda,
            "cagr": cagr_receitas
        }
        render_insights_section(data_ctx)