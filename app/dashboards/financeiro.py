# app/dashboards/financeiro.py

import math
import streamlit as st
import pandas as pd
from typing import Optional

from app.dashboards.utils import (
    pct,
    explain_kpi,
    format_brl,
    metric_with_tooltip,
    cached_quality_tag,
)
from app.dashboards.utils import cached_format_brl

def _safe_sum(df: Optional[pd.DataFrame], cols):
    """Soma a primeira coluna existente na lista cols; retorna 0.0 se nenhuma existir ou df vazio."""
    if df is None or df.empty:
        return 0.0
    for c in cols:
        if c in df.columns:
            try:
                return float(pd.to_numeric(df[c], errors="coerce").fillna(0).sum())
            except Exception:
                return 0.0
    return 0.0

def show_finance(df: Optional[pd.DataFrame], modo: str = "Resumido"):
    st.subheader("💰 Fluxo de Caixa")

    if df is None:
        df = pd.DataFrame()

    entradas = _safe_sum(df, ["entradas", "recebimentos", "receita"])
    saidas = _safe_sum(df, ["saidas", "pagamentos", "despesas"])
    # saldo preferimos coluna saldo quando existe, senão calculamos entradas - saidas
    saldo = _safe_sum(df, ["saldo"]) if "saldo" in df.columns and not df.empty else entradas - saidas
    caixa = _safe_sum(df, ["caixa", "disponibilidade", "balanco_caixa"])

    # Percentuais (pct retorna 0..100; recebe part,total)
    total_mov = entradas + saidas if (entradas + saidas) != 0 else None
    entr_pct_total = pct(entradas, total_mov)
    said_pct_total = pct(saidas, total_mov)
    saldo_pct_entr = pct(saldo, entradas) if entradas != 0 else 0.0
    caixa_pct_saldo = pct(caixa, saldo) if saldo != 0 else 0.0

    # Quality tags para saldo e caixa (usa thresholds automáticos quando possível)
    # Aqui usamos kpi_name as aproximação; thresholds para "liquidez_corrente" não existem para caixa/saldo,
    # mas cached_quality_tag aceita None e retornará fallback.
    _, saldo_tag, saldo_color = cached_quality_tag(saldo, kpi_name="liquidez_corrente")
    _, caixa_tag, caixa_color = cached_quality_tag(caixa, kpi_name="liquidez_corrente")

    # KPIs em 4 colunas
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        explain_kpi(
            title="Recebimentos",
            value=format_brl(entradas) if entradas is not None else "—",
            percent=entr_pct_total,
            base_label="Recebimentos + Pagamentos",
            help_text="Somatório de recebimentos no período."
        )
    with c2:
        explain_kpi(
            title="Pagamentos",
            value=format_brl(saidas) if saidas is not None else "—",
            percent=said_pct_total,
            base_label="Recebimentos + Pagamentos",
            help_text="Somatório de pagamentos no período."
        )
    with c3:
        explain_kpi(
            title="Saldo do Período",
            value=format_brl(saldo) if saldo is not None else "—",
            percent=saldo_pct_entr,
            base_label="Entradas",
            help_text="Entradas menos Saídas. Indica geração de caixa no período.",
            color=("#16a34a" if saldo >= 0 else "#ef4444")
        )
    with c4:
        explain_kpi(
            title="Caixa Atual",
            value=format_brl(caixa) if caixa is not None else "—",
            percent=caixa_pct_saldo,
            base_label="Saldo do Período",
            help_text="Posição de caixa após movimentações.",
            color=("#16a34a" if caixa >= 0 else "#ef4444")
        )

    # Gráfico resumo
    try:
        chart_df = pd.DataFrame({
            "categoria": ["Caixa", "Entradas", "Saídas", "Saldo"],
            "valor": [float(caixa or 0.0), float(entradas or 0.0), float(saidas or 0.0), float(saldo or 0.0)]
        }).set_index("categoria")
        st.bar_chart(chart_df, use_container_width=True)
    except Exception:
        st.info("Gráfico resumo indisponível para este conjunto de dados.")

    # Exibir métricas auxiliares (fluxo líquido, saldo médio)
    fluxo_liquido = entradas - saidas
    # saldo_medio por linha se existir coluna 'saldo', senão NaN
    saldo_medio = None
    if "saldo" in df.columns and not df.empty:
        try:
            saldo_medio = float(pd.to_numeric(df["saldo"], errors="coerce").dropna().mean())
        except Exception:
            saldo_medio = None

    col_a, col_b = st.columns(2)
    with col_a:
        metric_with_tooltip(
            "Fluxo Líquido",
            format_brl(fluxo_liquido),
            tooltip="Entradas − Saídas no período.",
            tag_key=None
        )
    with col_b:
        metric_with_tooltip(
            "Saldo Médio",
            format_brl(saldo_medio) if saldo_medio is not None else "—",
            tooltip="Média dos saldos por mês (quando disponível).",
            tag_key=None
        )

    # Modo detalhado mostra tabela com colunas relevantes
    if modo == "Detalhado":
        st.markdown("### 📊 Detalhamento das movimentações")
        cols = [c for c in ["mes", "data", "entradas", "saidas", "saldo", "caixa"] if c in df.columns]
        if cols:
            st.dataframe(df[cols], use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)