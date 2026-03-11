# app/dashboards/setor.py
"""
Visualização de Setores para o dashboard.

Função principal:
    show_setor(st_container, df_setores=None, df_fin=None, df_ops=None,
               setor_col='setor', valor_col='valor', titulo='Distribuição por Setor', modo='Resumido')

Comportamento:
- Se receber um DataFrame explícito em df_setores, usa-o.
- Caso contrário, tenta agregar a partir de df_fin (colunas comuns: 'setor', 'receita', 'valor', 'entradas')
  ou df_ops (colunas: 'setor', 'despesa', 'custo').
- Gera um pie chart (donut) e um bar chart lado a lado, além de uma tabela resumida.
- É defensivo quanto a colunas faltantes, tipos e zeros.
"""

from typing import Optional
import streamlit as st
import pandas as pd
import altair as alt

# Importar utilitário de formatação se disponível
try:
    from app.dashboards.utils import format_brl
except Exception:
    # fallback simples
    def format_brl(x):
        try:
            return f"R$ {float(x):,.2f}"
        except Exception:
            return x


def _ensure_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _safe_to_numeric(s):
    return pd.to_numeric(s, errors="coerce").fillna(0)


def _build_from_finance(df_fin: pd.DataFrame, setor_col: str, valor_col_candidates=None) -> pd.DataFrame:
    """
    Tenta construir df_setores a partir de df_fin usando colunas candidatas para valor.
    """
    if df_fin is None or df_fin.empty:
        return pd.DataFrame(columns=[setor_col, "valor"])
    df = df_fin.copy()
    # normalizar nomes
    if setor_col not in df.columns:
        # tentar colunas alternativas
        for alt in ("departamento", "area", "segmento"):
            if alt in df.columns:
                df[setor_col] = df[alt]
                break
    # escolher coluna de valor
    valor_col = None
    if valor_col_candidates:
        for c in valor_col_candidates:
            if c in df.columns:
                valor_col = c
                break
    else:
        for c in ("valor", "receita", "entradas", "faturamento"):
            if c in df.columns:
                valor_col = c
                break
    if valor_col is None:
        # nada para agregar
        return pd.DataFrame(columns=[setor_col, "valor"])
    tmp = df[[setor_col, valor_col]].copy()
    tmp[valor_col] = _safe_to_numeric(tmp[valor_col])
    out = tmp.groupby(setor_col, as_index=False)[valor_col].sum().rename(columns={valor_col: "valor"})
    out = out.sort_values("valor", ascending=False)
    return out


def _build_from_ops(df_ops: pd.DataFrame, setor_col: str, valor_col_candidates=None) -> pd.DataFrame:
    """
    Tenta construir df_setores a partir de df_ops (ex.: custo por setor).
    """
    if df_ops is None or df_ops.empty:
        return pd.DataFrame(columns=[setor_col, "valor"])
    df = df_ops.copy()
    if setor_col not in df.columns:
        for alt in ("departamento", "area", "segmento"):
            if alt in df.columns:
                df[setor_col] = df[alt]
                break
    valor_col = None
    if valor_col_candidates:
        for c in valor_col_candidates:
            if c in df.columns:
                valor_col = c
                break
    else:
        for c in ("custo", "despesa", "valor"):
            if c in df.columns:
                valor_col = c
                break
    if valor_col is None:
        return pd.DataFrame(columns=[setor_col, "valor"])
    tmp = df[[setor_col, valor_col]].copy()
    tmp[valor_col] = _safe_to_numeric(tmp[valor_col])
    out = tmp.groupby(setor_col, as_index=False)[valor_col].sum().rename(columns={valor_col: "valor"})
    out = out.sort_values("valor", ascending=False)
    return out


def show_setor(
    st_container,
    df_setores: Optional[pd.DataFrame] = None,
    df_fin: Optional[pd.DataFrame] = None,
    df_ops: Optional[pd.DataFrame] = None,
    setor_col: str = "setor",
    valor_col: str = "valor",
    titulo: str = "Distribuição por Setor",
    modo: str = "Resumido",
):
    """
    Exibe visualização de setores (pie + bar + tabela).
    - st_container: contexto Streamlit (ex.: with tab_cont:)
    - df_setores: DataFrame já preparado com colunas [setor_col, valor_col] (opcional)
    - df_fin / df_ops: DataFrames auxiliares para construir df_setores se df_setores não for fornecido
    - modo: 'Resumido' ou 'Detalhado'
    """
    with st_container:
        st.subheader(titulo)

        # Preparar df_setores
        if isinstance(df_setores, pd.DataFrame) and not df_setores.empty:
            df = df_setores.copy()
            # garantir colunas
            if setor_col not in df.columns:
                df[setor_col] = "Geral"
            if valor_col not in df.columns:
                df[valor_col] = 0
            df[valor_col] = _safe_to_numeric(df[valor_col])
            df = df.groupby(setor_col, as_index=False)[valor_col].sum().sort_values(valor_col, ascending=False)
        else:
            # tentar construir a partir de df_fin
            df = _build_from_finance(_ensure_df(df_fin), setor_col, valor_col_candidates=[valor_col, "receita", "valor", "entradas"])
            if df.empty:
                df = _build_from_ops(_ensure_df(df_ops), setor_col, valor_col_candidates=[valor_col, "custo", "despesa", "valor"])

        # fallback: se ainda vazio, mensagem e retorno
        if df.empty or df["valor"].sum() == 0:
            st.info("Sem dados suficientes para exibir a distribuição por setor.")
            return

        # calcular percentuais e labels
        total = df["valor"].sum()
        df["percent"] = (df["valor"] / total * 100).round(2)
        df["label"] = df[setor_col].astype(str) + " (" + df["percent"].astype(str) + "%)"

        # cores simples (extendível)
        default_colors = {
            "Laboratório": "#1f77b4",
            "Internação": "#ff7f0e",
            "Clínica": "#2ca02c",
            "Ambulatório": "#d62728",
            "Imagem": "#9467bd",
            "Farmácia": "#8c564b",
            "Geral": "#7f7f7f"
        }
        unique_sectors = df[setor_col].astype(str).tolist()
        color_range = [default_colors.get(s, None) for s in unique_sectors]
        # substituir None por uma paleta genérica
        palette = alt.scheme("category20")
        # construir escala Altair: se houver None, Altair usará sua paleta
        color_scale = alt.Scale(domain=unique_sectors, range=[c for c in color_range if c is not None])

        # Pie (donut)
        pie = alt.Chart(df).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="valor", type="quantitative"),
            color=alt.Color(field=setor_col, type="nominal", scale=color_scale, legend=alt.Legend(title="Setor")),
            tooltip=[
                alt.Tooltip(setor_col, title="Setor"),
                alt.Tooltip("valor", title="Valor", format=",.2f"),
                alt.Tooltip("percent", title="% do total")
            ]
        ).properties(width=350, height=350)

        # Bar chart
        bar = alt.Chart(df).mark_bar().encode(
            x=alt.X("valor:Q", title="Valor"),
            y=alt.Y(f"{setor_col}:N", sort='-x', title="Setor"),
            color=alt.Color(field=setor_col, type="nominal", scale=color_scale, legend=None),
            tooltip=[
                alt.Tooltip(setor_col, title="Setor"),
                alt.Tooltip("valor", title="Valor", format=",.2f"),
                alt.Tooltip("percent", title="% do total")
            ]
        ).properties(width=450, height=350)

        # Layout
        col1, col2 = st.columns([1, 1])
        with col1:
            st.altair_chart(pie, use_container_width=True)
        with col2:
            st.altair_chart(bar, use_container_width=True)

        # Resumo tabular
        df_display = df[[setor_col, "valor", "percent"]].rename(columns={setor_col: "Setor", "valor": "Valor", "percent": "% do total"})
        # formatar valores para exibição
        df_display["Valor"] = df_display["Valor"].apply(format_brl)
        st.markdown("**Resumo por setor**")
        st.dataframe(df_display.reset_index(drop=True), use_container_width=True)

        # Modo detalhado: filtros e top N
        if modo and modo.lower().startswith("detalh"):
            top_n = st.slider("Top N setores", min_value=1, max_value=min(20, len(df)), value=min(10, len(df)))
            st.write(df.head(top_n))

        # Pequeno insight automático: destacar maior e menor
        try:
            maior = df.iloc[0]
            menor = df[df["valor"] > 0].iloc[-1] if (df["valor"] > 0).any() else None
            st.markdown(f"**Maior setor:** {maior[setor_col]} — {format_brl(maior['valor'])} ({maior['percent']}%)")
            if menor is not None:
                st.markdown(f"**Menor setor (com valor > 0):** {menor[setor_col]} — {format_brl(menor['valor'])} ({menor['percent']}%)")
        except Exception:
            # não falhar por causa de insight
            pass