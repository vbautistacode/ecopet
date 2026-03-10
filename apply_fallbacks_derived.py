# apply_fallbacks_derived.py
# KPIs e fallbacks (apply_fallbacks_derived.py)	Garantir Margem Líquida e CAGR calculados corretamente.

import math
import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
from app.dashboards.utils_calc import calc_all_kpis

DB = Path("streamdash.db")

def safe_div(a, b):
    try:
        if a is None or b is None:
            return np.nan
        a = float(a)
        b = float(b)
        if math.isfinite(a) and math.isfinite(b) and b != 0:
            return a / b
    except Exception:
        pass
    return np.nan

def apply_fallbacks(derived):
    # copia para não alterar original por referência
    df = derived.copy()

    # garantir colunas existam
    for c in ["valor_mercado", "valor_firma", "patrimonio_liquido", "ebitda",
              "lucro_liquido", "numero_papeis", "free_float", "cagr_receitas",
              "vendas", "vendedores", "producao", "quantidade", "custo_total",
              "receita", "investimento", "leads_gerados", "clientes_ativos"]:
        if c not in df.columns:
            df[c] = np.nan

    # fallback p_vp: prefer P / VPA (valor_mercado / patrimonio_liquido) se numero_papeis/price não disponíveis
    def compute_p_vp(row):
        # try existing p_vp
        if pd.notna(row.get("p_vp")):
            return row.get("p_vp")
        # if valor_mercado and patrimonio_liquido available
        vm = row.get("valor_mercado")
        patr = row.get("patrimonio_liquido")
        if pd.notna(vm) and pd.notna(patr) and patr != 0:
            return safe_div(vm, patr)
        # if number of shares and price per share exist (price col optional)
        price = row.get("preco_acao") if "preco_acao" in row.index else None
        npap = row.get("numero_papeis")
        if pd.notna(price) and pd.notna(npap) and npap != 0:
            return safe_div(price * npap, patr) if pd.notna(patr) and patr != 0 else np.nan
        return np.nan

    # fallback pl: valor_mercado / lucro_liquido
    def compute_pl(row):
        if pd.notna(row.get("pl")):
            return row.get("pl")
        vm = row.get("valor_mercado")
        lucro = row.get("lucro_liquido")
        return safe_div(vm, lucro)

    # fallback ev_ebitda: valor_firma / ebitda
    def compute_ev_ebitda(row):
        if pd.notna(row.get("ev_ebitda")):
            return row.get("ev_ebitda")
        vf = row.get("valor_firma")
        eb = row.get("ebitda")
        return safe_div(vf, eb)

    # fallback peg_ratio: pl / cagr_receitas (cagr em decimal)
    def compute_peg(row):
        if pd.notna(row.get("peg_ratio")):
            return row.get("peg_ratio")
        pl = compute_pl(row) if pd.isna(row.get("pl")) else row.get("pl")
        cagr = row.get("cagr_receitas")
        if pd.notna(pl) and pd.notna(cagr) and cagr != 0:
            return safe_div(pl, cagr)
        return np.nan

    # produtividade: prefer coluna; senão producao / vendedores ou vendas / vendedores
    def compute_produtividade(row):
        if pd.notna(row.get("produtividade")):
            return row.get("produtividade")
        vendedores = row.get("vendedores")
        producao = row.get("producao")
        vendas = row.get("vendas")
        if pd.notna(vendedores) and vendedores != 0:
            if pd.notna(producao):
                return safe_div(producao, vendedores)
            if pd.notna(vendas):
                return safe_div(vendas, vendedores)
        return np.nan

    # custo_unidade: prefer coluna; senão custo_total / quantidade
    def compute_custo_unidade(row):
        if pd.notna(row.get("custo_unidade")):
            return row.get("custo_unidade")
        custo_total = row.get("custo_total")
        quantidade = row.get("quantidade")
        return safe_div(custo_total, quantidade)

    # taxa_engajamento: fallback simples = leads_gerados / receita (se fizer sentido) ou NaN
    def compute_taxa_engajamento(row):
        if pd.notna(row.get("taxa_engajamento")):
            return row.get("taxa_engajamento")
        leads = row.get("leads_gerados")
        receita = row.get("receita")
        if pd.notna(leads) and pd.notna(receita) and receita != 0:
            return safe_div(leads, receita)
        return np.nan

    # taxa_retencao, nps: sem dados, mantemos NaN; but try simple proxy: change in clientes_ativos
    def compute_taxa_retencao(row, next_row=None):
        if pd.notna(row.get("taxa_retencao")):
            return row.get("taxa_retencao")
        # proxy: if next_row provided compute retention as current_clients / previous_clients (handled outside)
        return np.nan

    def compute_nps(row):
        # no proxy possible without survey data
        return row.get("nps") if pd.notna(row.get("nps")) else np.nan

    # apply per-row
    # for taxa_retencao we need time-order; so we'll compute it per group (tenant)
    df["p_vp"] = df.apply(compute_p_vp, axis=1)
    df["pl"] = df.apply(compute_pl, axis=1)
    df["ev_ebitda"] = df.apply(compute_ev_ebitda, axis=1)
    df["peg_ratio"] = df.apply(compute_peg, axis=1)
    df["produtividade"] = df.apply(compute_produtividade, axis=1)
    df["custo_unidade"] = df.apply(compute_custo_unidade, axis=1)
    df["taxa_engajamento"] = df.apply(compute_taxa_engajamento, axis=1)
    df["nps"] = df.apply(compute_nps, axis=1)

    # taxa_retencao proxy: compute month-over-month retention per tenant from clientes_ativos
    if "clientes_ativos" in df.columns:
        df = df.sort_values(["tenant_id", "mes"])
        df["taxa_retencao"] = np.nan
        for tenant, g in df.groupby("tenant_id"):
            g = g.sort_values("mes")
            prev = None
            for idx, row in g.iterrows():
                cur_clients = row.get("clientes_ativos")
                if prev is not None and pd.notna(prev) and pd.notna(cur_clients):
                    # retention = current / previous if previous > 0
                    if prev != 0:
                        df.at[idx, "taxa_retencao"] = safe_div(cur_clients, prev)
                    else:
                        df.at[idx, "taxa_retencao"] = np.nan
                prev = cur_clients

    # ensure numeric types where appropriate
    for col in ["p_vp", "ev_ebitda", "pl", "peg_ratio", "produtividade", "custo_unidade", "taxa_engajamento", "taxa_retencao", "nps"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def main():
    # carregar DFs a partir do DB como o app faria
    conn = sqlite3.connect(str(DB))
    tables = {
      "dre": "dre_financeiro",
      "contabeis": "dados_contabeis",
      "vendas": "indicadores_vendas",
      "finance": "indicadores_financeiros",
      "marketing": "indicadores_marketing",
      "operacional": "indicadores_operacionais",
      "clientes": "indicadores_clientes"
    }
    dfs = {}
    for k, t in tables.items():
        try:
            dfs[k] = pd.read_sql_query(f"SELECT * FROM {t}", conn)
        except Exception:
            dfs[k] = pd.DataFrame()
    conn.close()

    res = calc_all_kpis(dfs)
    derived = res.get("derived") if isinstance(res, dict) else None
    if derived is None:
        print("calc_all_kpis did not return derived")
        return

    derived_fixed = apply_fallbacks(derived)
    pd.set_option("display.max_columns", None)
    print("\\n=== derived head (fixed) ===")
    print(derived_fixed.head(10).to_string(index=False))
    print("\\n=== derived null counts (fixed) ===")
    print(derived_fixed.isna().sum().to_string())

if __name__ == "__main__":
    main()