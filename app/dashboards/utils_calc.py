# app/dashboards/utils_calc.py
"""
Cálculos derivados e compat shim.

Compatível com dataset sem tenant_id: quando tenant_id estiver ausente, os KPIs
são calculados por 'mes' (uma linha por mês). Se 'tenant_id' existir, mantém
o comportamento anterior (linhas por tenant+mes).
"""

from typing import Dict, Any, Optional
import math
import numpy as np
import pandas as pd

def _safe_div(num, den):
    try:
        if den is None or den == 0 or (isinstance(den, float) and math.isnan(den)):
            return np.nan
        return float(num) / float(den)
    except Exception:
        return np.nan

def _to_float_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def _sum_col(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return float(_to_float_series(df[col]).sum())

def _mean_col(df: pd.DataFrame, col: str) -> Optional[float]:
    if df is None or df.empty or col not in df.columns:
        return None
    vals = _to_float_series(df[col]).dropna()
    return float(vals.mean()) if not vals.empty else None

def _get_dfs(dfs: Dict[str, Optional[pd.DataFrame]]) -> Dict[str, pd.DataFrame]:
    mapping = {
        "dre": ["dre", "dre_financeiro"],
        "finance": ["financeiros", "finance", "indicadores_financeiros"],
        "vendas": ["vendas", "indicadores_vendas"],
        "operacional": ["operacionais", "indicadores_operacionais"],
        "marketing": ["marketing", "indicadores_marketing"],
        "clientes": ["clientes", "indicadores_clientes"],
        "contabeis": ["contabeis", "dados_contabeis"]
    }
    res = {}
    for key, candidates in mapping.items():
        found = None
        for c in candidates:
            if c in dfs and dfs[c] is not None:
                found = dfs[c]
                break
        res[key] = found.copy() if isinstance(found, pd.DataFrame) else pd.DataFrame()
    return res

def _compute_for_group(group: Dict[str, pd.DataFrame], tenant: Optional[str], mes: str) -> Dict[str, Any]:
    dre = group["dre"]
    finance = group["finance"]
    vendas = group["vendas"]
    oper = group["operacional"]
    mkt = group["marketing"]
    cli = group["clientes"]
    cont = group["contabeis"]

    out: Dict[str, Any] = {}
    if tenant is not None:
        out["tenant_id"] = tenant
    out["mes"] = mes

    receita_bruta = _sum_col(dre, "receita_bruta")
    deducoes = _sum_col(dre, "deducoes")
    receita_liquida = receita_bruta - deducoes if not (math.isnan(receita_bruta) or math.isnan(deducoes)) else np.nan
    cpv = _sum_col(dre, "custo_produto_vendido") or _sum_col(dre, "cpv")
    csp = _sum_col(dre, "custo_servico_prestado")
    desp_vendas = _sum_col(dre, "despesas_vendas")
    desp_admin = _sum_col(dre, "despesas_administrativas")
    outras = _sum_col(dre, "outras_despesas")
    despesas_operacionais = desp_vendas + desp_admin + outras

    lucro_bruto = receita_liquida - (cpv + csp) if not math.isnan(receita_liquida) else np.nan
    ebitda = lucro_bruto - despesas_operacionais if not math.isnan(lucro_bruto) else np.nan
    out["ebitda"] = float(ebitda) if not (isinstance(ebitda, float) and math.isnan(ebitda)) else np.nan

    rec_fin = _sum_col(dre, "receitas_financeiras")
    desp_fin = _sum_col(dre, "despesas_financeiras")
    resultado_fin = rec_fin - desp_fin
    ir = _sum_col(dre, "imposto_renda")
    lucro_operacional = lucro_bruto - despesas_operacionais if not math.isnan(lucro_bruto) else np.nan
    lucro_antes_ir = lucro_operacional + resultado_fin if not math.isnan(lucro_operacional) else np.nan
    lucro_liquido = lucro_antes_ir - ir if not math.isnan(lucro_antes_ir) else np.nan
    out["lucro_liquido"] = float(lucro_liquido) if not (isinstance(lucro_liquido, float) and math.isnan(lucro_liquido)) else np.nan

    out["margem_bruta"] = _safe_div(lucro_bruto, receita_liquida) if not math.isnan(receita_liquida) else np.nan
    out["margem_operacional"] = _safe_div(lucro_operacional, receita_liquida) if not math.isnan(receita_liquida) else np.nan
    out["margem_liquida"] = _safe_div(lucro_liquido, receita_liquida) if not math.isnan(receita_liquida) else np.nan

    patrimonio = None
    divida_liq = None
    divida_bruta = None
    valor_mercado = None
    valor_firma = None
    numero_papeis = None
    free_float = None
    last = None
    if not cont.empty:
        # preferir registros do mesmo mes; se não houver, usar último disponível
        if "mes" in cont.columns:
            cont_mes = cont[cont.get("mes").astype(str) == str(mes)]
            if not cont_mes.empty:
                last = cont_mes.iloc[-1]
            else:
                last = cont.sort_values("mes").iloc[-1] if "mes" in cont.columns and not cont.empty else cont.iloc[-1]
        else:
            last = cont.iloc[-1] if not cont.empty else None
        if last is not None:
            patrimonio = last.get("patrimonio_liquido") if "patrimonio_liquido" in last.index else None
            divida_liq = last.get("divida_liquida") if "divida_liquida" in last.index else None
            divida_bruta = last.get("divida_bruta") if "divida_bruta" in last.index else None
            valor_mercado = last.get("valor_mercado") if "valor_mercado" in last.index else None
            valor_firma = last.get("valor_firma") if "valor_firma" in last.index else None
            numero_papeis = last.get("numero_papeis") if "numero_papeis" in last.index else None
            free_float = last.get("free_float") if "free_float" in last.index else None

    debt_for_calc = divida_liq if divida_liq not in (None, np.nan) else divida_bruta
    out["divida_ebitda"] = _safe_div(debt_for_calc, ebitda) if not (isinstance(ebitda, float) and math.isnan(ebitda)) else np.nan

    out["roe"] = _safe_div(lucro_liquido, patrimonio)
    out["p_vp"] = _safe_div(valor_mercado, patrimonio)
    out["ev_ebitda"] = _safe_div(valor_firma, ebitda)
    out["pl"] = _safe_div(valor_mercado, lucro_liquido)

    out["cagr_receitas"] = np.nan
    out["peg_ratio"] = np.nan

    entradas_sum = _sum_col(finance, "entradas")
    saidas_sum = _sum_col(finance, "saidas")
    out["roi"] = _safe_div((entradas_sum - saidas_sum), saidas_sum) if saidas_sum else np.nan

    ticket_col_mean = _mean_col(vendas, "ticket_medio")
    if ticket_col_mean is not None:
        ticket_medio = ticket_col_mean
    else:
        receita_from_mkt = _sum_col(mkt, "receita") if "receita" in mkt.columns else 0.0
        receita_from_fin = entradas_sum
        total_revenue = receita_from_mkt + receita_from_fin
        volume = _sum_col(vendas, "volume_vendas")
        ticket_medio = _safe_div(total_revenue, volume) if volume else np.nan
    out["ticket_medio"] = float(ticket_medio) if not (isinstance(ticket_medio, float) and math.isnan(ticket_medio)) else np.nan

    taxa_conv = _mean_col(vendas, "taxa_conversao")
    if taxa_conv is None:
        visitas = _sum_col(mkt, "visitas") if "visitas" in mkt.columns else 0.0
        leads = _sum_col(mkt, "leads_gerados") if "leads_gerados" in mkt.columns else _sum_col(vendas, "leads_gerados")
        clientes_count = _sum_col(cli, "clientes_ativos") if "clientes_ativos" in cli.columns else _sum_col(vendas, "clientes_ativos")
        conv_vis_leads = _safe_div(leads, visitas) if visitas else np.nan
        conv_leads_cli = _safe_div(clientes_count, leads) if leads else np.nan
        conv_total = _safe_div(clientes_count, visitas) if visitas else np.nan
        taxa_conv = conv_total if not math.isnan(conv_total) else (conv_leads_cli if not math.isnan(conv_leads_cli) else np.nan)
    out["taxa_conversao"] = float(taxa_conv) if not (isinstance(taxa_conv, float) and math.isnan(taxa_conv)) else np.nan

    churn_val = _mean_col(cli, "churn_rate")
    if churn_val is None:
        if not cli.empty and "clientes_ativos" in cli.columns and "mes" in cli.columns:
            try:
                cli_sorted = cli.sort_values("mes")
                start = _to_float_series(cli_sorted["clientes_ativos"]).dropna().iloc[0] if not cli_sorted.empty else np.nan
                end = _to_float_series(cli_sorted["clientes_ativos"]).dropna().iloc[-1] if not cli_sorted.empty else np.nan
                churn_calc = _safe_div((start - end), start) if start and start != 0 else np.nan
                churn_val = churn_calc if not math.isnan(churn_calc) else np.nan
            except Exception:
                churn_val = np.nan
        else:
            churn_val = _mean_col(vendas, "churn_rate")
    out["churn_rate"] = float(churn_val) if not (isinstance(churn_val, float) and math.isnan(churn_val)) else np.nan

    ltv_val = _mean_col(vendas, "ltv")
    if ltv_val is None:
        receita_total_vendas = _sum_col(vendas, "receita") if "receita" in vendas.columns else 0.0
        clientes_count = _sum_col(cli, "clientes_ativos") if "clientes_ativos" in cli.columns else 0.0
        ltv_calc = _safe_div(receita_total_vendas, clientes_count) if clientes_count else np.nan
        ltv_val = ltv_calc
    out["ltv"] = float(ltv_val) if not (isinstance(ltv_val, float) and math.isnan(ltv_val)) else np.nan

    out["produtividade"] = float(_mean_col(oper, "produtividade") or _mean_col(oper, "producao") or np.nan)
    out["custo_unidade"] = float(_mean_col(oper, "custo_unidade") or np.nan)

    cac_val = _mean_col(mkt, "cac")
    if cac_val is None:
        investimento = _sum_col(mkt, "investimento")
        leads_sum = _sum_col(mkt, "leads_gerados")
        cac_calc = _safe_div(investimento, leads_sum) if leads_sum else np.nan
        cac_val = cac_calc
    out["cac"] = float(cac_val) if not (isinstance(cac_val, float) and math.isnan(cac_val)) else np.nan
    out["taxa_engajamento"] = float(_mean_col(mkt, "taxa_engajamento") or np.nan)
    investimento_sum = _sum_col(mkt, "investimento")
    leads_sum = _sum_col(mkt, "leads_gerados")
    out["custo_por_lead"] = float(_safe_div(investimento_sum, leads_sum) if leads_sum else np.nan)

    out["taxa_retencao"] = float(_mean_col(cli, "taxa_retencao") or np.nan)
    out["nps"] = float(_mean_col(cli, "nps") or np.nan)

    out["valor_mercado"] = float(valor_mercado) if valor_mercado not in (None, np.nan) else np.nan
    out["valor_firma"] = float(valor_firma) if valor_firma not in (None, np.nan) else np.nan
    try:
        out["numero_papeis"] = int(float(numero_papeis)) if (numero_papeis is not None and str(numero_papeis) != '' and not (isinstance(numero_papeis, float) and __import__('math').isnan(numero_papeis))) else np.nan
    except Exception:
        out["numero_papeis"] = np.nan
    out["free_float"] = float(free_float) if free_float not in (None, "", np.nan) else np.nan

    liq = np.nan
    try:
        if last is not None and "ativo_circulante" in last.index and "divida_bruta" in last.index:
            liq = _safe_div(last.get("ativo_circulante"), last.get("divida_bruta"))
    except Exception:
        liq = np.nan
    out["liquidez_corrente"] = float(liq) if not (isinstance(liq, float) and math.isnan(liq)) else np.nan

    return out


def calc_all_kpis(dfs: Dict[str, Optional[pd.DataFrame]]) -> Dict[str, Any]:
    standardized = _get_dfs(dfs)
    dre = standardized["dre"]
    finance = standardized["finance"]
    vendas = standardized["vendas"]
    oper = standardized["operacional"]
    mkt = standardized["marketing"]
    cli = standardized["clientes"]
    cont = standardized["contabeis"]

    # Determine grouping: if tenant_id exists in any source, compute per tenant+mes; otherwise per mes
    has_tenant = any(("tenant_id" in df.columns) for df in (dre, finance, vendas, oper, mkt, cli, cont) if df is not None and not df.empty)

    pairs = set()
    if has_tenant:
        for df in (dre, finance, vendas, oper, mkt, cli, cont):
            if df is None or df.empty:
                continue
            if "tenant_id" in df.columns and "mes" in df.columns:
                for t, m in zip(df["tenant_id"].astype(str), df["mes"].astype(str)):
                    pairs.add((t, m))
    else:
        # group by mes only (union of months across sources)
        months = set()
        for df in (dre, finance, vendas, oper, mkt, cli, cont):
            if df is None or df.empty:
                continue
            if "mes" in df.columns:
                months.update(df["mes"].astype(str).unique().tolist())
        for m in months:
            pairs.add((None, m))

    rows = []
    for tenant, mes in pairs:
        sub = {}
        # build subframes: if tenant present, filter by tenant+mes; otherwise filter by mes only
        for name, df in (("dre", dre), ("finance", finance), ("vendas", vendas), ("operacional", oper), ("marketing", mkt), ("clientes", cli), ("contabeis", cont)):
            if df is None or df.empty:
                sub[name] = pd.DataFrame()
                continue
            if tenant is not None and "tenant_id" in df.columns and "mes" in df.columns:
                sub[name] = df[(df.get("tenant_id").astype(str) == str(tenant)) & (df.get("mes").astype(str) == str(mes))]
            elif "mes" in df.columns:
                sub[name] = df[df.get("mes").astype(str) == str(mes)]
            else:
                sub[name] = df.copy()
        row = _compute_for_group(sub, tenant, mes)
        rows.append(row)

    derived = pd.DataFrame(rows)

    # calcular CAGR por grupo (tenant or global mes series)
    cagr_map = {}
    if not dre.empty and "mes" in dre.columns:
        if has_tenant:
            tenants = derived["tenant_id"].unique() if "tenant_id" in derived.columns else []
            for t in tenants:
                t_dre = dre[dre.get("tenant_id").astype(str) == str(t)]
                if t_dre.empty:
                    cagr_map[t] = np.nan
                    continue
                monthly = t_dre.groupby("mes")["receita_bruta"].sum().sort_index()
                if len(monthly) >= 2:
                    first = monthly.iloc[0]
                    last = monthly.iloc[-1]
                    n_months = monthly.size
                    years = n_months / 12.0
                    try:
                        cagr = (last / first) ** (1.0 / years) - 1.0 if first > 0 and years > 0 else np.nan
                    except Exception:
                        cagr = np.nan
                else:
                    cagr = np.nan
                cagr_map[t] = cagr
        else:
            monthly = dre.groupby("mes")["receita_bruta"].sum().sort_index()
            if len(monthly) >= 2:
                first = monthly.iloc[0]
                last = monthly.iloc[-1]
                n_months = monthly.size
                years = n_months / 12.0
                try:
                    cagr = (last / first) ** (1.0 / years) - 1.0 if first > 0 and years > 0 else np.nan
                except Exception:
                    cagr = np.nan
            else:
                cagr = np.nan
            cagr_map[None] = cagr

    if not derived.empty:
        if has_tenant and "tenant_id" in derived.columns:
            derived["cagr_receitas"] = derived["tenant_id"].map(cagr_map).astype(float)
        else:
            derived["cagr_receitas"] = derived["mes"].map(lambda m: cagr_map.get(None, np.nan)).astype(float)
        derived["peg_ratio"] = derived.apply(lambda r: _safe_div(r.get("pl"), r.get("cagr_receitas")) if not (r.get("cagr_receitas") in (None, np.nan)) else np.nan, axis=1)

    expected = [
        "tenant_id", "mes", "ebitda", "lucro_liquido", "margem_bruta", "margem_operacional", "margem_liquida",
        "divida_ebitda", "roe", "p_vp", "ev_ebitda", "pl", "cagr_receitas", "peg_ratio", "roi",
        "ticket_medio", "taxa_conversao", "churn_rate", "ltv",
        "produtividade", "custo_unidade", "cac", "taxa_engajamento", "custo_por_lead",
        "taxa_retencao", "nps", "valor_mercado", "valor_firma", "numero_papeis", "free_float", "liquidez_corrente"
    ]
    for col in expected:
        if col not in derived.columns:
            derived[col] = np.nan

    for col in derived.columns:
        if col not in ("tenant_id", "mes"):
            derived[col] = pd.to_numeric(derived[col], errors="coerce")

    out: Dict[str, Any] = {"derived": derived}

    try:
        summary = {}
        if "tenant_id" in derived.columns and derived["tenant_id"].notna().any():
            for tenant in derived["tenant_id"].unique():
                last_row = derived[derived["tenant_id"] == tenant].sort_values("mes").iloc[-1]
                summary[tenant] = {k: (None if pd.isna(last_row.get(k)) else last_row.get(k)) for k in expected if k not in ("tenant_id", "mes")}
        else:
            # summary global: last mes
            if not derived.empty:
                last_row = derived.sort_values("mes").iloc[-1]
                summary["global"] = {k: (None if pd.isna(last_row.get(k)) else last_row.get(k)) for k in expected if k not in ("tenant_id", "mes")}
        out["summary"] = summary
    except Exception:
        out["summary"] = {}

    return out


# compat shim
try:
    calc_all = globals().get("calc_all_kpis") or globals().get("calc_all")
except Exception:
    calc_all = None

def calc_estrategicos_from_dre(dfs):
    if callable(calc_all):
        try:
            res = calc_all(dfs)
            if isinstance(res, dict) and "summary" in res:
                return res["summary"]
            return res
        except Exception:
            pass
    # fallback defensivo
    try:
        dre = dfs.get("dre") if isinstance(dfs, dict) else (dfs if isinstance(dfs, pd.DataFrame) else pd.DataFrame())
        cont = dfs.get("contabeis") if isinstance(dfs, dict) else pd.DataFrame()
        receita = dre["receita_bruta"].sum() if ("receita_bruta" in dre.columns and not dre.empty) else 0.0
        deducoes = dre["deducoes"].sum() if ("deducoes" in dre.columns and not dre.empty) else 0.0
        receita_liq = receita - deducoes
        cpv = dre["custo_produto_vendido"].sum() if ("custo_produto_vendido" in dre.columns and not dre.empty) else 0.0
        csp = dre["custo_servico_prestado"].sum() if ("custo_servico_prestado" in dre.columns and not dre.empty) else 0.0
        desp_vendas = dre["despesas_vendas"].sum() if ("despesas_vendas" in dre.columns and not dre.empty) else 0.0
        desp_admin = dre["despesas_administrativas"].sum() if ("despesas_administrativas" in dre.columns and not dre.empty) else 0.0
        outras = dre["outras_despesas"].sum() if ("outras_despesas" in dre.columns and not dre.empty) else 0.0
        lucro_bruto = receita_liq - (cpv + csp)
        ebitda = lucro_bruto - (desp_vendas + desp_admin + outras)
        patrimonio = None
        if cont is not None and not cont.empty and "patrimonio_liquido" in cont.columns:
            try:
                patrimonio = cont.sort_values("mes").iloc[-1]["patrimonio_liquido"]
            except Exception:
                patrimonio = cont.iloc[-1].get("patrimonio_liquido")
        return {
            "ebitda": float(ebitda) if ebitda is not None else np.nan,
            "margem_liquida": float((ebitda / receita_liq)) if (receita_liq and receita_liq != 0) else np.nan,
            "patrimonio_liquido": float(patrimonio) if patrimonio is not None else np.nan
        }
    except Exception:
        return {}