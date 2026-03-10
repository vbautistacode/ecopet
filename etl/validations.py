# etl/validations.py
import pandas as pd
from typing import Optional

def validate_financial_df(df: pd.DataFrame, tenant_col: Optional[str] = None) -> pd.DataFrame:
    """
    Valida e anota o DataFrame financeiro com flags úteis para ETL.
    - tenant_col: nome da coluna de tenant (ex: "tenant_id"). Se None, validações por tenant são ignoradas.
    """
    if df is None:
        return pd.DataFrame()

    df = df.copy()

    # garantir colunas numéricas
    for c in ["receita", "ebitda", "margem_liquida", "roi"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # inicializa coluna de flag
    if "validation_flag" not in df.columns:
        df["validation_flag"] = None

    # checar nulos críticos (mes, receita, ebitda)
    critical_cols = [col for col in ["mes", "receita", "ebitda"] if col in df.columns]
    if critical_cols:
        critical_mask = df[critical_cols].isna().any(axis=1)
        df.loc[critical_mask, "validation_flag"] = df.loc[critical_mask, "validation_flag"].fillna("") + "missing_critical;"

    # checar duplicatas por tenant+mes (somente se tenant_col fornecido e existir)
    if tenant_col and tenant_col in df.columns and "mes" in df.columns:
        dup_mask = df.duplicated(subset=[tenant_col, "mes"], keep=False)
        df.loc[dup_mask, "validation_flag"] = df.loc[dup_mask, "validation_flag"].fillna("") + "duplicate;"

    # delta month-over-month por tenant (somente se tenant_col fornecido e existir)
    if tenant_col and tenant_col in df.columns and "receita" in df.columns and "mes" in df.columns:
        # ordenar por tenant e mes; se mes não for datetime, tenta converter (não fatal)
        try:
            df = df.sort_values([tenant_col, "mes"])
        except Exception:
            # fallback: sort apenas por mes se possível
            if "mes" in df.columns:
                try:
                    df = df.sort_values("mes")
                except Exception:
                    pass
        df["receita_pct_change"] = df.groupby(tenant_col)["receita"].pct_change()
        large_delta_mask = df["receita_pct_change"].abs() > 0.5
        df.loc[large_delta_mask, "validation_flag"] = df.loc[large_delta_mask, "validation_flag"].fillna("") + "large_delta;"

    # limpeza final: transformar "" em None
    df["validation_flag"] = df["validation_flag"].apply(lambda x: x if pd.notna(x) and x != "" else None)

    return df
