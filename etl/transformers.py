# etl/transformers.py
import pandas as pd

def transform_finance(df):
    df_fin = pd.DataFrame({
        "mes": df["mes"],
        "receita": df["receita"],
        "despesa": df["despesa"],
        "lucro": df["receita"] - (df["despesa"] + df["impostos"] + df["investimentos"]),
        "impostos": df["impostos"],
        "investimentos": df["investimentos"],
        "caixa": df["caixa"],
        "ebitda": df.get("ebitda", df["receita"] - df["despesa"]),
        "roi": df.get("roi", (df["receita"] - df["despesa"]) / df["investimentos"].replace(0, 1))
    })
    return df_fin

def transform_sales(df):
    df_sales = pd.DataFrame({
        "mes": df["mes"],
        "ticket_medio": df["receita"] / df["clientes"],
        "taxa_conversao": df.get("taxa_conversao", 0.15),
        "volume_vendas": df.get("volume_vendas", df["clientes"]),
        "churn_rate": df.get("churn_rate", 0.05),
        "ltv": df.get("ltv", (df["receita"] / df["clientes"]) * 12)
    })
    return df_sales

def transform_ops(df):
    df_ops = pd.DataFrame({
        "mes": df["mes"],
        "produtividade": df.get("produtividade", 95),
        "custo_unidade": df["despesa"] / df["clientes"].replace(0, 1),
        "tempo_entrega": df.get("tempo_entrega", 3.5),
        "taxa_retrabalho": df.get("taxa_retrabalho", 0.02)
    })
    return df_ops

def transform_marketing(df):
    df_mkt = pd.DataFrame({
        "mes": df["mes"],
        "cac": df.get("cac", 300),
        "leads_gerados": df.get("leads_gerados", 200),
        "taxa_engajamento": df.get("taxa_engajamento", 0.25)
    })
    return df_mkt

def transform_clients(df):
    df_cli = pd.DataFrame({
        "mes": df["mes"],
        "clientes_ativos": df["clientes"],
        "taxa_retencao": df.get("taxa_retencao", 0.90),
        "nps": df.get("nps", 75)
    })
    return df_cli

# etl/transformers.py (exemplo de join receita+despesa)
def build_financial_kpis(stg_receita, stg_despesa, stg_investimentos=None):
    rec = stg_receita.groupby('mes', as_index=False)['valor'].sum().rename(columns={'valor':'receita'})
    desp = stg_despesa.groupby('mes', as_index=False)['valor'].sum().rename(columns={'valor':'despesa'})
    df = rec.merge(desp, on='mes', how='outer').fillna(0)
    if stg_investimentos is not None:
        inv = stg_investimentos.groupby('mes', as_index=False)['valor'].sum().rename(columns={'valor':'investimentos'})
        df = df.merge(inv, on='mes', how='left').fillna({'investimentos':0})
    else:
        df['investimentos'] = 0
    df['lucro'] = df['receita'] - (df['despesa'] + df['investimentos'])
    df['ebitda'] = df.get('ebitda', df['receita'] - df['despesa'])
    df['roi'] = (df['receita'] - df['despesa']) / df['investimentos'].replace({0: pd.NA})
    return df/;