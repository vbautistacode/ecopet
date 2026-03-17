# db/seed_db.py
"""
Seed de exemplo para Postgres/Supabase (sem tenant_id).
Gera dados fictícios para 3 meses e faz upsert por 'mes' nas tabelas alvo.
"""

from datetime import datetime
from sqlalchemy import text
from typing import List
import pandas as pd

from db.models import create_tables
from db.connection import get_connection  # pode retornar psycopg2 connection ou SQLAlchemy engine


def _is_dbapi_conn(conn) -> bool:
    return hasattr(conn, "cursor")


def _bulk_upsert_dbapi(conn, table: str, df: pd.DataFrame, key_cols: List[str]):
    if df.empty:
        return

    cur = conn.cursor()
    tmp_table = f"tmp_{table}_{int(datetime.utcnow().timestamp())}"
    cols = list(df.columns)
    cols_sql = ", ".join([f'"{c}"' for c in cols])

    # criar temp table com colunas TEXT para evitar inferência complexa
    create_tmp_sql = f'CREATE TEMP TABLE "{tmp_table}" ({", ".join([f\'"{c}" TEXT\' for c in cols])}) ON COMMIT DROP;'
    try:
        cur.execute(create_tmp_sql)

        # inserir linhas na temp table
        insert_tmp_sql = f'INSERT INTO "{tmp_table}" ({cols_sql}) VALUES ({", ".join(["%s"] * len(cols))})'
        values = [tuple("" if pd.isna(v) else v for v in row) for row in df[cols].itertuples(index=False, name=None)]
        cur.executemany(insert_tmp_sql, values)

        # montar upsert
        conflict_cols = ", ".join([f'"{c}"' for c in key_cols]) if key_cols else ""
        non_key_cols = [c for c in cols if c not in key_cols]
        if conflict_cols and non_key_cols:
            set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols])
            upsert_sql = f'''
                INSERT INTO public."{table}" ({cols_sql})
                SELECT {cols_sql} FROM "{tmp_table}"
                ON CONFLICT ({conflict_cols}) DO UPDATE
                SET {set_sql};
            '''
        else:
            upsert_sql = f'''
                INSERT INTO public."{table}" ({cols_sql})
                SELECT {cols_sql} FROM "{tmp_table}";
            '''
        cur.execute(upsert_sql)
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass

def _upsert_via_engine(conn, table: str, df: pd.DataFrame, key_cols: List[str]):
    if df.empty:
        return
    tmp_table = f"tmp_{table}_{int(datetime.utcnow().timestamp())}"

    # valida colunas
    cols = list(df.columns)
    for c in cols:
        _ = _safe_ident(c)
    for k in key_cols:
        _ = _safe_ident(k)

    # criar tmp no schema public
    df.to_sql(tmp_table, conn, if_exists='replace', index=False, schema="public")
    cols_sql = ", ".join([_safe_ident(c) for c in cols])
    conflict_cols = ", ".join([_safe_ident(c) for c in key_cols]) if key_cols else ""
    non_key_cols = [c for c in cols if c not in key_cols]
    if conflict_cols and non_key_cols:
        set_sql = ", ".join([f'{_safe_ident(c)} = EXCLUDED.{_safe_ident(c)}' for c in non_key_cols])
        upsert_sql = f'''
            INSERT INTO public.{_safe_ident(table)} ({cols_sql})
            SELECT {cols_sql} FROM public.{_safe_ident(tmp_table)}
            ON CONFLICT ({conflict_cols}) DO UPDATE
            SET {set_sql};
        '''
    else:
        upsert_sql = f'''
            INSERT INTO public.{_safe_ident(table)} ({cols_sql})
            SELECT {cols_sql} FROM public.{_safe_ident(tmp_table)};
        '''
    with conn.begin() as c:
        c.execute(text(upsert_sql))
        c.execute(text(f'DROP TABLE IF EXISTS public.{_safe_ident(tmp_table)};'))

def _upsert_dataframe(conn, table: str, df: pd.DataFrame, key_cols: List[str]):
    if df.empty:
        return

    # normalizar datetimes para strings para evitar problemas em temp tables TEXT
    df2 = df.copy()
    for c in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[c]):
            df2[c] = df2[c].dt.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            df2[c] = df2[c].astype(object)

    if _is_dbapi_conn(conn):
        _bulk_upsert_dbapi(conn, table, df2, key_cols)
    else:
        _upsert_via_engine(conn, table, df2, key_cols)


def seed_db():
    conn = get_connection()
    try:
        create_tables(conn)
    except Exception:
        # create_tables pode aceitar engine ou conn; ignore falhas aqui
        pass

    meses = ["2025-01", "2025-02", "2025-03"]

    # Indicadores Financeiros
    data_fin = {"mes": [], "entradas": [], "saidas": [], "saldo": [], "caixa": []}
    base_fin = {"entradas": [25000.0, 26000.0, 27000.0], "saidas": [18500.0, 18700.0, 19000.0], "caixa": [12000.0, 12500.0, 13000.0]}
    for i, mes in enumerate(meses):
        entradas = base_fin["entradas"][i]
        saidas = base_fin["saidas"][i]
        saldo = entradas - saidas
        caixa = base_fin["caixa"][i]
        data_fin["mes"].append(mes)
        data_fin["entradas"].append(entradas)
        data_fin["saidas"].append(saidas)
        data_fin["saldo"].append(saldo)
        data_fin["caixa"].append(caixa)
    df_fin = pd.DataFrame(data_fin)
    _upsert_dataframe(conn, "indicadores_financeiros", df_fin, key_cols=["mes"])

    # DRE Financeira
    data_dre = {
        "mes": [], "receita_bruta": [], "deducoes": [], "custo_produto_vendido": [], "custo_servico_prestado": [],
        "despesas_vendas": [], "despesas_administrativas": [], "outras_despesas": [], "receitas_financeiras": [],
        "despesas_financeiras": [], "imposto_renda": []
    }
    dre_vals = {
        "receita_bruta": [250000.0, 255000.0, 260000.0],
        "deducoes": [15000.0, 15200.0, 15400.0],
        "custo_produto_vendido": [80000.0, 82000.0, 84000.0],
        "custo_servico_prestado": [20000.0, 20500.0, 21000.0],
        "despesas_vendas": [15000.0, 15200.0, 15300.0],
        "despesas_administrativas": [10000.0, 10100.0, 10200.0],
        "outras_despesas": [5000.0, 5200.0, 5300.0],
        "receitas_financeiras": [8000.0, 8200.0, 8400.0],
        "despesas_financeiras": [4000.0, 4100.0, 4200.0],
        "imposto_renda": [12000.0, 12200.0, 12400.0]
    }
    for i, mes in enumerate(meses):
        data_dre["mes"].append(mes)
        for k, vals in dre_vals.items():
            data_dre[k].append(vals[i])
    df_dre = pd.DataFrame(data_dre)
    _upsert_dataframe(conn, "dre_financeiro", df_dre, key_cols=["mes"])

    # Indicadores de Vendas
    data_vendas = {"mes": [], "volume_vendas": []}
    vendas_vals = [50, 52, 55]
    for i, mes in enumerate(meses):
        data_vendas["mes"].append(mes)
        data_vendas["volume_vendas"].append(vendas_vals[i])
    df_vendas = pd.DataFrame(data_vendas)
    _upsert_dataframe(conn, "indicadores_vendas", df_vendas, key_cols=["mes"])

    # Indicadores Operacionais
    data_op = {"mes": [], "vendas": [], "vendedores": [], "quantidade": [], "producao": []}
    op_vals = {"vendas": [50, 52, 55], "vendedores": [5, 5, 5], "quantidade": [120, 125, 130], "producao": [95.0, 95.5, 96.0]}
    for i, mes in enumerate(meses):
        data_op["mes"].append(mes)
        for k in ["vendas", "vendedores", "quantidade", "producao"]:
            data_op[k].append(op_vals[k][i])
    df_op = pd.DataFrame(data_op)
    _upsert_dataframe(conn, "indicadores_operacionais", df_op, key_cols=["mes"])

    # Indicadores de Marketing
    data_mkt = {"mes": [], "receita": [], "investimento": [], "leads_gerados": []}
    mkt_vals = {"receita": [25000.0, 26000.0, 27000.0], "investimento": [6000.0, 6200.0, 6400.0], "leads_gerados": [200, 210, 220]}
    for i, mes in enumerate(meses):
        data_mkt["mes"].append(mes)
        for k in ["receita", "investimento", "leads_gerados"]:
            data_mkt[k].append(mkt_vals[k][i])
    df_mkt = pd.DataFrame(data_mkt)
    _upsert_dataframe(conn, "indicadores_marketing", df_mkt, key_cols=["mes"])

    # Indicadores de Clientes
    data_cli = {"mes": [], "clientes_ativos": []}
    cli_vals = [60, 62, 64]
    for i, mes in enumerate(meses):
        data_cli["mes"].append(mes)
        data_cli["clientes_ativos"].append(cli_vals[i])
    df_cli = pd.DataFrame(data_cli)
    _upsert_dataframe(conn, "indicadores_clientes", df_cli, key_cols=["mes"])

    # Dados Contábeis
    data_cont = {
        "mes": [], "patrimonio_liquido": [], "ativos": [], "ativo_circulante": [], "disponibilidade": [],
        "divida_bruta": [], "divida_liquida": [], "numero_papeis": [], "free_float": [], "segmento_listagem": [], "tipo_empresa": []
    }
    cont_vals = {
        "patrimonio_liquido": 57200.0, "ativos": 45121.0, "ativo_circulante": 15965.0, "disponibilidade": 44560.0,
        "divida_bruta": 16298.0, "divida_liquida": 11842.0, "numero_papeis": 13534, "free_float": 0.989,
        "segmento_listagem": "Novo Mercado", "tipo_empresa": "aberta"
    }
    for mes in meses:
        data_cont["mes"].append(mes)
        for k, v in cont_vals.items():
            data_cont[k].append(v)
    df_cont = pd.DataFrame(data_cont)
    _upsert_dataframe(conn, "dados_contabeis", df_cont, key_cols=["mes"])

    # fechar conexão se for DB-API
    try:
        if _is_dbapi_conn(conn):
            conn.close()
    except Exception:
        pass

    print("🌱 Dados fictícios para 3 meses inseridos com sucesso!")


if __name__ == "__main__":
    seed_db()