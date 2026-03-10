# db/seed_db.py
# Adaptar ETL/seed para Postgres (db/seed_db.py)	Testes e dados de exemplo no ambiente Postgres.

"""
Atualizar db/seed_db.py para usar sqlalchemy engine (ou psycopg) em vez de SQLite specifics.

Garantir que upsert funcione com ON CONFLICT e que colunas novas sejam criadas via migrations ou ALTER TABLE antes do insert.

Rodar seed local apontando para um schema de teste no Supabase.
"""

import pandas as pd
from db.models import get_connection, create_tables

def _delete_existing(conn, table, tenant, mes):
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {table} WHERE tenant_id = ? AND mes = ?", (tenant, mes))
    conn.commit()

def _upsert_dataframe(conn, table, df):
    """
    Para cada linha do df, remove o registro existente (tenant_id, mes) e insere a linha.
    """
    for _, row in df.iterrows():
        tenant = row.get("tenant_id")
        mes = row.get("mes")
        if tenant is None or mes is None:
            continue
        _delete_existing(conn, table, tenant, mes)
    df.to_sql(table, conn, if_exists="append", index=False)

def seed_db():
    conn = get_connection()
    create_tables(conn)

    # meses: 2025-01, 2025-02, 2025-03
    tenants = ["clienteA", "clienteB"]
    meses = ["2025-01", "2025-02", "2025-03"]

    # -----------------------------
    # Indicadores Financeiros (Fluxo de Caixa)
    # -----------------------------
    data_fin = {
        "tenant_id": [],
        "mes": [],
        "entradas": [],
        "saidas": [],
        "saldo": [],
        "caixa": []
    }

    # valores base para clienteA e clienteB (crescimento leve mês a mês)
    base = {
        "clienteA": {
            "entradas": [25000.0, 26000.0, 27000.0],
            "saidas": [18500.0, 18700.0, 19000.0],
            "caixa": [12000.0, 12500.0, 13000.0]
        },
        "clienteB": {
            "entradas": [18000.0, 18500.0, 19000.0],
            "saidas": [14000.0, 14200.0, 14400.0],
            "caixa": [8000.0, 8200.0, 8400.0]
        }
    }

    for tenant in tenants:
        vals = base[tenant]
        for i, mes in enumerate(meses):
            entradas = vals["entradas"][i]
            saidas = vals["saidas"][i]
            saldo = entradas - saidas
            caixa = vals["caixa"][i]
            data_fin["tenant_id"].append(tenant)
            data_fin["mes"].append(mes)
            data_fin["entradas"].append(entradas)
            data_fin["saidas"].append(saidas)
            data_fin["saldo"].append(saldo)
            data_fin["caixa"].append(caixa)

    df_fin = pd.DataFrame(data_fin)
    _upsert_dataframe(conn, "indicadores_financeiros", df_fin)

    # -----------------------------
    # DRE Financeira
    # -----------------------------
    # exemplo de evolução mensal leve
    data_dre = {
        "tenant_id": [],
        "mes": [],
        "receita_bruta": [],
        "deducoes": [],
        "custo_produto_vendido": [],
        "custo_servico_prestado": [],
        "despesas_vendas": [],
        "despesas_administrativas": [],
        "outras_despesas": [],
        "receitas_financeiras": [],
        "despesas_financeiras": [],
        "imposto_renda": []
    }

    dre_base = {
        "clienteA": {
            "receita_bruta": [250000.0, 255000.0, 260000.0],
            "deducoes": [15000.0, 15200.0, 15400.0],
            "cpv": [80000.0, 82000.0, 84000.0],
            "csp": [20000.0, 20500.0, 21000.0],
            "desp_vendas": [15000.0, 15200.0, 15300.0],
            "desp_admin": [10000.0, 10100.0, 10200.0],
            "outras": [5000.0, 5200.0, 5300.0],
            "rec_fin": [8000.0, 8200.0, 8400.0],
            "desp_fin": [4000.0, 4100.0, 4200.0],
            "ir": [12000.0, 12200.0, 12400.0]
        },
        "clienteB": {
            "receita_bruta": [180000.0, 183000.0, 186000.0],
            "deducoes": [10000.0, 10100.0, 10200.0],
            "cpv": [60000.0, 61000.0, 62000.0],
            "csp": [15000.0, 15200.0, 15400.0],
            "desp_vendas": [12000.0, 12100.0, 12200.0],
            "desp_admin": [8000.0, 8100.0, 8200.0],
            "outras": [4000.0, 4100.0, 4200.0],
            "rec_fin": [5000.0, 5100.0, 5200.0],
            "desp_fin": [3000.0, 3050.0, 3100.0],
            "ir": [9000.0, 9200.0, 9400.0]
        }
    }

    for tenant in tenants:
        vals = dre_base[tenant]
        for i, mes in enumerate(meses):
            data_dre["tenant_id"].append(tenant)
            data_dre["mes"].append(mes)
            data_dre["receita_bruta"].append(vals["receita_bruta"][i])
            data_dre["deducoes"].append(vals["deducoes"][i])
            data_dre["custo_produto_vendido"].append(vals["cpv"][i])
            data_dre["custo_servico_prestado"].append(vals["csp"][i])
            data_dre["despesas_vendas"].append(vals["desp_vendas"][i])
            data_dre["despesas_administrativas"].append(vals["desp_admin"][i])
            data_dre["outras_despesas"].append(vals["outras"][i])
            data_dre["receitas_financeiras"].append(vals["rec_fin"][i])
            data_dre["despesas_financeiras"].append(vals["desp_fin"][i])
            data_dre["imposto_renda"].append(vals["ir"][i])

    df_dre = pd.DataFrame(data_dre)
    _upsert_dataframe(conn, "dre_financeiro", df_dre)

    # -----------------------------
    # Indicadores de Vendas
    # -----------------------------
    data_vendas = {
        "tenant_id": [],
        "mes": [],
        "volume_vendas": []
    }

    vendas_base = {
        "clienteA": {
            "volume": [50, 52, 55]},
        "clienteB": {"volume": [40, 41, 43]}
    }

    for tenant in tenants:
        vals = vendas_base[tenant]
        for i, mes in enumerate(meses):
            data_vendas["tenant_id"].append(tenant)
            data_vendas["mes"].append(mes)
            data_vendas["volume_vendas"].append(vals["volume"][i])

    df_vendas = pd.DataFrame(data_vendas)
    _upsert_dataframe(conn, "indicadores_vendas", df_vendas)

    # -----------------------------
    # Indicadores Operacionais
    # -----------------------------
    data_op = {
        "tenant_id": [],
        "mes": [],
        "vendas": [],
        "vendedores": [],
        "quantidade": [],
        "producao": []
    }

    # preenchimento plausível a partir dos valores originais:
    op_base = {
        "clienteA": {
            "prod": [95.0, 95.5, 96.0],   # mapeado para producao
            "vendedores": [5, 5, 5],     # valor plausível constante
            "quantidade": [120, 125, 130],# exemplo de unidades produzidas
            "vendas": [50, 52, 55]        # igual ao volume de vendas
        },
        "clienteB": {
            "prod": [90.0, 90.5, 91.0],
            "vendedores": [4, 4, 4],
            "quantidade": [90, 95, 98],
            "vendas": [40, 41, 43]
        }
    }

    for tenant in tenants:
        vals = op_base[tenant]
        for i, mes in enumerate(meses):
            data_op["tenant_id"].append(tenant)
            data_op["mes"].append(mes)
            data_op["vendas"].append(vals["vendas"][i])
            data_op["vendedores"].append(vals["vendedores"][i])
            data_op["quantidade"].append(vals["quantidade"][i])
            data_op["producao"].append(vals["prod"][i])

    df_op = pd.DataFrame(data_op)
    _upsert_dataframe(conn, "indicadores_operacionais", df_op)

    # -----------------------------
    # Indicadores de Marketing
    # -----------------------------
    
    data_mkt = {
        "tenant_id": [],
        "mes": [],
        "receita": [],
        "investimento": [],
        "leads_gerados": []
    }

    mkt_base = {
        "clienteA": {
            "receita": [25000.0, 26000.0, 27000.0],
            "invest": [6000.0, 6200.0, 6400.0],
            "leads": [200, 210, 220]
        },
        "clienteB": {
            "receita": [18000.0, 18500.0, 19000.0],
            "invest": [4000.0, 4200.0, 4400.0],
            "leads": [150, 155, 160]
        }
    }

    for tenant in tenants:
        vals = mkt_base[tenant]
        for i, mes in enumerate(meses):
            data_mkt["tenant_id"].append(tenant)
            data_mkt["mes"].append(mes)
            data_mkt["receita"].append(vals["receita"][i])
            data_mkt["investimento"].append(vals["invest"][i])
            data_mkt["leads_gerados"].append(vals["leads"][i])

    df_mkt = pd.DataFrame(data_mkt)
    _upsert_dataframe(conn, "indicadores_marketing", df_mkt)

    # -----------------------------
    # Indicadores de Clientes
    # -----------------------------
    data_cli = {
        "tenant_id": [],
        "mes": [],
        "clientes_ativos": []
    }

    cli_base = {
        "clienteA": {
            "ativos": [60, 62, 64]
        },
        "clienteB": {
            "ativos": [45, 46, 47]
        }
    }

    for tenant in tenants:
        vals = cli_base[tenant]
        for i, mes in enumerate(meses):
            data_cli["tenant_id"].append(tenant)
            data_cli["mes"].append(mes)
            data_cli["clientes_ativos"].append(vals["ativos"][i])

    df_cli = pd.DataFrame(data_cli)
    _upsert_dataframe(conn, "indicadores_clientes", df_cli)

    # -----------------------------
    # Dados Contábeis (repetidos por mês para simplificar)
    # -----------------------------
    data_contabil = {
        "tenant_id": [],
        "mes": [],
        "patrimonio_liquido": [],
        "ativos": [],
        "ativo_circulante": [],
        "disponibilidade": [],
        "divida_bruta": [],
        "divida_liquida": [],
        "numero_papeis": [],
        "free_float": [],
        "segmento_listagem": [],
        "tipo_empresa": []
    }

    cont_base = {
        "clienteA": {
            "patrimonio_liquido": 57200.0,
            "ativos": 45121.0,
            "ativo_circulante": 15965.0,
            "disponibilidade": 44560.0,
            "divida_bruta": 16298.0,
            "divida_liquida": 11842.0,
            "numero_papeis": 13534,
            "free_float": 0.989,
            "segmento_listagem": "Novo Mercado",
            "tipo_empresa": "aberta"
        },
        "clienteB": {
            "patrimonio_liquido": 32000.0,
            "ativos": 21000.0,
            "ativo_circulante": 75000.0,
            "disponibilidade": 25000.0,
            "divida_bruta": 80000.0,
            "divida_liquida": 5500.0,
            "numero_papeis": None,
            "free_float": None,
            "segmento_listagem": None,
            "tipo_empresa": "fechada"
        }
    }

    for tenant in tenants:
        vals = cont_base[tenant]
        for mes in meses:
            data_contabil["tenant_id"].append(tenant)
            data_contabil["mes"].append(mes)
            data_contabil["patrimonio_liquido"].append(vals["patrimonio_liquido"])
            data_contabil["ativos"].append(vals["ativos"])
            data_contabil["ativo_circulante"].append(vals["ativo_circulante"])
            data_contabil["disponibilidade"].append(vals["disponibilidade"])
            data_contabil["divida_bruta"].append(vals["divida_bruta"])
            data_contabil["divida_liquida"].append(vals["divida_liquida"])
            data_contabil["numero_papeis"].append(vals["numero_papeis"])
            data_contabil["free_float"].append(vals["free_float"])
            data_contabil["segmento_listagem"].append(vals["segmento_listagem"])
            data_contabil["tipo_empresa"].append(vals["tipo_empresa"])

    df_cont = pd.DataFrame(data_contabil)
    _upsert_dataframe(conn, "dados_contabeis", df_cont)

    conn.close()
    print("🌱 Dados fictícios para 3 meses inseridos com sucesso!")

if __name__ == "__main__":
    seed_db()

# Rodar este script irá popular o banco de dados com dados financeiros e contábeis fictícios para dois clientes: um de capital aberto e outro de capital fechado.
#python -m db.seed_db