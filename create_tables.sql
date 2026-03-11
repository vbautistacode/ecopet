-- create_tables.sql
-- DDL para criar as tabelas usadas pelo app (Postgres, schema public)

-- 1. indicadores_financeiros
CREATE TABLE IF NOT EXISTS public.indicadores_financeiros (
  id BIGSERIAL PRIMARY KEY,
  mes TEXT NOT NULL,
  entradas NUMERIC(18,2),
  saidas NUMERIC(18,2),
  saldo NUMERIC(18,2),
  caixa NUMERIC(18,2),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ind_fin_mes ON public.indicadores_financeiros (mes);

-- 2. dre_financeiro
CREATE TABLE IF NOT EXISTS public.dre_financeiro (
  id BIGSERIAL PRIMARY KEY,
  mes TEXT NOT NULL,
  receita_bruta NUMERIC(18,2),
  deducoes NUMERIC(18,2),
  custo_produto_vendido NUMERIC(18,2),
  custo_servico_prestado NUMERIC(18,2),
  despesas_vendas NUMERIC(18,2),
  despesas_administrativas NUMERIC(18,2),
  outras_despesas NUMERIC(18,2),
  receitas_financeiras NUMERIC(18,2),
  despesas_financeiras NUMERIC(18,2),
  imposto_renda NUMERIC(18,2),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dre_mes ON public.dre_financeiro (mes);

-- 3. indicadores_vendas
CREATE TABLE IF NOT EXISTS public.indicadores_vendas (
  id BIGSERIAL PRIMARY KEY,
  mes TEXT NOT NULL,
  volume_vendas NUMERIC,
  ticket_medio NUMERIC,
  taxa_conversao NUMERIC,
  churn_rate NUMERIC,
  ltv NUMERIC,
  receita NUMERIC,
  clientes_ativos NUMERIC,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_vendas_mes ON public.indicadores_vendas (mes);

-- 4. indicadores_operacionais
CREATE TABLE IF NOT EXISTS public.indicadores_operacionais (
  id BIGSERIAL PRIMARY KEY,
  mes TEXT NOT NULL,
  vendas NUMERIC,
  vendedores INTEGER,
  quantidade NUMERIC,
  producao NUMERIC,
  produtividade NUMERIC,
  custo_unidade NUMERIC,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_oper_mes ON public.indicadores_operacionais (mes);

-- 5. indicadores_marketing
CREATE TABLE IF NOT EXISTS public.indicadores_marketing (
  id BIGSERIAL PRIMARY KEY,
  mes TEXT NOT NULL,
  receita NUMERIC,
  investimento NUMERIC,
  leads_gerados INTEGER,
  cac NUMERIC,
  taxa_engajamento NUMERIC,
  visitas INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mkt_mes ON public.indicadores_marketing (mes);

-- 6. indicadores_clientes
CREATE TABLE IF NOT EXISTS public.indicadores_clientes (
  id BIGSERIAL PRIMARY KEY,
  mes TEXT NOT NULL,
  clientes_ativos INTEGER,
  taxa_retencao NUMERIC,
  nps NUMERIC,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cli_mes ON public.indicadores_clientes (mes);

-- 7. dados_contabeis
CREATE TABLE IF NOT EXISTS public.dados_contabeis (
  id BIGSERIAL PRIMARY KEY,
  mes TEXT NOT NULL,
  patrimonio_liquido NUMERIC(18,2),
  ativos NUMERIC(18,2),
  ativo_circulante NUMERIC(18,2),
  disponibilidade NUMERIC(18,2),
  divida_bruta NUMERIC(18,2),
  divida_liquida NUMERIC(18,2),
  numero_papeis BIGINT,
  free_float NUMERIC,
  segmento_listagem TEXT,
  tipo_empresa TEXT,
  valor_mercado NUMERIC(18,2),
  valor_firma NUMERIC(18,2),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cont_mes ON public.dados_contabeis (mes);

-- 8. uploads_log
CREATE TABLE IF NOT EXISTS public.uploads_log (
  id BIGSERIAL PRIMARY KEY,
  uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  uploaded_by TEXT,
  filename TEXT,
  file_hash TEXT,
  target_table TEXT,
  rows_count INTEGER,
  status TEXT,
  message TEXT
);

-- 9. users (autenticação)
CREATE TABLE IF NOT EXISTS public.users (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('admin','viewer')) DEFAULT 'viewer',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);