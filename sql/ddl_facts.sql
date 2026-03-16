-- ddl_facts.sql
-- fact_cashflow_daily
CREATE TABLE IF NOT EXISTS fact_cashflow_daily (
  id bigserial PRIMARY KEY,
  date date NOT NULL,
  filial text,
  caixa text,
  total_recebimentos numeric(18,2) NOT NULL DEFAULT 0,
  total_pagamentos numeric(18,2) NOT NULL DEFAULT 0,
  total_transfer_in numeric(18,2) NOT NULL DEFAULT 0,
  total_transfer_out numeric(18,2) NOT NULL DEFAULT 0,
  geracao_caixa_calc numeric(18,2) NOT NULL DEFAULT 0,
  closing_balance_reported numeric(18,2),
  import_batch_id bigint,
  created_at timestamptz DEFAULT now(),
  UNIQUE(date, filial, caixa, import_batch_id)
);
CREATE INDEX IF NOT EXISTS idx_fact_cashflow_daily_date ON fact_cashflow_daily(date);

-- fact_sales
CREATE TABLE IF NOT EXISTS fact_sales (
  id bigserial PRIMARY KEY,
  sale_datetime timestamptz NOT NULL,
  transaction_id text,
  product_code text NOT NULL,
  product_group text,
  product_name text,
  quantity integer NOT NULL,
  revenue_net numeric(18,2) NOT NULL,
  revenue_gross numeric(18,2),
  revenue_discount numeric(18,2),
  percent_com numeric(18,4),
  comission_ground numeric(18,2),
  client_id bigint,
  client_name text,
  employee_code text,
  cost_total numeric(18,2),
  sale_status text,
  item_type text,
  sale_price numeric(18,2),
  import_batch_id bigint,
  created_at timestamptz DEFAULT now(),
  UNIQUE(transaction_id, product_code, import_batch_id)
);
CREATE INDEX IF NOT EXISTS idx_fact_sales_sale_datetime ON fact_sales(sale_datetime);

-- fact_payables
CREATE TABLE IF NOT EXISTS fact_payables (
  id bigserial PRIMARY KEY,
  invoice_ref text,
  supplier_cnpj text,
  supplier_name text,
  category_level1 text,
  center_name text,
  date_competence date,
  due_date date,
  amount_original numeric(18,2),
  amount_paid numeric(18,2),
  amount_open numeric(18,2),
  status text,
  import_batch_id bigint,
  created_at timestamptz DEFAULT now(),
  UNIQUE(invoice_ref, supplier_cnpj, amount_original)
);
CREATE INDEX IF NOT EXISTS idx_fact_payables_date_competence ON fact_payables(date_competence);

-- dim_client
CREATE TABLE IF NOT EXISTS dim_client (
  id bigserial PRIMARY KEY,
  client_code text UNIQUE,
  name text,
  cpf_cnpj text,
  email text,
  phone text,
  created_at timestamptz,
  first_seen_at timestamptz,
  last_seen_at timestamptz
);

-- dim_date (populate with script)
CREATE TABLE IF NOT EXISTS dim_date (
  date date PRIMARY KEY,
  day integer,
  week integer,
  month integer,
  quarter integer,
  year integer,
  is_business_day boolean,
  fiscal_period text
);
