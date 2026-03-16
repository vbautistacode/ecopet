-- ddl_staging.sql
-- stg_cashflow_daily
CREATE TABLE IF NOT EXISTS stg_cashflow_daily (
  import_batch_id bigint,
  file_name text,
  line_number int,
  file_hash text,
  date date,
  filial text,
  caixa text,
  cash_in numeric(18,2),
  cash_out numeric(18,2),
  transfer_in numeric(18,2),
  transfer_out numeric(18,2),
  closing_balance numeric(18,2),
  line_hash text,
  created_at timestamptz DEFAULT now()
);

-- stg_sales
CREATE TABLE IF NOT EXISTS stg_sales (
  import_batch_id bigint,
  file_name text,
  line_number int,
  file_hash text,
  sale_datetime timestamptz,
  transaction_id text,
  product_group text,
  product_name text,
  product_code text,
  quantity integer,
  revenue_net numeric(18,2),
  revenue_gross numeric(18,2),
  revenue_discount numeric(18,2),
  percent_com numeric(18,4),
  comission_ground numeric(18,2),
  client text,
  client_code text,
  employee_code text,
  cost_total numeric(18,2),
  sale_status text,
  item_type text,
  sale_price numeric(18,2),
  notes_sale text,
  line_hash text,
  created_at timestamptz DEFAULT now()
);

-- stg_payables
CREATE TABLE IF NOT EXISTS stg_payables (
  import_batch_id bigint,
  file_name text,
  line_number int,
  file_hash text,
  invoice_ref text,
  supplier_cnpj text,
  supplier_name text,
  category_level1 text,
  center_name text,
  date_competence date,
  planned_date date,
  due_date date,
  payment_date date,
  amount_original numeric(18,2),
  amount_paid numeric(18,2),
  amount_total_paid numeric(18,2),
  amount_open numeric(18,2),
  interest_realized numeric(18,2),
  penalty_realized numeric(18,2),
  discount_realized numeric(18,2),
  recurrence text,
  recurrence_count integer,
  status text,
  payment_method text,
  payment_account text,
  origin text,
  notes text,
  scheduled_flag boolean,
  detail text,
  amount_category numeric(18,2),
  line_hash text,
  created_at timestamptz DEFAULT now()
);

-- stg_dim_client (optional staging for clients)
CREATE TABLE IF NOT EXISTS stg_dim_client (
  import_batch_id bigint,
  file_name text,
  line_number int,
  file_hash text,
  client_id text,
  client_name text,
  client_cpf text,
  client_email text,
  client_phone text,
  client_cep text,
  created_at timestamptz DEFAULT now(),
  line_hash text
);
