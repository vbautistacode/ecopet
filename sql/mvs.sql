-- sql/mvs.sql

-- mv_cashflow_daily
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cashflow_daily AS
WITH agg AS (
  SELECT
    date AS day,
    filial,
    caixa,
    COALESCE(SUM(cash_in),0) AS total_recebimentos,
    COALESCE(SUM(cash_out),0) AS total_pagamentos,
    COALESCE(SUM(transfer_in),0) AS total_transfer_in,
    COALESCE(SUM(transfer_out),0) AS total_transfer_out,
    COALESCE(SUM(cash_in),0) - COALESCE(SUM(cash_out),0) + COALESCE(SUM(transfer_in),0) - COALESCE(SUM(transfer_out),0) AS geracao_caixa_calc,
    MAX(closing_balance) AS closing_balance_reported
  FROM fact_cashflow_daily
  GROUP BY date, filial, caixa
)
SELECT
  day, filial, caixa,
  total_recebimentos, total_pagamentos, total_transfer_in, total_transfer_out,
  geracao_caixa_calc, closing_balance_reported,
  CASE WHEN COALESCE(closing_balance_reported,0)=0 THEN NULL ELSE (total_recebimentos / NULLIF(closing_balance_reported,0))*100 END AS pct_recebimentos_vs_closing,
  CASE WHEN COALESCE(closing_balance_reported,0)=0 THEN NULL ELSE (total_pagamentos / NULLIF(closing_balance_reported,0))*100 END AS pct_pagamentos_vs_closing,
  CASE WHEN ABS(geracao_caixa_calc - COALESCE(closing_balance_reported,0)) > 0.01 THEN true ELSE false END AS reconciliation_mismatch
FROM agg;

-- mv_revenue_daily
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_revenue_daily AS
SELECT
  date_trunc('day', sale_datetime)::date AS day,
  COALESCE(SUM(revenue_net),0) AS revenue_net,
  COUNT(*) AS transactions,
  COALESCE(SUM(quantity),0) AS total_quantity
FROM fact_sales
GROUP BY day;

-- mv_ticket_medio_daily
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ticket_medio_daily AS
SELECT
  day,
  revenue_net,
  transactions,
  CASE WHEN transactions = 0 THEN NULL ELSE revenue_net / NULLIF(transactions,0) END AS ticket_medio
FROM mv_revenue_daily;

-- mv_payables_summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_payables_summary AS
SELECT
  date_competence,
  category_level1,
  COALESCE(SUM(amount_original),0) AS total_amount_original,
  COALESCE(SUM(amount_paid),0) AS total_amount_paid,
  COALESCE(SUM(amount_open),0) AS total_amount_open,
  COUNT(*) FILTER (WHERE status = 'overdue') AS overdue_count
FROM fact_payables
GROUP BY date_competence, category_level1;
