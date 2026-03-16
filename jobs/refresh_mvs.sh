#!/bin/bash
# refresh_mvs.sh
# Executar após ingestão ou via cron

export DATABASE_URL="postgresql://user:pass@host:5432/dbname"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_cashflow_daily;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_revenue_daily;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ticket_medio_daily;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_payables_summary;
SQL