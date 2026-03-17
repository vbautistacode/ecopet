# etl/writer.py
from typing import Optional, Dict, Any
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
import logging

from .utils import setup_logger, _safe_ident, _qualify, connection_context
from .loaders import load_to_staging

logger = setup_logger(__name__)


def _write_upload_error(engine, import_batch_id: str, file_name: str, line_number: Optional[int],
                        target_table: str, error_code: str, error_message: str, raw_value: Optional[str] = None) -> None:
    sql = """
    INSERT INTO upload_errors (import_batch_id, file_name, line_number, target_table, error_code, error_message, raw_value, created_at)
    VALUES (:import_batch_id, :file_name, :line_number, :target_table, :error_code, :error_message, :raw_value, now())
    """
    params = {
        "import_batch_id": import_batch_id,
        "file_name": file_name,
        "line_number": line_number,
        "target_table": target_table,
        "error_code": error_code,
        "error_message": error_message,
        "raw_value": raw_value
    }
    try:
        with connection_context(engine) as conn:
            conn.execute(text(sql), params)
    except Exception:
        logger.exception("Falha ao gravar upload_error para import_batch_id=%s file=%s", import_batch_id, file_name)


def upsert_fact_payables(engine, df: pd.DataFrame) -> None:
    tmp = f"tmp_payables_{int(pd.Timestamp.utcnow().timestamp())}"
    try:
        with connection_context(engine) as conn:
            df.to_sql(tmp, conn, if_exists='replace', index=False, method='multi', chunksize=10000, schema="public")
            upsert_sql = f"""
            INSERT INTO fact_payables (supplier_cnpj, supplier_name, invoice_ref, category, center_name,
            date_competence, due_date, amount_original, amount_paid, status, payment_date, payment_account, import_batch_id, imported_at)
            SELECT supplier_cnpj, supplier_name, invoice_ref, category, center_name,
            date_competence, due_date, amount_original, amount_paid, status, payment_date, payment_account, import_batch_id, now()
            FROM public."{tmp}"
            ON CONFLICT (invoice_ref, supplier_cnpj, amount_original)
            DO UPDATE SET
            amount_paid = EXCLUDED.amount_paid,
            status = EXCLUDED.status,
            payment_date = EXCLUDED.payment_date,
            imported_at = now();
            DROP TABLE IF EXISTS public."{tmp}";
            """
            conn.execute(text(upsert_sql))
        logger.info("Upserted payables from %s", tmp)
    except Exception:
        logger.exception("Erro no upsert_fact_payables")
        raise


def upsert_fact_sales(engine, df: pd.DataFrame) -> None:
    tmp = f"tmp_sales_{int(pd.Timestamp.utcnow().timestamp())}"
    try:
        with connection_context(engine) as conn:
            df.to_sql(tmp, conn, if_exists='replace', index=False, method='multi', chunksize=10000, schema="public")
            upsert_sql = f"""
            INSERT INTO fact_sales (sale_datetime, date_id, product_code, product_name, product_group,
            quantity, revenue_net, cost_total, center_name, professional_id, import_batch_id, imported_at)
            SELECT sale_datetime, date_id, product_code, product_name, product_group,
            quantity, revenue_net, cost_total, center_name, professional_id, import_batch_id, now()
            FROM public."{tmp}"
            ON CONFLICT (sale_datetime, product_code, quantity)
            DO UPDATE SET
            revenue_net = EXCLUDED.revenue_net,
            cost_total = EXCLUDED.cost_total,
            imported_at = now();
            DROP TABLE IF EXISTS public."{tmp}";
            """
            conn.execute(text(upsert_sql))
        logger.info("Upserted sales from %s", tmp)
    except Exception:
        logger.exception("Erro no upsert_fact_sales")
        raise


def write_finance(engine, df: Optional[pd.DataFrame], import_batch_id: str,
                  file_hash: Optional[str] = None, file_name: Optional[str] = None) -> None:
    table = "cashflow_daily"
    try:
        if df is None:
            promote_sql = """
INSERT INTO fact_cashflow_daily (date, filial, caixa, cash_in, cash_out, import_batch_id, imported_at)
SELECT date, filial, caixa, SUM(cash_in)::numeric, SUM(cash_out)::numeric, :import_batch_id, now()
FROM stg_cashflow_daily
WHERE import_batch_id = :import_batch_id
GROUP BY date, filial, caixa
ON CONFLICT (date, filial, caixa) DO UPDATE
  SET cash_in = EXCLUDED.cash_in, cash_out = EXCLUDED.cash_out, import_batch_id = EXCLUDED.import_batch_id, imported_at = now();
"""
            with connection_context(engine) as conn:
                conn.execute(text(promote_sql), {"import_batch_id": import_batch_id})
            logger.info("Promoted cashflow for import_batch_id=%s", import_batch_id)
            return

        required = ["date", "cash_in", "cash_out"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            msg = f"Missing required columns: {missing}"
            logger.error(msg)
            _write_upload_error(engine, import_batch_id, file_name or "unknown", None, table, "MISSING_REQUIRED", msg)
            return

        df = df.copy()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for num in ("cash_in", "cash_out", "transfer_in", "transfer_out", "closing_balance"):
            if num in df.columns:
                df[num] = pd.to_numeric(df[num], errors="coerce").fillna(0)

        rows = load_to_staging(df, table, upload_id=import_batch_id, import_batch_id=import_batch_id, engine=engine)
        logger.info("Wrote %d rows to stg_%s", rows, table)

        promote_sql2 = """
INSERT INTO fact_cashflow_daily (date, filial, caixa, cash_in, cash_out, import_batch_id, imported_at)
SELECT date, filial, caixa, SUM(cash_in)::numeric, SUM(cash_out)::numeric, :import_batch_id, now()
FROM stg_cashflow_daily
WHERE import_batch_id = :import_batch_id
GROUP BY date, filial, caixa
ON CONFLICT (date, filial, caixa) DO UPDATE
  SET cash_in = EXCLUDED.cash_in, cash_out = EXCLUDED.cash_out, import_batch_id = EXCLUDED.import_batch_id, imported_at = now();
"""
        with connection_context(engine) as conn:
            conn.execute(text(promote_sql2), {"import_batch_id": import_batch_id})
        logger.info("Promotion complete for import_batch_id=%s", import_batch_id)

    except Exception as e:
        logger.exception("Erro em write_finance import_batch_id=%s", import_batch_id)
        _write_upload_error(engine, import_batch_id, file_name or "unknown", None, table, "WRITE_ERROR", str(e))
        raise