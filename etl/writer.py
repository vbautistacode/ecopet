# etl/writer.py
from sqlalchemy import text
from .utils import setup_logger
import pandas as pd

logger = setup_logger(__name__)

def upsert_fact_payables(engine, df: pd.DataFrame):
    """
    Upsert df into fact_payables using a temp table and ON CONFLICT.
    Assumes UNIQUE(invoice_ref, supplier_cnpj, amount_original) exists.
    """
    tmp = "tmp_payables"
    df.to_sql(tmp, engine, if_exists='replace', index=False, method='multi', chunksize=10000)
    upsert_sql = f"""
    INSERT INTO fact_payables (supplier_cnpj, supplier_name, invoice_ref, category, center_name,
      date_competence, due_date, amount_original, amount_paid, status, payment_date, payment_account, import_batch_id, imported_at)
    SELECT supplier_cnpj, supplier_name, invoice_ref, category, center_name,
      date_competence, due_date, amount_original, amount_paid, status, payment_date, payment_account, import_batch_id, now()
    FROM {tmp}
    ON CONFLICT (invoice_ref, supplier_cnpj, amount_original)
    DO UPDATE SET
      amount_paid = EXCLUDED.amount_paid,
      status = EXCLUDED.status,
      payment_date = EXCLUDED.payment_date,
      imported_at = now();
    DROP TABLE IF EXISTS {tmp};
    """
    with engine.begin() as conn:
        conn.execute(text(upsert_sql))
    logger.info("Upserted payables from %s", tmp)

def upsert_fact_sales(engine, df: pd.DataFrame):
    tmp = "tmp_sales"
    df.to_sql(tmp, engine, if_exists='replace', index=False, method='multi', chunksize=10000)
    upsert_sql = f"""
    INSERT INTO fact_sales (sale_datetime, date_id, product_code, product_name, product_group,
      quantity, revenue_net, cost_total, center_name, professional_id, import_batch_id, imported_at)
    SELECT sale_datetime, date_id, product_code, product_name, product_group,
      quantity, revenue_net, cost_total, center_name, professional_id, import_batch_id, now()
    FROM {tmp}
    ON CONFLICT (sale_datetime, product_code, quantity)
    DO UPDATE SET
      revenue_net = EXCLUDED.revenue_net,
      cost_total = EXCLUDED.cost_total,
      imported_at = now();
    DROP TABLE IF EXISTS {tmp};
    """
    with engine.begin() as conn:
        conn.execute(text(upsert_sql))
    logger.info("Upserted sales from %s", tmp)

def record_upload_result(engine, upload_id: int, stats: dict):
    stmt = text("""
      UPDATE uploads SET row_count = :row_count, status = :status, processed_at = now(), import_batch_id = :batch
      WHERE upload_id = :upload_id
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            'row_count': stats.get('rows_read', 0),
            'status': stats.get('status', 'processed'),
            'batch': stats.get('import_batch_id'),
            'upload_id': upload_id
        })