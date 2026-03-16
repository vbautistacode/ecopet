# etl_skeleton.py
import csv
import hashlib
import os
from datetime import datetime
from sqlalchemy import create_engine, text
import decimal

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def normalize_decimal(s):
    if s is None or s == "":
        return None
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return decimal.Decimal(s)
    except Exception:
        return None

def sha256_hash(*parts):
    h = hashlib.sha256()
    h.update("||".join([str(p) if p is not None else "" for p in parts]).encode("utf-8"))
    return h.hexdigest()

def load_csv_to_staging(file_path, mapping, stg_table, import_batch_id, file_hash):
    """
    mapping: list of dicts with keys: source_column, target_column, transform, tipo_destino, required
    """
    rows = []
    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for i, r in enumerate(reader, start=1):
            row = {
                "import_batch_id": import_batch_id,
                "file_name": os.path.basename(file_path),
                "line_number": i,
                "file_hash": file_hash,
            }
            for m in mapping:
                src = m['source_column']
                tgt = m['target_column']
                val = r.get(src, "").strip()
                if m.get('transform') == 'parse decimal':
                    val = normalize_decimal(val)
                elif m.get('transform') == 'parse dayfirst':
                    try:
                        val = datetime.strptime(val, "%d/%m/%Y").date()
                    except Exception:
                        val = None
                elif m.get('transform') == 'parse dayfirst to ISO':
                    try:
                        dt = datetime.strptime(val, "%d/%m/%Y %H:%M:%S")
                        val = dt.isoformat()
                    except Exception:
                        val = None
                # add other transforms as needed
                row[tgt] = val
            row['line_hash'] = sha256_hash(*[row.get(c['target_column']) for c in mapping])
            rows.append(row)
    # bulk insert into staging
    with engine.begin() as conn:
        cols = rows[0].keys()
        insert_sql = f"INSERT INTO {stg_table} ({', '.join(cols)}) VALUES " + ", ".join(
            ["(" + ", ".join([f":{c}" for c in cols]) + ")" for _ in rows]
        )
        conn.execute(text(insert_sql), rows)

def promote_cashflow(import_batch_id):
    """
    Example promotion: aggregate staging into fact_cashflow_daily and upsert.
    """
    with engine.begin() as conn:
        # aggregate
        agg_sql = text("""
        INSERT INTO fact_cashflow_daily (date, filial, caixa, total_recebimentos, total_pagamentos, total_transfer_in, total_transfer_out, geracao_caixa_calc, closing_balance_reported, import_batch_id)
        SELECT
          date,
          filial,
          caixa,
          COALESCE(SUM(cash_in),0),
          COALESCE(SUM(cash_out),0),
          COALESCE(SUM(transfer_in),0),
          COALESCE(SUM(transfer_out),0),
          COALESCE(SUM(cash_in),0) - COALESCE(SUM(cash_out),0) + COALESCE(SUM(transfer_in),0) - COALESCE(SUM(transfer_out),0),
          MAX(closing_balance),
          :import_batch_id
        FROM stg_cashflow_daily
        WHERE import_batch_id = :import_batch_id
        GROUP BY date, filial, caixa
        ON CONFLICT (date, filial, caixa, import_batch_id) DO UPDATE
        SET total_recebimentos = EXCLUDED.total_recebimentos,
            total_pagamentos = EXCLUDED.total_pagamentos,
            total_transfer_in = EXCLUDED.total_transfer_in,
            total_transfer_out = EXCLUDED.total_transfer_out,
            geracao_caixa_calc = EXCLUDED.geracao_caixa_calc,
            closing_balance_reported = EXCLUDED.closing_balance_reported,
            created_at = now();
        """)
        conn.execute(agg_sql, {"import_batch_id": import_batch_id})

if __name__ == "__main__":
    # exemplo de uso
    # gerar file_hash, import_batch_id externamente
    pass