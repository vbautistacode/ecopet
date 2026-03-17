# etl_skeleton.py
import csv
import hashlib
import os
import decimal
from datetime import datetime
from typing import List, Dict, Any, Optional, Iterable
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection

DATABASE_URL = os.getenv("DATABASE_URL")
# se preferir manter engine global, descomente a linha abaixo
# engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# -------------------------
# Utilitários
# -------------------------
def normalize_decimal(s: Optional[str]) -> Optional[decimal.Decimal]:
    if s is None or s == "":
        return None
    # remove separador de milhares e normaliza decimal
    s = str(s).replace(".", "").replace(",", ".").strip()
    try:
        return decimal.Decimal(s)
    except Exception:
        return None

def sha256_hash(*parts: Any) -> str:
    h = hashlib.sha256()
    joined = "||".join(["" if p is None else str(p) for p in parts])
    h.update(joined.encode("utf-8"))
    return h.hexdigest()

# -------------------------
# Leitura e transformação
# -------------------------
def _read_csv_rows(file_path: str, delimiter: str = ";", encodings: Iterable[str] = ("utf-8", "latin-1")) -> List[Dict[str, str]]:
    """
    Lê CSV tentando múltiplos encodings. Retorna lista de dicts (linhas brutas).
    """
    last_exc = None
    for enc in encodings:
        try:
            with open(file_path, newline="", encoding=enc) as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                return [r for r in reader]
        except UnicodeDecodeError as e:
            last_exc = e
            continue
    # fallback: read with replacement to avoid crash
    with open(file_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return [r for r in reader]

def apply_mapping_and_transforms(rows: List[Dict[str, str]], mapping: List[Dict[str, Any]], file_name: str, import_batch_id: str, file_hash: Optional[str]) -> pd.DataFrame:
    """
    mapping: lista de dicts com chaves:
      - source_column (nome na fonte)
      - target_column (nome destino)
      - transform (opcional): 'parse decimal', 'parse dayfirst', 'parse dayfirst to ISO', etc.
      - required (opcional): bool
    Retorna DataFrame pronto para gravar em staging.
    """
    out_rows = []
    for i, r in enumerate(rows, start=1):
        out = {
            "import_batch_id": import_batch_id,
            "file_name": os.path.basename(file_name),
            "line_number": i,
            "file_hash": file_hash,
        }
        for m in mapping:
            src = m.get("source_column")
            tgt = m.get("target_column")
            raw_val = (r.get(src) or "").strip()
            val = raw_val
            transform = (m.get("transform") or "").lower()
            if transform == "parse decimal":
                val = normalize_decimal(raw_val)
            elif transform == "parse dayfirst":
                try:
                    val = datetime.strptime(raw_val, "%d/%m/%Y").date()
                except Exception:
                    val = None
            elif transform == "parse dayfirst to iso":
                try:
                    dt = datetime.strptime(raw_val, "%d/%m/%Y %H:%M:%S")
                    val = dt.isoformat()
                except Exception:
                    val = None
            # adicione outros transforms conforme necessário
            out[tgt] = val
        # linha hash com os valores mapeados (ordem do mapping)
        line_values = [out.get(m.get("target_column")) for m in mapping]
        out["line_hash"] = sha256_hash(*line_values)
        out_rows.append(out)
    # criar DataFrame e normalizar tipos simples
    df = pd.DataFrame(out_rows)
    return df

# -------------------------
# Gravação em staging
# -------------------------
def load_csv_to_staging(file_path: str, mapping: List[Dict[str, Any]], stg_table: str,
                       import_batch_id: str, file_hash: Optional[str], engine: Optional[Engine] = None,
                       delimiter: str = ";") -> int:
    """
    Lê CSV, aplica mapping/transforms e grava em stg_table (schema public).
    Retorna número de linhas inseridas.
    """
    if engine is None:
        if DATABASE_URL is None:
            raise ValueError("DATABASE_URL não configurado e engine não fornecido")
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    rows = _read_csv_rows(file_path, delimiter=delimiter)
    if not rows:
        return 0

    df = apply_mapping_and_transforms(rows, mapping, file_path, import_batch_id, file_hash)

    # grava em staging usando pandas.to_sql com conexão (evita mismatch de paramstyle)
    # usa schema public e append por padrão
    try:
        with connection_context(engine) as conn:
            # pandas to_sql aceita Connection
            df.to_sql(stg_table, conn, if_exists="append", index=False, method="multi", schema="public")
        return len(df)
    except Exception:
        # fallback: tentar inserir em batches menores para isolar erros
        batch_size = 5000
        total = 0
        for start in range(0, len(df), batch_size):
            chunk = df.iloc[start:start+batch_size]
            with connection_context(engine) as conn:
                chunk.to_sql(stg_table, conn, if_exists="append", index=False, method="multi", schema="public")
            total += len(chunk)
        return total

# -------------------------
# Promoção para fact
# -------------------------
def promote_cashflow(import_batch_id: str, engine: Optional[Engine] = None) -> None:
    """
    Agrega stg_cashflow_daily por import_batch_id e upserta em fact_cashflow_daily.
    """
    if engine is None:
        if DATABASE_URL is None:
            raise ValueError("DATABASE_URL não configurado e engine não fornecido")
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    agg_sql = text("""
    INSERT INTO fact_cashflow_daily (
      date, filial, caixa,
      total_recebimentos, total_pagamentos, total_transfer_in, total_transfer_out,
      geracao_caixa_calc, closing_balance_reported, import_batch_id, imported_at
    )
    SELECT
      date,
      filial,
      caixa,
      COALESCE(SUM(cash_in),0) AS total_recebimentos,
      COALESCE(SUM(cash_out),0) AS total_pagamentos,
      COALESCE(SUM(transfer_in),0) AS total_transfer_in,
      COALESCE(SUM(transfer_out),0) AS total_transfer_out,
      COALESCE(SUM(cash_in),0) - COALESCE(SUM(cash_out),0) + COALESCE(SUM(transfer_in),0) - COALESCE(SUM(transfer_out),0) AS geracao_caixa_calc,
      MAX(closing_balance) AS closing_balance_reported,
      :import_batch_id
    FROM stg_cashflow_daily
    WHERE import_batch_id = :import_batch_id
    GROUP BY date, filial, caixa
    ON CONFLICT (date, filial, caixa) DO UPDATE
    SET
      total_recebimentos = EXCLUDED.total_recebimentos,
      total_pagamentos = EXCLUDED.total_pagamentos,
      total_transfer_in = EXCLUDED.total_transfer_in,
      total_transfer_out = EXCLUDED.total_transfer_out,
      geracao_caixa_calc = EXCLUDED.geracao_caixa_calc,
      closing_balance_reported = EXCLUDED.closing_balance_reported,
      imported_at = now();
    """)

    with connection_context(engine) as conn:
        conn.execute(agg_sql, {"import_batch_id": import_batch_id})

# -------------------------
# Exemplo de uso
# -------------------------
if __name__ == "__main__":
    # Exemplo mínimo de execução local (ajuste mapping e paths)
    # mapping = [
    #   {"source_column": "Data", "target_column": "date", "transform": "parse dayfirst"},
    #   {"source_column": "Recebimentos", "target_column": "cash_in", "transform": "parse decimal"},
    #   ...
    # ]
    # file_hash = sha256_hash("path/to/file.csv")
    # import_batch_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    # engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    # rows_written = load_csv_to_staging("samples/fluxo_de_caixa_diario.csv", mapping, "stg_cashflow_daily", import_batch_id, file_hash, engine)
    # promote_cashflow(import_batch_id, engine)
    pass