# etl/writer.py (refatorado)
from typing import Optional, Dict, Any
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from .utils import setup_logger
from .loaders import load_to_staging

logger = setup_logger(__name__)

# -------------------------
# Low-level helpers
# -------------------------
def _write_upload_error(engine: Engine, import_batch_id: str, file_name: str, line_number: Optional[int],
                        target_table: str, error_code: str, error_message: str, raw_value: Optional[str] = None) -> None:
    """
    Insere um registro em upload_errors para rastrear problemas de ingestão.
    Usa engine/connection compatível com SQLAlchemy.
    """
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
        with engine.begin() as conn:
            conn.execute(text(sql), params)
    except Exception:
        logger.exception("Falha ao gravar upload_error para import_batch_id=%s file=%s", import_batch_id, file_name)

def record_upload_result(engine: Engine, upload_id: int, stats: Dict[str, Any]) -> None:
    """
    Atualiza a tabela uploads com estatísticas do processamento.
    stats deve conter: rows_read, status, import_batch_id
    """
    stmt = text("""
      UPDATE uploads SET row_count = :row_count, status = :status, processed_at = now(), import_batch_id = :batch
      WHERE upload_id = :upload_id
    """)
    try:
        with engine.begin() as conn:
            conn.execute(stmt, {
                'row_count': stats.get('rows_read', 0),
                'status': stats.get('status', 'processed'),
                'batch': stats.get('import_batch_id'),
                'upload_id': upload_id
            })
    except Exception:
        logger.exception("Falha ao atualizar uploads upload_id=%s", upload_id)

# -------------------------
# Upsert helpers (fatos)
# -------------------------
def upsert_fact_payables(engine: Engine, df: pd.DataFrame) -> None:
    """
    Upsert df into fact_payables using a temp table and ON CONFLICT.
    Assumes UNIQUE(invoice_ref, supplier_cnpj, amount_original) exists.
    """
    tmp = "tmp_payables"
    try:
        with engine.begin() as conn:
            df.to_sql(tmp, conn, if_exists='replace', index=False, method='multi', chunksize=10000)
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
    except Exception:
        logger.exception("Erro no upsert_fact_payables")
        raise

def upsert_fact_sales(engine: Engine, df: pd.DataFrame) -> None:
    """
    Upsert df into fact_sales using a temp table and ON CONFLICT.
    Ajuste a chave de conflito conforme sua modelagem.
    """
    tmp = "tmp_sales"
    try:
        with engine.begin() as conn:
            df.to_sql(tmp, conn, if_exists='replace', index=False, method='multi', chunksize=10000)
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
        except Exception:
            logger.exception("Erro no upsert_fact_sales")
            raise

# -------------------------
# Writers públicos (contrato)
# -------------------------
def write_finance(engine: Engine, df: Optional[pd.DataFrame], import_batch_id: str,
                  file_hash: Optional[str] = None, file_name: Optional[str] = None) -> None:
    """
    Escreve dados financeiros:
    - Se df fornecido: valida, grava em stg_cashflow_daily e promove para fact (via upsert/promotion).
    - Se df is None: assume que dados já foram carregados em staging e promove por import_batch_id.
    """
    table = "cashflow_daily"
    try:
        if df is None:
            # modo streaming / já em staging: promover diretamente
            # Exemplo simples: promover agregando e upsert em fact_cashflow_daily
            promote_sql = """
            INSERT INTO fact_cashflow_daily (date, filial, caixa, cash_in, cash_out, import_batch_id, imported_at)
            SELECT date, filial, caixa, SUM(cash_in)::numeric, SUM(cash_out)::numeric, :import_batch_id, now()
            FROM stg_cashflow_daily
            WHERE import_batch_id = :import_batch_id
            GROUP BY date, filial, caixa
            ON CONFLICT (date, filial, caixa) DO UPDATE
              SET cash_in = EXCLUDED.cash_in, cash_out = EXCLUDED.cash_out, import_batch_id = EXCLUDED.import_batch_id, imported_at = now();
            """
            with engine.begin() as conn:
                conn.execute(text(promote_sql), {"import_batch_id": import_batch_id})
            logger.info("Promoted cashflow for import_batch_id=%s", import_batch_id)
            return

        # validações básicas
        required = ["date", "cash_in", "cash_out"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            msg = f"Missing required columns: {missing}"
            logger.error(msg)
            _write_upload_error(engine, import_batch_id, file_name or "unknown", None, table, "MISSING_REQUIRED", msg)
            return

        # limpeza e coerção
        df = df.copy()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for num in ("cash_in", "cash_out", "transfer_in", "transfer_out", "closing_balance"):
            if num in df.columns:
                df[num] = pd.to_numeric(df[num], errors="coerce").fillna(0)

        # carregar em staging
        rows = load_to_staging(df, table, upload_id=import_batch_id, import_batch_id=import_batch_id, engine=engine)
        logger.info("Wrote %d rows to stg_%s", rows, table)

        # promover para fact (exemplo simples)
        with engine.begin() as conn:
            conn.execute(text("""
            INSERT INTO fact_cashflow_daily (date, filial, caixa, cash_in, cash_out, import_batch_id, imported_at)
            SELECT date, filial, caixa, SUM(cash_in)::numeric, SUM(cash_out)::numeric, :import_batch_id, now()
            FROM stg_cashflow_daily
            WHERE import_batch_id = :import_batch_id
            GROUP BY date, filial, caixa
            ON CONFLICT (date, filial, caixa) DO UPDATE
              SET cash_in = EXCLUDED.cash_in, cash_out = EXCLUDED.cash_out, import_batch_id = EXCLUDED.import_batch_id, imported_at = now();
            """), {"import_batch_id": import_batch_id})
        logger.info("Promotion complete for import_batch_id=%s", import_batch_id)

    except Exception as e:
        logger.exception("Erro em write_finance import_batch_id=%s", import_batch_id)
        _write_upload_error(engine, import_batch_id, file_name or "unknown", None, table, "WRITE_ERROR", str(e))
        raise

def write_ops(engine: Engine, df: Optional[pd.DataFrame], import_batch_id: str,
              file_hash: Optional[str] = None, file_name: Optional[str] = None) -> None:
    """
    Escrita para contas a pagar / operações.
    - Se df: valida, grava em stg_payables (ou stg_{table}) e chama upsert_fact_payables.
    - Se df is None: assume staging já populado e chama upsert por import_batch_id.
    """
    table = "payables"
    try:
        if df is None:
            # promover tudo do staging para fact usando import_batch_id
            # aqui assumimos que a promoção consome stg_payables por import_batch_id
            promote_sql = """
            INSERT INTO fact_payables (supplier_cnpj, supplier_name, invoice_ref, category, center_name,
              date_competence, due_date, amount_original, amount_paid, status, payment_date, payment_account, import_batch_id, imported_at)
            SELECT supplier_cnpj, supplier_name, invoice_ref, category, center_name,
              date_competence, due_date, amount_original, amount_paid, status, payment_date, payment_account, import_batch_id, now()
            FROM stg_payables
            WHERE import_batch_id = :import_batch_id
            ON CONFLICT (invoice_ref, supplier_cnpj, amount_original)
            DO UPDATE SET
              amount_paid = EXCLUDED.amount_paid,
              status = EXCLUDED.status,
              payment_date = EXCLUDED.payment_date,
              imported_at = now();
            """
            with engine.begin() as conn:
                conn.execute(text(promote_sql), {"import_batch_id": import_batch_id})
            logger.info("Promoted payables for import_batch_id=%s", import_batch_id)
            return

        # validações mínimas
        required = ["supplier_cnpj", "invoice_ref", "amount_original"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            msg = f"Missing required columns: {missing}"
            logger.error(msg)
            _write_upload_error(engine, import_batch_id, file_name or "unknown", None, table, "MISSING_REQUIRED", msg)
            return

        # coerção de tipos
        df = df.copy()
        for num in ("amount_original", "amount_paid"):
            if num in df.columns:
                df[num] = pd.to_numeric(df[num], errors="coerce").fillna(0)

        # carregar em staging
        rows = load_to_staging(df, table, upload_id=import_batch_id, import_batch_id=import_batch_id, engine=engine)
        logger.info("Wrote %d rows to stg_%s", rows, table)

        # upsert para fact
        upsert_fact_payables(engine, df)
    except Exception as e:
        logger.exception("Erro em write_ops import_batch_id=%s", import_batch_id)
        _write_upload_error(engine, import_batch_id, file_name or "unknown", None, table, "WRITE_ERROR", str(e))
        raise

def write_sales(engine: Engine, df: Optional[pd.DataFrame], import_batch_id: str,
                file_hash: Optional[str] = None, file_name: Optional[str] = None) -> None:
    """
    Escrita para vendas:
    - Se df: valida, grava em stg_sales e chama upsert_fact_sales.
    - Se df is None: assume staging já populado e chama upsert por import_batch_id.
    """
    table = "sales"
    try:
        if df is None:
            # promover do staging
            promote_sql = """
            INSERT INTO fact_sales (sale_datetime, date_id, product_code, product_name, product_group,
              quantity, revenue_net, cost_total, center_name, professional_id, import_batch_id, imported_at)
            SELECT sale_datetime, date_id, product_code, product_name, product_group,
              quantity, revenue_net, cost_total, center_name, professional_id, import_batch_id, now()
            FROM stg_sales
            WHERE import_batch_id = :import_batch_id
            ON CONFLICT (sale_datetime, product_code, quantity)
            DO UPDATE SET
              revenue_net = EXCLUDED.revenue_net,
              cost_total = EXCLUDED.cost_total,
              imported_at = now();
            """
            with engine.begin() as conn:
                conn.execute(text(promote_sql), {"import_batch_id": import_batch_id})
            logger.info("Promoted sales for import_batch_id=%s", import_batch_id)
            return

        # validações mínimas
        required = ["sale_datetime", "product_code", "quantity"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            msg = f"Missing required columns: {missing}"
            logger.error(msg)
            _write_upload_error(engine, import_batch_id, file_name or "unknown", None, table, "MISSING_REQUIRED", msg)
            return

        # coerção de tipos
        df = df.copy()
        if "sale_datetime" in df.columns:
            df["sale_datetime"] = pd.to_datetime(df["sale_datetime"], errors="coerce")
        if "quantity" in df.columns:
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
        for num in ("revenue_net", "cost_total"):
            if num in df.columns:
                df[num] = pd.to_numeric(df[num], errors="coerce").fillna(0)

        # carregar em staging
        rows = load_to_staging(df, table, upload_id=import_batch_id, import_batch_id=import_batch_id, engine=engine)
        logger.info("Wrote %d rows to stg_%s", rows, table)

        # upsert para fact
        upsert_fact_sales(engine, df)
    except Exception as e:
        logger.exception("Erro em write_sales import_batch_id=%s", import_batch_id)
        _write_upload_error(engine, import_batch_id, file_name or "unknown", None, table, "WRITE_ERROR", str(e))
        raise

def write_marketing(engine: Engine, df: Optional[pd.DataFrame], import_batch_id: str,
                    file_hash: Optional[str] = None, file_name: Optional[str] = None) -> None:
    """
    Escrita para indicadores de marketing: grava em stg_indicadores_marketing e promove.
    Implementação genérica — adapte regras de promoção conforme sua modelagem.
    """
    table = "indicadores_marketing"
    try:
        if df is None:
            # promover do staging para fact/indicadores (exemplo genérico)
            promote_sql = """
            INSERT INTO indicadores_marketing (mes, receita, investimento, leads_gerados, import_batch_id, imported_at)
            SELECT mes, receita, investimento, leads_gerados, import_batch_id, now()
            FROM stg_indicadores_marketing
            WHERE import_batch_id = :import_batch_id
            ON CONFLICT (mes) DO UPDATE
              SET receita = EXCLUDED.receita, investimento = EXCLUDED.investimento, leads_gerados = EXCLUDED.leads_gerados, imported_at = now();
            """
            with engine.begin() as conn:
                conn.execute(text(promote_sql), {"import_batch_id": import_batch_id})
            logger.info("Promoted marketing indicators for import_batch_id=%s", import_batch_id)
            return

        # validações mínimas (ajuste conforme colunas reais)
        df = df.copy()
        # coerções simples
        for num in ("receita", "investimento"):
            if num in df.columns:
                df[num] = pd.to_numeric(df[num], errors="coerce").fillna(0)

        rows = load_to_staging(df, table, upload_id=import_batch_id, import_batch_id=import_batch_id, engine=engine)
        logger.info("Wrote %d rows to stg_%s", rows, table)

        # promoção genérica
        with engine.begin() as conn:
            conn.execute(text("""
            INSERT INTO indicadores_marketing (mes, receita, investimento, leads_gerados, import_batch_id, imported_at)
            SELECT mes, receita, investimento, leads_gerados, import_batch_id, now()
            FROM stg_indicadores_marketing
            WHERE import_batch_id = :import_batch_id
            ON CONFLICT (mes) DO UPDATE
              SET receita = EXCLUDED.receita, investimento = EXCLUDED.investimento, leads_gerados = EXCLUDED.leads_gerados, imported_at = now();
            """), {"import_batch_id": import_batch_id})
    except Exception as e:
        logger.exception("Erro em write_marketing import_batch_id=%s", import_batch_id)
        _write_upload_error(engine, import_batch_id, file_name or "unknown", None, table, "WRITE_ERROR", str(e))
        raise

def write_clients(engine: Engine, df: Optional[pd.DataFrame], import_batch_id: str,
                  file_hash: Optional[str] = None, file_name: Optional[str] = None) -> None:
    """
    Escrita para clientes (clientes/produtos/pacientes conforme seu mapeamento).
    - Grava em stg_clients e promove para fact_clients ou tabelas relevantes.
    """
    table = "clients"
    try:
        if df is None:
            # promoção genérica do staging
            promote_sql = """
            INSERT INTO fact_clients (client_id, client_name, client_cpf, client_cep, import_batch_id, imported_at)
            SELECT client_id, client_name, client_cpf, client_cep, import_batch_id, now()
            FROM stg_clients
            WHERE import_batch_id = :import_batch_id
            ON CONFLICT (client_id) DO UPDATE
              SET client_name = EXCLUDED.client_name, client_cpf = EXCLUDED.client_cpf, client_cep = EXCLUDED.client_cep, imported_at = now();
            """
            with engine.begin() as conn:
                conn.execute(text(promote_sql), {"import_batch_id": import_batch_id})
            logger.info("Promoted clients for import_batch_id=%s", import_batch_id)
            return

        # validações mínimas
        df = df.copy()
        # limpeza simples
        for c in df.select_dtypes(include=['object']).columns:
            df[c] = df[c].astype(str).str.strip()

        rows = load_to_staging(df, table, upload_id=import_batch_id, import_batch_id=import_batch_id, engine=engine)
        logger.info("Wrote %d rows to stg_%s", rows, table)

        # promoção genérica
        with engine.begin() as conn:
            conn.execute(text("""
            INSERT INTO fact_clients (client_id, client_name, client_cpf, client_cep, import_batch_id, imported_at)
            SELECT client_id, client_name, client_cpf, client_cep, import_batch_id, now()
            FROM stg_clients
            WHERE import_batch_id = :import_batch_id
            ON CONFLICT (client_id) DO UPDATE
              SET client_name = EXCLUDED.client_name, client_cpf = EXCLUDED.client_cpf, client_cep = EXCLUDED.client_cep, imported_at = now();
            """), {"import_batch_id": import_batch_id})
    except Exception as e:
        logger.exception("Erro em write_clients import_batch_id=%s", import_batch_id)
        _write_upload_error(engine, import_batch_id, file_name or "unknown", None, table, "WRITE_ERROR", str(e))
        raise