# etl/run.py
import argparse
import os
import tempfile
import requests
import logging
from typing import Optional

import pandas as pd

from db.connection import get_engine
from etl.utils import (
    generate_batch_id,
    file_hash,
    load_mapping,
    setup_logger,
    connection_context,
)
from etl.loaders import read_chunks, load_to_staging
from etl.transformers import (
    apply_mapping,
    normalize_dates,
    normalize_numbers,
    generate_product_code,
    strip_non_digits_series,
)
from etl.validations import validate_required, validate_numeric, validate_cnpj_basic
from etl.writer import upsert_fact_payables, upsert_fact_sales, record_upload_result

logger = setup_logger(__name__)


def download_signed_url(url: str, dest: str, timeout: int = 120) -> str:
    """
    Download a signed URL to a local path. Raises on HTTP errors.
    """
    logger.info("Downloading %s -> %s", url, dest)
    r = requests.get(url, stream=True, timeout=timeout)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(8192):
            if chunk:
                f.write(chunk)
    return dest


def _validate_table_identifier(name: str) -> str:
    """
    Validate a simple SQL identifier (letters, numbers, underscore).
    Returns the normalized name (lowercase) or raises ValueError.
    """
    if not isinstance(name, str):
        raise ValueError("table name must be a string")
    n = name.strip()
    # allow letters, numbers and underscore and optionally mixed case; normalize to lower
    import re

    if not re.match(r"^[A-Za-z0-9_]+$", n):
        raise ValueError(f"Invalid table name: {name!r}")
    return n.lower()


def process_file(
    file_path: str,
    upload_id: int,
    mapping_path: str,
    table_name: str,
    engine,
    chunk_size: int = 100_000,
):
    """
    Process a single file: read in chunks, transform, load to staging and upsert to fact.
    engine should be a SQLAlchemy Engine or connection-like; connection_context will normalize.
    """
    rows_read = 0
    import_batch_id = generate_batch_id()
    _, mapping = load_mapping(mapping_path)

    # normalize/validate table name
    try:
        table_key = _validate_table_identifier(table_name)
    except ValueError as e:
        logger.error("Invalid table name: %s", e)
        raise

    # iterate chunks
    for chunk in read_chunks(file_path, chunk_size=chunk_size, sep=";", encoding="utf-8"):
        rows_read += len(chunk)
        # apply mapping
        df = apply_mapping(chunk, mapping)

        # basic normalizations (dates/numbers)
        date_cols = [
            c
            for c in df.columns
            if "date" in c.lower() or c.endswith("_date") or c in ("sale_datetime", "date_competence")
        ]
        num_cols = [
            c
            for c in df.columns
            if any(k in c.lower() for k in ["amount", "valor", "price", "revenue", "cost", "quantity", "quantidade", "liquido", "net"])
        ]
        df = normalize_dates(df, date_cols)
        df = normalize_numbers(df, num_cols)

        # additional transforms
        if "supplier_cnpj" in df.columns:
            df["supplier_cnpj"] = strip_non_digits_series(df["supplier_cnpj"])

        if table_key.lower() == "vendasprodutos":
            # ensure product_code exists
            if "product_code" not in df.columns:
                df = generate_product_code(df, group_col="product_group", name_col="product_name", out_col="product_code")
            # create date_id
            if "sale_datetime" in df.columns:
                df["date_id"] = df["sale_datetime"].dt.date

        # add import_batch_id
        df["import_batch_id"] = import_batch_id

        # load to staging (connection_context inside load_to_staging handles engine/conn)
        load_to_staging(df, table_key, upload_id, import_batch_id, engine)

    # after staging, perform upsert from staging to fact
    stats = {"rows_read": rows_read, "import_batch_id": import_batch_id}
    try:
        # use connection_context when reading staging tables with pandas
        stg_table = f"stg_{table_key}"
        with connection_context(engine) as conn:
            # prefer read_sql_table when conn is SQLAlchemy Connection; connection_context yields compatible object
            try:
                df_all = pd.read_sql_table(stg_table, con=conn, schema="public")
            except Exception:
                df_all = pd.read_sql(f"SELECT * FROM public.{stg_table}", con=conn)

        if table_key == "visao_contas_a_pagar" or table_key == "visao_contas_a_pagar".lower():
            upsert_fact_payables(engine, df_all)
        elif table_key == "vendasprodutos":
            upsert_fact_sales(engine, df_all)
        else:
            logger.info("No upsert handler for table %s; leaving data in staging", table_key)
        stats["status"] = "processed"
    except Exception as e:
        logger.exception("Error during promotion/upsert for %s: %s", table_key, e)
        stats["status"] = "failed"
        stats["error"] = str(e)

    # record result (record_upload_result should accept engine/conn)
    try:
        record_upload_result(engine, upload_id, stats)
    except Exception:
        logger.exception("Failed to record upload result for upload_id=%s", upload_id)

    logger.info("Processed %s rows for %s (batch=%s)", rows_read, table_key, import_batch_id)
    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="Local file path")
    parser.add_argument("--signed_url", help="Signed URL to download file")
    parser.add_argument("--upload_id", type=int, required=True)
    parser.add_argument("--mapping", default="etl/mappings/mapping_v1.csv")
    parser.add_argument(
        "--table",
        required=True,
        help="Source table name: VendasProdutos or Visao_Contas_a_Pagar or fluxo_de_caixa_diario",
    )
    parser.add_argument("--chunk-size", type=int, default=100_000)
    args = parser.parse_args()

    # get engine from central helper (ensures DATABASE_URL is used consistently)
    try:
        engine = get_engine()
    except Exception as e:
        logger.error("Failed to create engine: %s", e)
        return

    tmp_path: Optional[str] = None
    try:
        # prepare file
        if args.signed_url:
            suffix = os.path.basename(args.signed_url) or ".tmp"
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
            tmp_path = tmp.name
            tmp.close()
            download_signed_url(args.signed_url, tmp_path)
            path = tmp_path
        elif args.file:
            path = args.file
        else:
            logger.error("Either --file or --signed_url must be provided")
            return

        # process
        stats = process_file(path, args.upload_id, args.mapping, args.table, engine, chunk_size=args.chunk_size)
        logger.info("Done: %s", stats)
    finally:
        # cleanup temp file if created
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                logger.debug("Could not remove temp file %s", tmp_path)
        # dispose engine to release pool resources
        try:
            engine.dispose()
        except Exception:
            pass


if __name__ == "__main__":
    main()