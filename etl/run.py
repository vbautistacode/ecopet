#etl/run.py
#!/usr/bin/env python3
import argparse
import os
import tempfile
import requests
from sqlalchemy import create_engine
from .utils import generate_batch_id, file_hash, load_mapping, setup_logger
from .loaders import read_chunks, load_to_staging
from .transformers import apply_mapping, normalize_dates, normalize_numbers, generate_product_code, strip_non_digits_series
from .validations import validate_required, validate_numeric, validate_cnpj_basic
from .writer import upsert_fact_payables, upsert_fact_sales, record_upload_result
import pandas as pd

logger = setup_logger(__name__)

def download_signed_url(url, dest):
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(dest, 'wb') as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    return dest

def process_file(file_path, upload_id, mapping_path, table_name, engine, chunk_size=100000):
    rows_read = 0
    import_batch_id = generate_batch_id()
    _, mapping = load_mapping(mapping_path)
    # iterate chunks
    for chunk in read_chunks(file_path, chunk_size=chunk_size, sep=';', encoding='utf-8'):
        rows_read += len(chunk)
        # apply mapping
        df = apply_mapping(chunk, mapping)
        # basic normalizations (dates/numbers) - heuristics: look for columns with 'date' or numeric names
        date_cols = [c for c in df.columns if 'date' in c.lower() or c.endswith('_date') or c in ('sale_datetime','date_competence')]
        num_cols = [c for c in df.columns if any(k in c.lower() for k in ['amount','valor','price','revenue','cost','quantity','quantidade','liquido','net'])]
        df = normalize_dates(df, date_cols)
        df = normalize_numbers(df, num_cols)
        # additional transforms
        if 'supplier_cnpj' in df.columns:
            df['supplier_cnpj'] = strip_non_digits_series(df['supplier_cnpj'])
        if table_name == 'VendasProdutos':
            # ensure product_code exists
            if 'product_code' not in df.columns:
                df = generate_product_code(df, group_col='product_group', name_col='product_name', out_col='product_code')
            # create date_id
            if 'sale_datetime' in df.columns:
                df['date_id'] = df['sale_datetime'].dt.date
        # add import_batch_id
        df['import_batch_id'] = import_batch_id
        # load to staging
        load_to_staging(df, table_name.lower(), upload_id, import_batch_id, engine)
    # after staging, perform upsert from staging to fact
    stats = {'rows_read': rows_read, 'import_batch_id': import_batch_id}
    if table_name == 'Visao_Contas_a_Pagar':
        # read staging into df for upsert (small volumes) or use writer.upsert with SQL
        df_all = pd.read_sql_table(f"stg_{table_name.lower()}", engine)
        upsert_fact_payables(engine, df_all)
    elif table_name == 'VendasProdutos':
        df_all = pd.read_sql_table(f"stg_{table_name.lower()}", engine)
        upsert_fact_sales(engine, df_all)
    stats['status'] = 'processed'
    record_upload_result(engine, upload_id, stats)
    logger.info("Processed %s rows for %s", rows_read, table_name)
    return stats

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help='Local file path')
    parser.add_argument('--signed_url', help='Signed URL to download file')
    parser.add_argument('--upload_id', type=int, required=True)
    parser.add_argument('--mapping', default='etl/mappings/mapping_v1.csv')
    parser.add_argument('--table', required=True, help='Source table name: VendasProdutos or Visao_Contas_a_Pagar or fluxo_de_caixa_diario')
    parser.add_argument('--chunk-size', type=int, default=100000)
    args = parser.parse_args()

    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        logger.error("DATABASE_URL not set")
        return
    engine = create_engine(db_url, pool_pre_ping=True)

    # prepare file
    if args.signed_url:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.basename(args.signed_url))
        path = tmp.name
        tmp.close()
        download_signed_url(args.signed_url, path)
    elif args.file:
        path = args.file
    else:
        logger.error("Either --file or --signed_url must be provided")
        return

    # process
    stats = process_file(path, args.upload_id, args.mapping, args.table, engine, chunk_size=args.chunk_size)
    logger.info("Done: %s", stats)

if __name__ == '__main__':
    main()
