# etl/loaders.py
import os
import io
import pandas as pd
from typing import Iterator, Optional, Dict, Any
from sqlalchemy import text
from .utils import connection_context
import logging

logger = logging.getLogger(__name__)

__all__ = ["read_chunks", "load_csv", "load_excel", "load_to_staging"]

def read_chunks(file_path: str, chunk_size: int = 100_000, sep: str = ",", encoding: str = "utf-8", **pd_read_csv_kwargs) -> Iterator[pd.DataFrame]:
    """
    Generator que lê CSV em chunks (pandas.read_csv chunksize).
    - Suporta arquivos locais e file-like paths.
    - Retorna DataFrames já com tipos brutos; transformações ficam a cargo do pipeline.
    """
    # detect simple Excel by extension and delegate
    ext = os.path.splitext(file_path)[1].lower()
    if ext in (".xls", ".xlsx"):
        # pandas.read_excel doesn't support chunks; read full and yield in slices
        df = pd.read_excel(file_path, engine="openpyxl")
        for start in range(0, len(df), chunk_size):
            yield df.iloc[start : start + chunk_size].reset_index(drop=True)
        return

    # CSV path: use pandas read_csv with chunksize
    reader = pd.read_csv(file_path, sep=sep, encoding=encoding, chunksize=chunk_size, **pd_read_csv_kwargs)
    for chunk in reader:
        yield chunk.reset_index(drop=True)


def load_csv(path: str, **kwargs) -> pd.DataFrame:
    """
    Conveniência: lê CSV inteiro (não em chunks).
    """
    return pd.read_csv(path, **kwargs)


def load_excel(path: str, sheet_name: Optional[str] = 0, **kwargs) -> pd.DataFrame:
    """
    Conveniência: lê Excel inteiro.
    """
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl", **kwargs)


def load_to_staging(df: pd.DataFrame,
                    table_name: str,
                    upload_id: Any,
                    import_batch_id: str,
                    engine_or_conn,
                    if_exists: str = "append",
                    chunksize: int = 10_000) -> int:
    """
    Grava DataFrame em stg_{table_name} usando pandas.to_sql.
    engine_or_conn pode ser SQLAlchemy Engine, SQLAlchemy Connection, psycopg.Connection ou raw DBAPI.
    Retorna número de linhas inseridas.
    """
    if df is None or df.empty:
        logger.debug("No rows to load for %s", table_name)
        return 0

    # ensure table_name normalized (no schema)
    stg_name = f"stg_{table_name}"

    # add metadata columns if missing (optional)
    if "import_batch_id" not in df.columns:
        df["import_batch_id"] = import_batch_id
    if "upload_id" not in df.columns:
        df["upload_id"] = upload_id

    try:
        # connection_context yields a SQLAlchemy Connection or compatible object accepted by pandas
        with connection_context(engine_or_conn) as conn:
            # pandas.to_sql expects SQLAlchemy connectable or URI; connection_context ensures compatibility
            df.to_sql(stg_name, conn, if_exists=if_exists, index=False, method="multi", chunksize=chunksize, schema="public")
        logger.info("Loaded %d rows into %s", len(df), stg_name)
        return len(df)
    except Exception:
        logger.exception("Erro ao gravar em %s", stg_name)
        raise