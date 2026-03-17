# etl/loaders.py
import os
import io
import pandas as pd
import csv
import chardet

from pandas.errors import EmptyDataError
from typing import Iterator, Optional, Dict, Any
from sqlalchemy import text
from .utils import connection_context
import logging

logger = logging.getLogger(__name__)

__all__ = ["read_chunks", "load_csv", "load_excel", "load_to_staging"]

def _detect_encoding(path, nbytes=4096):
    with open(path, "rb") as f:
        raw = f.read(nbytes)
    res = chardet.detect(raw)
    return res.get("encoding") or "utf-8"

def _detect_delimiter(path: str, sample_lines: int = 5) -> str:
    with open(path, "rb") as f:
        raw = b""
        for _ in range(sample_lines):
            line = f.readline()
            if not line:
                break
            raw += line
    sample = raw.decode("utf-8", errors="replace")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        return dialect.delimiter
    except Exception:
        return ";"

def load_csv(path: str, sep: Optional[str] = None, encoding: Optional[str] = None, **kwargs) -> pd.DataFrame:
    if not os.path.isfile(path):
        logger.error("Arquivo não encontrado: %s", path)
        return pd.DataFrame()
    if sep is None:
        sep = _detect_delimiter(path)
    encodings = [encoding] if encoding else []
    encodings += ["utf-8", "cp1252", "latin-1"]
    last_exc = None
    for enc in [e for e in encodings if e]:
        try:
            df = pd.read_csv(path, sep=sep, encoding=enc, **kwargs)
            if df is None or df.shape[1] == 0:
                return pd.DataFrame()
            return df
        except EmptyDataError:
            return pd.DataFrame()
        except UnicodeDecodeError as e:
            last_exc = e
            continue
        except Exception as e:
            last_exc = e
            continue
    try:
        fallback_enc = encodings[-1] if encodings else "utf-8"
        df = pd.read_csv(path, sep=sep, encoding=fallback_enc, errors="replace", **kwargs)
        if df is None or df.shape[1] == 0:
            return pd.DataFrame()
        return df
    except Exception:
        raise last_exc or RuntimeError("Failed to read CSV")

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