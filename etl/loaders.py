# loaders.py -> load and normalize
from typing import Iterator, Optional, Union
import io
import os
import logging
import pandas as pd
from sqlalchemy.engine import Engine
from .utils import setup_logger

logger = setup_logger(__name__)

# -------------------------
# Low level helpers
# -------------------------
def _detect_extension(file_obj: Union[str, object]) -> str:
    """Detect file extension from path or UploadedFile.name attribute."""
    if isinstance(file_obj, str):
        return os.path.splitext(file_obj)[1].lstrip('.').lower()
    # file-like (Streamlit UploadedFile) may have .name
    name = getattr(file_obj, "name", None)
    if name:
        return os.path.splitext(name)[1].lstrip('.').lower()
    return ""

def _ensure_seekable(file_obj: Union[str, object]) -> Union[str, io.BytesIO, io.StringIO, object]:
    """
    Ensure the file-like object is seekable for pandas.
    - If str (path) -> return as-is.
    - If has read() and seek() -> reset and return.
    - If has read() but not seek() -> read into BytesIO/StringIO and return.
    """
    try:
        if isinstance(file_obj, str):
            return file_obj
        if hasattr(file_obj, "read"):
            # If it already supports seek, just rewind and return
            if hasattr(file_obj, "seek"):
                try:
                    file_obj.seek(0)
                except Exception:
                    pass
                return file_obj
            # Otherwise read content into buffer
            content = file_obj.read()
            if isinstance(content, bytes):
                return io.BytesIO(content)
            return io.StringIO(content)
    except Exception:
        logger.exception("Erro ao tornar file_obj seekable")
    return file_obj

def _detect_csv_delimiter(sample: Union[str, bytes]) -> str:
    """
    Heuristic to detect CSV delimiter from a sample string/bytes.
    Defaults to ';' if detection fails (project uses ';' by default).
    """
    try:
        if isinstance(sample, bytes):
            sample = sample.decode('utf-8', errors='ignore')
        # common delimiters
        candidates = [',', ';', '\t', '|']
        counts = {d: sample.count(d) for d in candidates}
        # choose delimiter with highest count
        best = max(counts, key=counts.get)
        # if counts are all zero, fallback to ';'
        if counts[best] == 0:
            return ';'
        return best
    except Exception:
        return ';'

# -------------------------
# Chunked readers
# -------------------------
def read_chunks(file_path_or_obj: Union[str, object], chunk_size: int = 100_000, **pd_read_kwargs) -> Iterator[pd.DataFrame]:
    """
    Yield pandas DataFrame chunks from CSV/XLSX. Accepts path or file-like.
    - For CSV: uses pandas.read_csv with chunksize.
    - For Excel: reads full sheet then yields slices (pandas.read_excel doesn't support chunks).
    """
    ext = _detect_extension(file_path_or_obj)
    file_or_path = _ensure_seekable(file_path_or_obj)

    if ext == 'csv':
        # detect delimiter if not provided
        if 'sep' not in pd_read_kwargs:
            # try to sample first 8KB
            try:
                if isinstance(file_or_path, (io.BytesIO, io.StringIO)):
                    pos = file_or_path.tell()
                    sample = file_or_path.read(8192)
                    file_or_path.seek(pos)
                elif hasattr(file_or_path, "read"):
                    pos = file_or_path.tell()
                    sample = file_or_path.read(8192)
                    file_or_path.seek(pos)
                else:
                    with open(file_or_path, 'rb') as f:
                        sample = f.read(8192)
                sep = _detect_csv_delimiter(sample)
            except Exception:
                sep = ';'
            pd_read_kwargs.setdefault('sep', sep)
        pd_read_kwargs.setdefault('dtype', str)
        pd_read_kwargs.setdefault('low_memory', False)
        for chunk in pd.read_csv(file_or_path, chunksize=chunk_size, **pd_read_kwargs):
            yield chunk
    elif ext in ('xls', 'xlsx'):
        # read full sheet in memory then chunk
        read_kwargs = dict(pd_read_kwargs)
        read_kwargs.setdefault('dtype', str)
        df = pd.read_excel(file_or_path, **read_kwargs)
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i + chunk_size]
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

# -------------------------
# Simple loaders (wrappers)
# -------------------------
def load_csv(file: Union[str, object], sep: Optional[str] = None, **pd_read_kwargs) -> pd.DataFrame:
    """
    Read CSV and return pandas.DataFrame.
    - Accepts path (str) or file-like (UploadedFile).
    - If sep is None, tries to detect delimiter; default fallback is ';'.
    """
    file_or_path = _ensure_seekable(file)
    read_kwargs = dict(pd_read_kwargs)
    if sep is not None:
        read_kwargs['sep'] = sep
    else:
        # try to detect delimiter from sample
        try:
            if isinstance(file_or_path, (io.BytesIO, io.StringIO)):
                pos = file_or_path.tell()
                sample = file_or_path.read(8192)
                file_or_path.seek(pos)
            elif hasattr(file_or_path, "read"):
                pos = file_or_path.tell()
                sample = file_or_path.read(8192)
                file_or_path.seek(pos)
            else:
                with open(file_or_path, 'rb') as f:
                    sample = f.read(8192)
            read_kwargs['sep'] = _detect_csv_delimiter(sample)
        except Exception:
            read_kwargs['sep'] = ';'
    read_kwargs.setdefault('dtype', str)
    read_kwargs.setdefault('low_memory', False)
    df = pd.read_csv(file_or_path, **read_kwargs)
    return df

def load_excel(file: Union[str, object], sheet_name: Union[int, str] = 0, **pd_read_kwargs) -> pd.DataFrame:
    """
    Read Excel and return pandas.DataFrame.
    - Accepts path (str) or file-like.
    """
    file_or_path = _ensure_seekable(file)
    read_kwargs = dict(pd_read_kwargs)
    read_kwargs.setdefault('sheet_name', sheet_name)
    read_kwargs.setdefault('dtype', str)
    df = pd.read_excel(file_or_path, **read_kwargs)
    return df

# -------------------------
# Staging loader
# -------------------------
def load_to_staging(df: pd.DataFrame, table_name: str, upload_id: Union[int, str], import_batch_id: str, engine: Engine, if_exists: str = 'append', chunksize: int = 10_000) -> int:
    if df is None or df.empty:
        logger.info("No rows to load into stg_%s", table_name)
        return 0
    df = df.copy()
    df['upload_id'] = upload_id
    df['import_batch_id'] = import_batch_id
    try:
        # use connection context to avoid pandas/sqlalchemy paramstyle mismatch
        with engine.begin() as conn:
            df.to_sql(f"stg_{table_name}", conn, if_exists=if_exists, index=False, method='multi', chunksize=chunksize, schema="public")
        logger.info("Loaded %d rows into stg_%s", len(df), table_name)
        return len(df)
    except Exception:
        logger.exception("Erro ao gravar em stg_%s", table_name)
        raise

# -------------------------
# High level helper: load file to staging (streaming aware)
# -------------------------
def load_file_to_staging(file: Union[str, object], table_name: str, upload_id: Union[int, str], import_batch_id: str, engine: Engine, chunk_threshold_bytes: int = 10 * 1024 * 1024, chunk_size: int = 100_000, **read_kwargs) -> int:
    """
    High-level helper that decides between streaming (read_chunks -> load_to_staging per chunk)
    or reading full file into memory and loading once.
    - chunk_threshold_bytes: if file-like has .size and exceeds threshold, use streaming.
    - read_kwargs passed to read_chunks/load_csv/load_excel.
    - Returns total rows loaded.
    """
    ext = _detect_extension(file)
    # decide streaming by size if available
    size = getattr(file, "size", None)
    use_chunks = False
    if size and isinstance(size, (int, float)) and size > chunk_threshold_bytes:
        use_chunks = True

    total = 0
    if use_chunks and ext == 'csv':
        logger.info("Using chunked upload for %s (size=%s)", getattr(file, "name", "<file>"), size)
        for chunk in read_chunks(file, chunk_size=chunk_size, **read_kwargs):
            try:
                rows = load_to_staging(chunk, table_name, upload_id, import_batch_id, engine)
                total += rows
            except Exception:
                logger.exception("Erro ao carregar chunk para stg_%s", table_name)
                # continue with next chunk or decide to abort based on policy
        return total
    else:
        # read full file and load once
        if ext == 'csv':
            df = load_csv(file, **read_kwargs)
        elif ext in ('xls', 'xlsx'):
            df = load_excel(file, **read_kwargs)
        else:
            raise ValueError(f"Unsupported file extension: {ext}")
        total = load_to_staging(df, table_name, upload_id, import_batch_id, engine)
        return total