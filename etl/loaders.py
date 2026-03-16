# loaders.py -> load and normalize
import pandas as pd
from sqlalchemy import text
from .utils import setup_logger

logger = setup_logger(__name__)

def read_chunks(file_path: str, chunk_size: int = 100000, **pd_read_kwargs):
    """Yield pandas DataFrame chunks from CSV/XLSX. Detect extension."""
    ext = file_path.split('.')[-1].lower()
    if ext in ('csv',):
        for chunk in pd.read_csv(file_path, chunksize=chunk_size, dtype=str, low_memory=False, **pd_read_kwargs):
            yield chunk
    elif ext in ('xls','xlsx'):
        # read full sheet in memory then chunk
        df = pd.read_excel(file_path, dtype=str)
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i+chunk_size]
    else:
        raise ValueError("Unsupported file extension: " + ext)

def load_to_staging(df, table_name: str, upload_id: int, import_batch_id: str, engine):
    """Append df to staging table stg_{table_name} with upload/import metadata."""
    df = df.copy()
    df['upload_id'] = upload_id
    df['import_batch_id'] = import_batch_id
    # write in chunks to avoid memory spikes
    df.to_sql(f"stg_{table_name}", engine, if_exists='append', index=False, method='multi', chunksize=10000)
    logger.info("Loaded %d rows into stg_%s", len(df), table_name)

# --- Compat wrappers para API esperada pelo app ---
import io
from typing import Union

def _ensure_seekable(file_obj):
    try:
        if isinstance(file_obj, str):
            return file_obj
        if hasattr(file_obj, "read") and hasattr(file_obj, "seek"):
            file_obj.seek(0)
            return file_obj
        if hasattr(file_obj, "read"):
            content = file_obj.read()
            if isinstance(content, bytes):
                return io.BytesIO(content)
            return io.StringIO(content)
    except Exception:
        pass
    return file_obj

def load_csv(file: Union[str, object], sep: str = ";", **pd_read_kwargs):
    """
    Lê CSV e retorna pandas.DataFrame.
    Usa sep=';' por padrão; ajuste se seus CSVs usam vírgula.
    """
    file_or_path = _ensure_seekable(file)
    read_kwargs = {"sep": sep, "dtype": str, "low_memory": False}
    read_kwargs.update(pd_read_kwargs or {})
    df = pd.read_csv(file_or_path, **read_kwargs)
    return df

def load_excel(file: Union[str, object], sheet_name=0, **pd_read_kwargs):
    """
    Lê Excel e retorna pandas.DataFrame.
    """
    file_or_path = _ensure_seekable(file)
    read_kwargs = {"sheet_name": sheet_name, "dtype": str}
    read_kwargs.update(pd_read_kwargs or {})
    df = pd.read_excel(file_or_path, **read_kwargs)
    return df