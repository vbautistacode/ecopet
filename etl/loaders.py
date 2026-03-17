import logging
from typing import Union
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from etl.utils import connection_context

logger = logging.getLogger(__name__)


def load_to_staging(df: pd.DataFrame,
                    table_name: str,
                    upload_id: Union[int, str],
                    import_batch_id: str,
                    engine,
                    if_exists: str = 'append',
                    chunksize: int = 10_000) -> int:
    """
    Grava DataFrame em stg_{table_name} usando pandas.to_sql.
    engine pode ser SQLAlchemy Engine, SQLAlchemy Connection, psycopg.Connection ou raw DBAPI.
    Retorna número de linhas inseridas.
    """
    if df is None or df.empty:
        logger.info("No rows to load for %s", table_name)
        return 0

    # adicionar colunas de metadados se não existirem
    meta_cols = {
        "import_batch_id": import_batch_id,
        "file_name": None,
        "line_number": None,
        "file_hash": None,
    }
    for k, v in meta_cols.items():
        if k not in df.columns:
            df[k] = v

    try:
        with connection_context(engine) as conn:
            # pandas aceita SQLAlchemy Connection; connection_context garante compatibilidade
            df.to_sql(f"stg_{table_name}", conn, if_exists=if_exists, index=False, method="multi",
                      chunksize=chunksize, schema="public")
        logger.info("Loaded %d rows into stg_%s", len(df), table_name)
        return len(df)
    except Exception:
        logger.exception("Erro ao gravar em stg_%s", table_name)
        raise