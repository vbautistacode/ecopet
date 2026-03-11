# etl/utils.py
import pandas as pd, unidecode, hashlib
from datetime import datetime

def normalize_cols(cols):
    return [unidecode.unidecode(c).strip().lower().replace(" ", "_").replace("-", "_") for c in cols]

def safe_to_numeric(s):
    return pd.to_numeric(s.astype(str).str.replace(r'[^\d\.\-]', '', regex=True), errors='coerce')

def normalize_mes(col):
    # retorna Timestamp com primeiro dia do mês
    return pd.to_datetime(col, errors='coerce').dt.to_period('M').dt.to_timestamp()

def row_hash(df):
    return df.fillna("").astype(str).agg("||".join, axis=1).apply(lambda s: hashlib.md5(s.encode()).hexdigest())

def safe_divide(a, b):
    b = b.replace({0: pd.NA})
    return a / b