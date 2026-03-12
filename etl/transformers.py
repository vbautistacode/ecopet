# etl/transformers.py
import pandas as pd
from .utils import setup_logger
import re

logger = setup_logger(__name__)

def apply_mapping(df: pd.DataFrame, mapping: dict):
    """Rename columns according to mapping dict (coluna_origem -> nome_destino)."""
    # Only rename columns that exist
    rename_map = {k: v for k, v in mapping.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    return df

def normalize_dates(df: pd.DataFrame, cols, dayfirst=True):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors='coerce', dayfirst=dayfirst)
    return df

def normalize_numbers(df: pd.DataFrame, cols, thousand='.', decimal=','):
    for c in cols:
        if c in df.columns:
            s = df[c].astype(str).str.strip()
            # remove thousand separators and replace decimal
            if thousand:
                s = s.str.replace(thousand, '', regex=False)
            if decimal:
                s = s.str.replace(decimal, '.', regex=False)
            s = s.replace(r'^\s*$', None, regex=True)
            df[c] = pd.to_numeric(s, errors='coerce')
    return df

def strip_non_digits_series(s):
    return s.astype(str).str.replace(r'\D+', '', regex=True).replace(r'^\s*$', None, regex=True)

def generate_product_code(df: pd.DataFrame, group_col='product_group', name_col='product_name', out_col='product_code'):
    if out_col not in df.columns:
        df[out_col] = (df.get(group_col, '').fillna('') + '|' + df.get(name_col, '').fillna('')).apply(
            lambda x: hashlib_short(x))
    return df

def hashlib_short(s):
    import hashlib
    return hashlib.sha1(s.encode('utf-8')).hexdigest()[:12]