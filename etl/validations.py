# etl/validations.py
from .utils import setup_logger
import pandas as pd

logger = setup_logger(__name__)

def validate_required(df: pd.DataFrame, required_cols: list):
    errors = []
    for col in required_cols:
        if col not in df.columns:
            errors.append({'row': None, 'column': col, 'error': 'missing_column'})
            continue
        null_rows = df[df[col].isna()].index.tolist()
        for r in null_rows:
            errors.append({'row': int(r)+1, 'column': col, 'error': 'missing_value'})
    return errors

def validate_numeric(df: pd.DataFrame, numeric_cols: list):
    errors = []
    for col in numeric_cols:
        if col in df.columns:
            bad = df[~df[col].apply(lambda x: pd.isna(x) or isinstance(x, (int,float)))]
            for r in bad.index.tolist():
                errors.append({'row': int(r)+1, 'column': col, 'error': 'not_numeric'})
    return errors

def validate_cnpj_basic(df: pd.DataFrame, col):
    errors = []
    if col not in df.columns:
        return errors
    for idx, val in df[col].fillna('').astype(str).iteritems():
        digits = ''.join([c for c in val if c.isdigit()])
        if digits == '':
            errors.append({'row': int(idx)+1, 'column': col, 'error': 'empty_cnpj'})
        # basic length check
        if digits and len(digits) not in (11,14):
            errors.append({'row': int(idx)+1, 'column': col, 'error': 'invalid_length'})
    return errors