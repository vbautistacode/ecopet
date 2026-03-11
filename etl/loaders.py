# etl/loaders.py
import pandas as pd
import unidecode

def _normalize_cols(cols):
    cols = [unidecode.unidecode(c).strip().lower().replace(" ", "_") for c in cols]
    return cols

def load_csv(file, dtype_map=None, parse_dates=None):
    df = pd.read_csv(file, dtype=dtype_map, parse_dates=parse_dates)
    df.columns = _normalize_cols(df.columns)
    return df
