# loaders.py -> load and normalize
import pandas as pd, unidecode, hashlib, datetime as dt

def normalize_cols(cols):
    return [unidecode.unidecode(c).strip().lower().replace(" ", "_") for c in cols]

def load_csv(path, parse_dates=None):
    df = pd.read_csv(path, parse_dates=parse_dates)
    df.columns = normalize_cols(df.columns)
    df['source_file'] = path
    df['loaded_at'] = pd.Timestamp.now()
    # row hash
    df['row_hash'] = df.astype(str).apply(lambda r: hashlib.md5("||".join(r).encode()).hexdigest(), axis=1)
    return df

# transform example: aggregate receita and despesa then join
rec = load_csv("receita.csv")
desp = load_csv("despesa.csv")

rec['mes'] = pd.to_datetime(rec['mes']).dt.to_period('M').dt.to_timestamp()
desp['mes'] = pd.to_datetime(desp['mes']).dt.to_period('M').dt.to_timestamp()

rec_sum = rec.groupby('mes', as_index=False)['valor'].sum().rename(columns={'valor':'receita'})
desp_sum = desp.groupby('mes', as_index=False)['valor'].sum().rename(columns={'valor':'despesa'})

df = rec_sum.merge(desp_sum, on='mes', how='outer').fillna(0)
df['lucro'] = df['receita'] - df['despesa']
df['ebitda'] = df.get('ebitda', df['receita'] - df['despesa'])
df['roi'] = (df['receita'] - df['despesa']) / df['investimentos'].replace({0: pd.NA})
