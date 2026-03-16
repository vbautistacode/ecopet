# etl/transformers.py
import pandas as pd
import os
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

# --- Compat wrappers públicos esperados por app/main.py ---
def _load_mapping_csv(path="mapping_v1_validated.csv"):
    """
    Tenta carregar um mapping CSV no formato (coluna_origem -> nome_destino).
    Retorna dict {coluna_origem: nome_destino} ou {} se não existir/erro.
    """
    try:
        if not os.path.exists(path):
            return {}
        df_map = pd.read_csv(path, sep=';', dtype=str)
        # colunas esperadas: coluna_origem;nome_destino
        df_map = df_map.fillna('')
        mapping = {}
        for _, r in df_map.iterrows():
            src = r.get('coluna_origem', '').strip()
            tgt = r.get('nome_destino', '').strip()
            if src and tgt:
                mapping[src] = tgt
        return mapping
    except Exception:
        logger.exception("Erro ao carregar mapping CSV")
        return {}

# função utilitária para normalizações padrão
def _basic_normalize(df):
    if df is None or df.empty:
        return df
    # normalizar datas: tenta detectar colunas com 'date' ou 'data' no nome
    date_cols = [c for c in df.columns if 'date' in c.lower() or 'data' in c.lower() or 'dt' in c.lower()]
    if date_cols:
        df = normalize_dates(df, date_cols, dayfirst=True)
    # normalizar números: colunas com 'valor','amount','revenue','price','total','quant' etc.
    num_indicators = ('valor','amount','revenue','price','total','quant','qty','cash','receb','pag')
    num_cols = [c for c in df.columns if any(k in c.lower() for k in num_indicators)]
    if num_cols:
        df = normalize_numbers(df, num_cols, thousand='.', decimal=',')
    return df

# wrapper genérico para transformar dados financeiros
def transform_finance(df, mapping=None):
    """
    Transformação financeira básica:
    - aplica mapping se fornecido ou se existir mapping_v1_validated.csv
    - normaliza datas e números
    - retorna DataFrame pronto para write_finance
    """
    if df is None:
        return None
    if mapping is None:
        mapping = _load_mapping_csv()
    if mapping:
        df = apply_mapping(df, mapping)
    df = _basic_normalize(df)
    # exemplos de ajustes financeiros comuns
    # garantir colunas numéricas existam
    for col in ('cash_in','cash_out','transfer_in','transfer_out','closing_balance'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

# wrapper para vendas
def transform_sales(df, mapping=None):
    """
    Transformação de vendas:
    - aplica mapping
    - normaliza datas/números
    - gera product_code se ausente
    """
    if df is None:
        return None
    if mapping is None:
        mapping = _load_mapping_csv()
    if mapping:
        df = apply_mapping(df, mapping)
    df = _basic_normalize(df)
    # gerar product_code se não existir
    if 'product_code' not in df.columns and ('product_group' in df.columns or 'product_name' in df.columns):
        df = generate_product_code(df, group_col='product_group', name_col='product_name', out_col='product_code')
    # garantir tipos
    if 'quantity' in df.columns:
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
    if 'revenue_net' in df.columns:
        df['revenue_net'] = pd.to_numeric(df['revenue_net'], errors='coerce').fillna(0)
    return df

# wrapper para operações (produtividade, staff, etc.)
def transform_ops(df, mapping=None):
    """
    Transformação operacional genérica: aplica mapping e normalizações.
    Ajuste depois para regras de produtividade específicas.
    """
    if df is None:
        return None
    if mapping is None:
        mapping = _load_mapping_csv()
    if mapping:
        df = apply_mapping(df, mapping)
    df = _basic_normalize(df)
    # exemplo: garantir employee_code presente
    if 'employee_code' in df.columns:
        df['employee_code'] = df['employee_code'].astype(str).str.strip()
    return df

# wrapper para marketing (simples)
def transform_marketing(df, mapping=None):
    """
    Transformação de marketing: aplica mapping e limpeza básica.
    """
    if df is None:
        return None
    if mapping is None:
        mapping = _load_mapping_csv()
    if mapping:
        df = apply_mapping(df, mapping)
    # limpeza básica de strings
    for c in df.select_dtypes(include=['object']).columns:
        df[c] = df[c].astype(str).str.strip()
    return df

# wrapper para clientes
def transform_clients(df, mapping=None):
    """
    Transformação de clientes: aplica mapping, normaliza CEP/CPF/telefones.
    """
    if df is None:
        return None
    if mapping is None:
        mapping = _load_mapping_csv()
    if mapping:
        df = apply_mapping(df, mapping)
    df = _basic_normalize(df)
    # strip non-digits para campos comuns
    for col in ('client_cpf','client_cep','client_phone','supplier_cnpj'):
        if col in df.columns:
            df[col] = strip_non_digits_series(df[col])
    return df