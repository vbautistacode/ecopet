# etl/utils.py
import uuid
import hashlib
import logging
import os
import csv

def generate_batch_id():
    return str(uuid.uuid4())

def file_hash(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

def setup_logger(name=__name__):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    return logger

import csv
from typing import Tuple, Dict
import io

def load_mapping(path: str, sep: str = ';') -> Tuple[list, dict]:
    """
    Carrega mapping CSV e retorna (rows_list, mapping_dict).
    Tenta utf-8, depois latin-1; em último caso lê com errors='replace'.
    """
    def _read_with_encoding(enc):
        with open(path, 'r', encoding=enc, errors='strict') as f:
            reader = csv.DictReader(f, delimiter=sep)
            rows = [r for r in reader]
            return rows

    # 1) try utf-8
    try:
        rows = _read_with_encoding('utf-8')
    except UnicodeDecodeError:
        # 2) try latin-1 / cp1252
        try:
            rows = _read_with_encoding('latin-1')
        except UnicodeDecodeError:
            # 3) fallback: replace invalid chars to avoid crash
            with open(path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f, delimiter=sep)
                rows = [r for r in reader]

    # build mapping dict coluna_origem -> nome_destino (ignore empty)
    mapping = {}
    for r in rows:
        src = (r.get('coluna_origem') or '').strip()
        tgt = (r.get('nome_destino') or '').strip()
        if src and tgt:
            mapping[src] = tgt

    return rows, mapping