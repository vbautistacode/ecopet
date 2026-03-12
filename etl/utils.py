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

def load_mapping(path):
    """
    Load mapping CSV (semicolon separated) into list of dicts and mapping dict.
    Returns: (rows, mapping_dict) where mapping_dict maps coluna_origem -> nome_destino
    """
    rows = []
    mapping = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for r in reader:
            rows.append(r)
            origem = r.get('coluna_origem')
            destino = r.get('nome_destino')
            if origem and destino:
                mapping[origem.strip()] = destino.strip()
    return rows, mapping