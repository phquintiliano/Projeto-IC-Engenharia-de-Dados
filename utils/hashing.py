import hashlib
import pandas as pd

def normalize_hash_value(value):
    if pd.isna(value):
        return "NA"
    return str(value).strip().upper()

def make_hash(*values) -> str:
    text = "|".join(normalize_hash_value(v) for v in values)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def hash_row(row, cols):
    return make_hash(*(row.get(col) for col in cols))