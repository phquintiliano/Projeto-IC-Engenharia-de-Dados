import pandas as pd

def normalize_text_value(value):
    if pd.isna(value):
        return pd.NA
    value = str(value).strip()
    if value == "":
        return pd.NA
    return value.upper()

def normalize_text_series(series: pd.Series) -> pd.Series:
    return series.apply(normalize_text_value).astype("string")

def pad_code_series(series: pd.Series, size: int) -> pd.Series:
    s = normalize_text_series(series)
    return s.str.zfill(size)

def to_datetime_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")

def to_numeric_nullable(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")