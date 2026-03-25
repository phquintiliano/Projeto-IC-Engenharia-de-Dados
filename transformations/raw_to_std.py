import pandas as pd

from ingestion.zika.config.schemas import (
    COLUNAS_ESPERADAS,
    COLUNAS_TEXTO,
    COLUNAS_DATA,
    COLUNAS_NUMERICAS,
    TAMANHO_CODIGOS,
)
from utils.normalization import (
    normalize_text_series,
    pad_code_series,
    to_datetime_series,
    to_numeric_nullable,
)

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.str.strip().str.lower()
    return out

def ensure_expected_columns(df: pd.DataFrame, expected_columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in expected_columns:
        if col not in out.columns:
            out[col] = pd.NA
    return out

def standardize_text_columns(df: pd.DataFrame, text_columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in text_columns:
        out[col] = normalize_text_series(out[col])
    return out

def standardize_code_columns(df: pd.DataFrame, code_sizes: dict[str, int]) -> pd.DataFrame:
    out = df.copy()
    for col, size in code_sizes.items():
        out[col] = pad_code_series(out[col], size)
    return out

def convert_numeric_columns(df: pd.DataFrame, numeric_columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in numeric_columns:
        out[col] = to_numeric_nullable(out[col])
    return out

def convert_date_columns(df: pd.DataFrame, date_columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in date_columns:
        out[col] = to_datetime_series(out[col])
    return out

def add_metadata_columns(df: pd.DataFrame, record_source: str) -> pd.DataFrame:
    out = df.copy()
    out["load_date"] = pd.Timestamp.utcnow()
    out["record_source"] = record_source
    return out

def build_bk_notificacao(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    dt_notific_str = out["dt_notific"].dt.strftime("%Y-%m-%d").astype("string").fillna("NA")
    out["bk_notificacao"] = (
        out["id_agravo"].fillna("NA").astype("string") + "|"
        + dt_notific_str + "|"
        + out["id_municip"].fillna("NA").astype("string") + "|"
        + out["id_unidade"].fillna("NA").astype("string")
    )
    return out

def transform_padronizar_raw(df_raw: pd.DataFrame, record_source: str) -> pd.DataFrame:
    df = df_raw.copy()
    df = standardize_column_names(df)
    df = ensure_expected_columns(df, COLUNAS_ESPERADAS)
    df = standardize_text_columns(df, COLUNAS_TEXTO)
    df = standardize_code_columns(df, TAMANHO_CODIGOS)
    df = convert_numeric_columns(df, COLUNAS_NUMERICAS)
    df = convert_date_columns(df, COLUNAS_DATA)
    df = add_metadata_columns(df, record_source)
    df = build_bk_notificacao(df)
    return df