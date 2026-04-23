from __future__ import annotations

import pandas as pd

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


def ensure_expected_columns(
    df: pd.DataFrame, expected_columns: list[str]
) -> pd.DataFrame:
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


def standardize_code_columns(
    df: pd.DataFrame, code_sizes: dict[str, int]
) -> pd.DataFrame:
    out = df.copy()
    for col, size in code_sizes.items():
        out[col] = pad_code_series(out[col], size)
    return out


def convert_numeric_columns(
    df: pd.DataFrame, numeric_columns: list[str]
) -> pd.DataFrame:
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


def transform_raw_data(
    df_raw: pd.DataFrame,
    *,
    expected_columns: list[str],
    text_columns: list[str],
    code_sizes: dict[str, int],
    numeric_columns: list[str],
    date_columns: list[str],
    record_source: str,
) -> pd.DataFrame:
    df = df_raw.copy()
    df = standardize_column_names(df)
    df = ensure_expected_columns(df, expected_columns)
    df = standardize_text_columns(df, text_columns)
    df = standardize_code_columns(df, code_sizes)
    df = convert_numeric_columns(df, numeric_columns)
    df = convert_date_columns(df, date_columns)
    df = add_metadata_columns(df, record_source)
    return df
