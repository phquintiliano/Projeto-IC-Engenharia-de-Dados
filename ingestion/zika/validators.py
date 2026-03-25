from __future__ import annotations

import pandas as pd


def summarize_standardized_data(df: pd.DataFrame) -> dict[str, int]:
    return {
        "rows": len(df),
        "null_id_agravo": int(df["id_agravo"].isna().sum()),
        "null_dt_notific": int(df["dt_notific"].isna().sum()),
        "null_id_municip": int(df["id_municip"].isna().sum()),
        "null_id_unidade": int(df["id_unidade"].isna().sum()),
        "duplicate_bk_notificacao": int(df["bk_notificacao"].duplicated().sum()),
    }


def validate_required_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}")
