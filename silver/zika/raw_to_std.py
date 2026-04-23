from __future__ import annotations

import pandas as pd

from silver.zika.config import (
    COLUNAS_DATA,
    COLUNAS_ESPERADAS,
    COLUNAS_NUMERICAS,
    COLUNAS_TEXTO,
    TAMANHO_CODIGOS,
)
from transformations.raw_to_std import transform_raw_data


def build_bk_notificacao(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    dt_notific_str = out['dt_notific'].dt.strftime('%Y-%m-%d').astype('string').fillna('NA')
    out['bk_notificacao'] = (
        out['id_agravo'].fillna('NA').astype('string') + '|'
        + dt_notific_str + '|'
        + out['id_municip'].fillna('NA').astype('string') + '|'
        + out['id_unidade'].fillna('NA').astype('string')
    )
    return out


def transform_padronizar_raw(df_raw: pd.DataFrame, record_source: str = 'minio_raw_zika') -> pd.DataFrame:
    df = transform_raw_data(
        df_raw,
        expected_columns=COLUNAS_ESPERADAS,
        text_columns=COLUNAS_TEXTO,
        code_sizes=TAMANHO_CODIGOS,
        numeric_columns=COLUNAS_NUMERICAS,
        date_columns=COLUNAS_DATA,
        record_source=record_source,
    )
    df = build_bk_notificacao(df)
    return df
