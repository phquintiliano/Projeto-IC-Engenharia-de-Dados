from __future__ import annotations

import pandas as pd

from ingestion.zika.utils import hash_row


def build_sat_notificacao_evento(df: pd.DataFrame) -> pd.DataFrame:
    attrs = ["tp_not", "dt_notific", "sem_not", "nu_ano", "dt_sin_pri", "sem_pri", "dt_invest"]
    sat = df.copy()
    sat["hashdiff"] = sat.apply(lambda row: hash_row(row, attrs), axis=1)
    return sat[[
        "hk_notificacao",
        "hashdiff",
        "tp_not",
        "dt_notific",
        "sem_not",
        "nu_ano",
        "dt_sin_pri",
        "sem_pri",
        "dt_invest",
        "load_date",
        "record_source",
    ]].drop_duplicates().reset_index(drop=True)


def build_sat_notificacao_pessoa(df: pd.DataFrame) -> pd.DataFrame:
    attrs = ["nu_idade_n", "cs_sexo", "cs_gestant", "cs_raca", "cs_escol_n"]
    sat = df.copy()
    sat["hashdiff"] = sat.apply(lambda row: hash_row(row, attrs), axis=1)
    return sat[[
        "hk_notificacao",
        "hashdiff",
        "nu_idade_n",
        "cs_sexo",
        "cs_gestant",
        "cs_raca",
        "cs_escol_n",
        "load_date",
        "record_source",
    ]].drop_duplicates().reset_index(drop=True)


def build_sat_notificacao_encerramento(df: pd.DataFrame) -> pd.DataFrame:
    attrs = ["classi_fin", "criterio", "tpautocto", "doenca_tra", "evolucao", "dt_obito", "dt_encerra", "dt_digita"]
    sat = df.copy()
    sat["hashdiff"] = sat.apply(lambda row: hash_row(row, attrs), axis=1)
    return sat[[
        "hk_notificacao",
        "hashdiff",
        "classi_fin",
        "criterio",
        "tpautocto",
        "doenca_tra",
        "evolucao",
        "dt_obito",
        "dt_encerra",
        "dt_digita",
        "load_date",
        "record_source",
    ]].drop_duplicates().reset_index(drop=True)
