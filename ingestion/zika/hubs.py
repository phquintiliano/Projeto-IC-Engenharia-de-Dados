from __future__ import annotations

import pandas as pd


def build_hub_notificacao(df: pd.DataFrame) -> pd.DataFrame:
    hub = df[["hk_notificacao", "bk_notificacao", "load_date", "record_source"]].drop_duplicates()
    return hub.drop_duplicates(subset=["hk_notificacao"]).reset_index(drop=True)


def build_hub_agravo(df: pd.DataFrame) -> pd.DataFrame:
    hub = df[["hk_agravo", "id_agravo", "load_date", "record_source"]].copy()
    hub = hub[hub["id_agravo"].notna()].rename(columns={"id_agravo": "bk_agravo"})
    return hub.drop_duplicates(subset=["hk_agravo"]).reset_index(drop=True)


def build_hub_municipio(df: pd.DataFrame) -> pd.DataFrame:
    mun_not = df[["hk_municipio_notificacao", "id_municip", "load_date", "record_source"]].rename(
        columns={"hk_municipio_notificacao": "hk_municipio", "id_municip": "bk_municipio"}
    )
    mun_res = df[["hk_municipio_residencia", "id_mn_resi", "load_date", "record_source"]].rename(
        columns={"hk_municipio_residencia": "hk_municipio", "id_mn_resi": "bk_municipio"}
    )
    mun_inf = df[["hk_municipio_infeccao", "comuninf", "load_date", "record_source"]].rename(
        columns={"hk_municipio_infeccao": "hk_municipio", "comuninf": "bk_municipio"}
    )
    hub = pd.concat([mun_not, mun_res, mun_inf], ignore_index=True)
    hub = hub[hub["bk_municipio"].notna()]
    return hub.drop_duplicates(subset=["hk_municipio"]).reset_index(drop=True)


def build_hub_pais(df: pd.DataFrame) -> pd.DataFrame:
    pais_res = df[["hk_pais_residencia", "id_pais", "load_date", "record_source"]].rename(
        columns={"hk_pais_residencia": "hk_pais", "id_pais": "bk_pais"}
    )
    pais_inf = df[["hk_pais_infeccao", "copaisinf", "load_date", "record_source"]].rename(
        columns={"hk_pais_infeccao": "hk_pais", "copaisinf": "bk_pais"}
    )
    hub = pd.concat([pais_res, pais_inf], ignore_index=True)
    hub = hub[hub["bk_pais"].notna()]
    return hub.drop_duplicates(subset=["hk_pais"]).reset_index(drop=True)


def build_hub_unidade_saude(df: pd.DataFrame) -> pd.DataFrame:
    hub = df[["hk_unidade_saude", "id_unidade", "load_date", "record_source"]].copy()
    hub = hub[hub["id_unidade"].notna()].rename(columns={"id_unidade": "bk_unidade_saude"})
    return hub.drop_duplicates(subset=["hk_unidade_saude"]).reset_index(drop=True)
