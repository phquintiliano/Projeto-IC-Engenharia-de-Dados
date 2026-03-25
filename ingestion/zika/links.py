from __future__ import annotations

import pandas as pd

from ingestion.zika.utils import make_hash


def build_lnk_notificacao_contexto(df: pd.DataFrame) -> pd.DataFrame:
    lnk = df.copy()
    lnk["hk_lnk_notificacao_contexto"] = lnk.apply(
        lambda row: make_hash(
            row["bk_notificacao"],
            row["id_agravo"],
            row["id_municip"],
            row["id_mn_resi"],
            row["comuninf"],
            row["id_pais"],
            row["copaisinf"],
            row["id_unidade"],
        ),
        axis=1,
    )
    return lnk[[
        "hk_lnk_notificacao_contexto",
        "hk_notificacao",
        "hk_agravo",
        "hk_municipio_notificacao",
        "hk_municipio_residencia",
        "hk_municipio_infeccao",
        "hk_pais_residencia",
        "hk_pais_infeccao",
        "hk_unidade_saude",
        "load_date",
        "record_source",
    ]].drop_duplicates().reset_index(drop=True)
