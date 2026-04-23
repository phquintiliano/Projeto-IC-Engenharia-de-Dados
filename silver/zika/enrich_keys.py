from __future__ import annotations

import pandas as pd

from utils.hashing import make_hash


def enrich_with_hks(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out['hk_notificacao'] = out['bk_notificacao'].apply(make_hash)
    out['hk_agravo'] = out['id_agravo'].apply(lambda x: make_hash(x) if pd.notna(x) else pd.NA)

    out['hk_municipio_notificacao'] = out['id_municip'].apply(lambda x: make_hash(x) if pd.notna(x) else pd.NA)
    out['hk_municipio_residencia'] = out['id_mn_resi'].apply(lambda x: make_hash(x) if pd.notna(x) else pd.NA)
    out['hk_municipio_infeccao'] = out['comuninf'].apply(lambda x: make_hash(x) if pd.notna(x) else pd.NA)

    out['hk_pais_residencia'] = out['id_pais'].apply(lambda x: make_hash(x) if pd.notna(x) else pd.NA)
    out['hk_pais_infeccao'] = out['copaisinf'].apply(lambda x: make_hash(x) if pd.notna(x) else pd.NA)

    out['hk_unidade_saude'] = out['id_unidade'].apply(lambda x: make_hash(x) if pd.notna(x) else pd.NA)
    return out
