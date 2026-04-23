from __future__ import annotations

from silver.zika.hubs import (
    build_hub_agravo,
    build_hub_municipio,
    build_hub_notificacao,
    build_hub_pais,
    build_hub_unidade_saude,
)
from silver.zika.links import build_lnk_notificacao_contexto
from silver.zika.satellites import (
    build_sat_notificacao_encerramento,
    build_sat_notificacao_evento,
    build_sat_notificacao_pessoa,
)


def build_data_vault_tables(df):
    return {
        'hub_notificacao': build_hub_notificacao(df),
        'hub_agravo': build_hub_agravo(df),
        'hub_municipio': build_hub_municipio(df),
        'hub_pais': build_hub_pais(df),
        'hub_unidade_saude': build_hub_unidade_saude(df),
        'lnk_notificacao_contexto': build_lnk_notificacao_contexto(df),
        'sat_notificacao_evento': build_sat_notificacao_evento(df),
        'sat_notificacao_pessoa': build_sat_notificacao_pessoa(df),
        'sat_notificacao_encerramento': build_sat_notificacao_encerramento(df),
    }
