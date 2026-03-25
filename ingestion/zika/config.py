from __future__ import annotations

COLUNAS_ESPERADAS = [
    "tp_not",
    "id_agravo",
    "dt_notific",
    "sem_not",
    "nu_ano",
    "id_municip",
    "id_regiona",
    "dt_sin_pri",
    "sem_pri",
    "nu_idade_n",
    "cs_sexo",
    "cs_gestant",
    "cs_raca",
    "cs_escol_n",
    "id_mn_resi",
    "id_rg_resi",
    "id_pais",
    "dt_invest",
    "id_unidade",
    "classi_fin",
    "criterio",
    "tpautocto",
    "copaisinf",
    "comuninf",
    "doenca_tra",
    "evolucao",
    "dt_obito",
    "dt_encerra",
    "dt_digita",
]

COLUNAS_TEXTO = [
    "tp_not",
    "id_agravo",
    "sem_not",
    "id_regiona",
    "sem_pri",
    "cs_sexo",
    "cs_gestant",
    "cs_raca",
    "cs_escol_n",
    "id_rg_resi",
    "classi_fin",
    "criterio",
    "tpautocto",
    "doenca_tra",
    "evolucao",
]

COLUNAS_DATA = [
    "dt_notific",
    "dt_sin_pri",
    "dt_invest",
    "dt_obito",
    "dt_encerra",
    "dt_digita",
]

COLUNAS_NUMERICAS = ["nu_ano", "nu_idade_n"]

TAMANHO_CODIGOS = {
    "id_agravo": 5,
    "id_municip": 6,
    "id_mn_resi": 6,
    "comuninf": 6,
    "id_pais": 4,
    "copaisinf": 4,
    "id_unidade": 7,
}

RENOMEAR_COLUNAS = {
    "id_municipio": "id_municip",
    "id_munic": "id_municip",
    "id_municip": "id_municip",
    "id_mn_resid": "id_mn_resi",
    "id_resi": "id_mn_resi",
    "paisinf": "copaisinf",
    "pais_inf": "copaisinf",
    "municipio_inf": "comuninf",
    "id_unid": "id_unidade",
}
