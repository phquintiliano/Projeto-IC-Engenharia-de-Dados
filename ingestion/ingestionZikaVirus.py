from __future__ import annotations

from ingestion.ingestion_estrutura import IngestionConfig, run_ingestion_year

ZIKA_CONFIG = IngestionConfig(
    dataset="zikavirus",
    base_url="https://apidadosabertos.saude.gov.br/arboviroses/zikavirus",
    limit=20,
    bucket="datalake",
    raw_prefix="raw",
)


def run_zika_year(nu_ano: int) -> dict:
    return run_ingestion_year(ZIKA_CONFIG, nu_ano)


def run_zika_range(start_year: int, end_year: int) -> list[dict]:
    results = []

    for year in range(start_year, end_year + 1):
        results.append(run_zika_year(year))

    return results
