from __future__ import annotations

from ingestion.ingestion_estrutura import IngestionConfig, run_ingestion_year


ZIKA_CONFIG = IngestionConfig(
    dataset="zika_virus",
    base_url="URL_DA_API_DO_ZIKA",
    limit=100,
    bucket="datalake",
    raw_prefix="raw",
)


def run_zika_year(nu_ano: int) -> None:
    run_ingestion_year(ZIKA_CONFIG, nu_ano)