from ingestion.ingestion_estrutura import IngestionConfig, run_ingestion_year

ZIKA = IngestionConfig(
    dataset="zikavirus",
    base_url="https://apidadosabertos.saude.gov.br/arboviroses/zikavirus",
    limit=100,
)


def run(nu_ano: int):
    run_ingestion_year(ZIKA, nu_ano)
