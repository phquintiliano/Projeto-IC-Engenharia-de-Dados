from ingestion.ingestion_estrutura import IngestionConfig, run_ingestion_year

DENGUE = IngestionConfig(
    dataset="dengue",
    base_url="https://apidadosabertos.saude.gov.br/arboviroses/dengue",
    limit=20,
)


def run(nu_ano: int):
    run_ingestion_year(DENGUE, nu_ano)
