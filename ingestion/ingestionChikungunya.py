from ingestion.ingestion_estrutura import IngestionConfig, run_ingestion_year

CHIK = IngestionConfig(
    dataset="chikungunya",
    base_url="https://apidadosabertos.saude.gov.br/arboviroses/chikungunya",
    limit=20,
)


def run(nu_ano: int):
    run_ingestion_year(CHIK, nu_ano)
