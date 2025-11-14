# 🚀 Data Lake Stack com Docker: Apache Airflow, Spark e MinIO

Este projeto configura um ambiente completo para pipelines de dados usando Docker Compose com:

- **Apache Airflow** (orquestração de workflows)
- **Apache Spark** (processamento distribuído)
- **MinIO** (armazenamento S3 compatível)
- **PostgreSQL** (banco para o Airflow)

---

## 📦 Pré-requisitos

- Docker instalado: [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)
- Docker Compose instalado (vem junto com o Docker Desktop)
- Para inicializar digite no terminal docker-compose up --build --scale spark-worker=3 -d

---

## 📁 Estrutura do Projeto

docker exec -it airflow-webserver bash

airflow@airflow-webserver:/opt/airflow$

python -c "from ingestion import ingestionZikaVirus; ingestionZikaVirus.run_ingestion_range(start_year=2016)"
