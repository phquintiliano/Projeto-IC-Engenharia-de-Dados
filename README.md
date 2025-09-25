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

$ docker run --rm -v "$(pwd -W):/app" -w //app python:3.11-slim \
 sh -lc 'python -V; which python; python -m pip install -U pip pandas && python data/hello_spark.py'
