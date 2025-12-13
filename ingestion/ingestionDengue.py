import os
import io
import time
import random
import pandas as pd
import boto3
from botocore.client import Config
from urllib.error import URLError, HTTPError
from http.client import IncompleteRead
from datetime import datetime

BASE = "https://apidadosabertos.saude.gov.br/arboviroses/dengue"
LIMIT = 100


def fetch_page(
    nu_ano: int,
    offset: int,
    max_retries: int = 10,
    base_delay: float = 1.0,
) -> pd.DataFrame:
    url = f"{BASE}?nu_ano={nu_ano}&limit={LIMIT}&offset={offset}"

    attempt = 0
    while True:
        try:
            print(f"[INFO] Buscando {url} (tentativa {attempt + 1})", flush=True)
            s = pd.read_json(url, typ="series")

            params = s.get("parametros", None)
            if not isinstance(params, list):
                print(
                    f"[ERRO] Formato inesperado em {url}. Chaves: {list(s.index)}",
                    flush=True,
                )
                return pd.DataFrame()

            return pd.DataFrame(params)

        except (ValueError, URLError, HTTPError, OSError, IncompleteRead) as e:
            attempt += 1
            if attempt > max_retries:
                print(
                    f"[ERRO] Falha ao baixar/parsear {url} após {max_retries} tentativas: {e}",
                    flush=True,
                )
                return pd.DataFrame()

            max_sleep = base_delay * (2 ** (attempt - 1))
            sleep_time = random.uniform(0, max_sleep)

            print(
                f"[WARN] Erro ao acessar {url}: {e}. "
                f"Tentativa {attempt}/{max_retries}. "
                f"Aguardando {sleep_time:.2f}s antes de tentar novamente...",
                flush=True,
            )
            time.sleep(sleep_time)


def fetch_ano(nu_ano: int) -> pd.DataFrame:
    frames = []
    offset = 0
    while True:
        df_page = fetch_page(nu_ano, offset)
        if df_page.empty:
            break
        frames.append(df_page)
        if len(df_page) < LIMIT:
            break
        offset += LIMIT
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def save_parquet_to_minio(df: pd.DataFrame, bucket: str, key: str):
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "admin123")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    buckets = s3.list_buckets().get("Buckets", [])
    existing = {b["Name"] for b in buckets}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    print(f"✅ Gravado no MinIO: s3://{bucket}/{key}")


def run_ingestion_year(nu_ano: int):
    df = fetch_ano(nu_ano)
    if df.empty:
        print(f"[AVISO] Nenhum dado retornado para {nu_ano}")
        return
    print(df.head())
    print(f"Total {nu_ano}: {len(df)}")
    save_parquet_to_minio(
        df,
        bucket="datalake",
        key=f"raw/dengue/ano={nu_ano}/dengue_{nu_ano}.parquet",
    )


def run_ingestion_range(start_year: int = 2020, end_year: int | None = None):
    if end_year is None:
        end_year = datetime.now().year

    for year in range(start_year, end_year + 1):
        print("\n==============================")
        print(f" Iniciando ingestão do ano {year}")
        print("==============================")
        run_ingestion_year(year)
