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

BASE = "https://apidadosabertos.saude.gov.br/arboviroses/febre-amarela-humanos-primatas-nao-humanos"
LIMIT = 20


def fetch_page(
    offset: int, max_retries: int = 10, base_delay: float = 1.0
) -> pd.DataFrame:
    url = f"{BASE}?limit={LIMIT}&offset={offset}"
    attempt = 0

    while True:
        try:
            print(f"[INFO] Buscando {url} (tentativa {attempt + 1})", flush=True)
            payload = pd.read_json(url, typ="series")

            rows = payload.get("febre_amarela_humanos_primatas", None)
            if not isinstance(rows, list):
                raise ValueError(
                    f"Resposta sem lista esperada. Chaves: {list(payload.index)}"
                )

            return pd.DataFrame(rows)

        except (ValueError, URLError, HTTPError, OSError, IncompleteRead) as e:
            attempt += 1
            if attempt > max_retries:
                print(f"[ERRO] Falha após {max_retries} tentativas: {e}", flush=True)
                return pd.DataFrame()

            max_sleep = base_delay * (2 ** (attempt - 1))
            sleep_time = random.uniform(0, max_sleep)
            print(
                f"[WARN] Erro ao acessar {url}: {e}. Tentativa {attempt}/{max_retries}. "
                f"Aguardando {sleep_time:.2f}s...",
                flush=True,
            )
            time.sleep(sleep_time)


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
    print(f"Gravado no MinIO: s3://{bucket}/{key}")


def fetch_all(limit: int = LIMIT, max_pages: int | None = None) -> pd.DataFrame:
    frames = []
    offset = 0
    pages = 0

    while True:
        df_page = fetch_page(offset)
        if df_page.empty:
            break

        frames.append(df_page)

        if len(df_page) < limit:
            break

        offset += limit
        pages += 1

        if max_pages is not None and pages >= max_pages:
            print(f"[WARN] Parando por max_pages={max_pages}")
            break

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run_ingestion():
    df = fetch_all()

    if df.empty:
        print("[AVISO] Nenhum dado retornado da API de febre amarela (PNH).")
        return

    print(df.head())
    print(f"Total de registros: {len(df)}")

    save_parquet_to_minio(
        df,
        bucket="datalake",
        key="raw/febre_amarela/febre_amarela_primatas_nao_humanos.parquet",
    )


if __name__ == "__main__":
    run_ingestion()
