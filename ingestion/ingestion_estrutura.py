from __future__ import annotations

import os
import io
import time
import random
from dataclasses import dataclass
from typing import Optional, Iterable, Any, Dict, List

import pandas as pd
import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

import requests
from requests.exceptions import RequestException


@dataclass(frozen=True)
class IngestionConfig:
    dataset: str
    base_url: str
    limit: int = 100
    bucket: str = "datalake"
    raw_prefix: str = "raw"


def _get_s3_cliente():
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    acess_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "admin123")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=acess_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket: str) -> None:
    buckets = s3.list_buckets().get("Buckets", [])
    existing = {b["Name"] for b in buckets}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)


def save_parquet_to_minio(s3, df: pd.DataFrame, bucket: str, key: str) -> None:
    ensure_bucket(s3, bucket)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    print(f"Gravado no MinIO : s3://{bucket}/{key}")


def fetch_page(
    base_url: str,
    nu_ano: int,
    limit: int,
    offset: int,
    max_retries: int = 10,
    base_delay: float = 1.0,
) -> pd.DataFrame:
    url = f"{base_url}?nu_ano={nu_ano}&limit={limit}&offset={offset}"

    attempt = 0
    while True:
        try:
            print(f"[INFO] Buscando {url} (tentativa {attempt + 1})", flush=True)
            s = pd.read_json(url, typ="series")

            params = s.get("parametros", None)
            if not isinstance(params, list):
                print(
                    f"[ERRO] Formato inesperado em {url}. Chaves {list(s.index)}",
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


def fetch_year(cfg: IngestionConfig, nu_ano: int) -> pd.DataFrame:
    frames = []
    offset = 0

    while True:
        df_page = fetch_page(
            base_url=cfg.base_url, nu_ano=nu_ano, limit=cfg.limit, offset=offset
        )

        if df_page.empty:
            break

        frames.append(df_page)

        if len(df_page) < cfg.limit:
            break

        offset += cfg.limit

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run_ingestion_year(cfg: IngestionConfig, nu_ano: int, s3=None) -> None:

    if s3 is None:
        s3 = _get_s3_cliente()

    df = fetch_year(cfg, nu_ano)

    if df.empty:
        print(
            f"[AVISO] Nenhum dado retornado para {cfg.dataset} em {nu_ano}", flush=True
        )
        return

    print(df.head(), flush=True)
    print(f"[INFO] Total {cfg.dataset} {nu_ano}: {len(df)}", flush=True)

    key = f"{cfg.raw_prefix}/{cfg.dataset}/ano={nu_ano}/{cfg.dataset}_{nu_ano}.parquet"
    save_parquet_to_minio(s3, df=df, bucket=cfg.bucket, key=key)
