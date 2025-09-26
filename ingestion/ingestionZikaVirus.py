import os
import io
import pandas as pd
import boto3
from botocore.client import Config
from urllib.error import URLError, HTTPError

BASE = "https://apidadosabertos.saude.gov.br/arboviroses/zikavirus"
LIMIT = 20


def fetch_page(nu_ano: int, offset: int) -> pd.DataFrame:
    url = f"{BASE}?nu_ano={nu_ano}&limit={LIMIT}&offset={offset}"
    try:
        s = pd.read_json(url, typ="series")
    except (ValueError, URLError, HTTPError, OSError) as e:
        print(f"[ERRO] Falha ao baixar/parsear {url}: {e}", flush=True)
        return pd.DataFrame()

    params = s.get("parametros", None)
    if not isinstance(params, list):
        print(
            f"[ERRO] Formato inesperado em {url}. Chaves: {list(s.index)}", flush=True
        )
        return pd.DataFrame()

    return pd.DataFrame(params)


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

    # garante o bucket
    buckets = s3.list_buckets().get("Buckets", [])
    existing = {b["Name"] for b in buckets}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

    # escreve Parquet em memória e envia
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


if __name__ == "__main__":
    df2016 = fetch_ano(2016)
    print(df2016.head())
    print("Total 2016:", len(df2016))
    save_parquet_to_minio(
        df2016,
        bucket="datalake",
        key="raw/zikavirus/ano=2016/zikavirus_2016.parquet",
    )
