from __future__ import annotations

import io
import os
from typing import Iterable

import boto3
import pandas as pd
from botocore.client import Config

from silver.zika.config import DEFAULT_BUCKET, RAW_PREFIX, SILVER_PREFIX


def get_s3_client():
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "admin123")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def resolve_s3_client(s3=None):
    return s3 or get_s3_client()


def list_raw_keys(
    s3=None, bucket: str = DEFAULT_BUCKET, prefix: str = RAW_PREFIX
) -> list[str]:
    s3 = resolve_s3_client(s3)
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)

    return sorted(keys)


def read_parquet_from_minio(
    key: str, s3=None, bucket: str = DEFAULT_BUCKET
) -> pd.DataFrame:
    s3 = resolve_s3_client(s3)
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(response["Body"].read()))


def load_raw_zika_data(
    keys: Iterable[str] | None = None, s3=None, bucket: str = DEFAULT_BUCKET
) -> pd.DataFrame:
    s3 = resolve_s3_client(s3)
    selected_keys = (
        list(keys) if keys is not None else list_raw_keys(s3=s3, bucket=bucket)
    )

    frames = [
        read_parquet_from_minio(key=key, s3=s3, bucket=bucket) for key in selected_keys
    ]
    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def save_parquet_to_minio(
    df: pd.DataFrame, key: str, s3=None, bucket: str = DEFAULT_BUCKET
) -> None:
    s3 = resolve_s3_client(s3)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )


def save_silver_tables(
    tables: dict[str, pd.DataFrame],
    s3=None,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = SILVER_PREFIX,
) -> None:
    for table_name, df in tables.items():
        key = f"{prefix}{table_name}.parquet"
        save_parquet_to_minio(df=df, key=key, s3=s3, bucket=bucket)
