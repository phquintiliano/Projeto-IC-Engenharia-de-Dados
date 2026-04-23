from __future__ import annotations

from silver.zika.builder import build_data_vault_tables
from silver.zika.enrich_keys import enrich_with_hks
from silver.zika.io_minio import load_raw_zika_data, save_silver_tables
from silver.zika.raw_to_std import transform_padronizar_raw
from silver.zika.validators import validate_standardized_data


def run_zika_silver(keys=None, s3=None):
    raw_df = load_raw_zika_data(keys=keys, s3=s3)
    if raw_df.empty:
        return {'status': 'empty', 'validation': None, 'tables': {}}

    std_df = transform_padronizar_raw(raw_df)
    enriched_df = enrich_with_hks(std_df)
    validation = validate_standardized_data(enriched_df)
    tables = build_data_vault_tables(enriched_df)
    save_silver_tables(tables=tables, s3=s3)

    return {
        'status': 'ok',
        'validation': validation,
        'tables': {name: len(df) for name, df in tables.items()},
    }
