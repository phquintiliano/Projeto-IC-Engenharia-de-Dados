def validate_standardized_data(df):
    return {
        'total_linhas': len(df),
        'nulos_id_agravo': int(df['id_agravo'].isna().sum()),
        'nulos_dt_notific': int(df['dt_notific'].isna().sum()),
        'nulos_id_municip': int(df['id_municip'].isna().sum()),
        'nulos_id_unidade': int(df['id_unidade'].isna().sum()),
        'duplicados_bk_notificacao': int(df['bk_notificacao'].duplicated().sum()),
    }
