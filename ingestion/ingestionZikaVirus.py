import pandas as pd
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


if __name__ == "__main__":
    df2016 = fetch_ano(2016)
    print(df2016.head())
    print("Total 2016:", len(df2016))
    df2016.to_csv("zikavirus_2016.csv", index=False)
    print("Arquivo salvo: zikavirus_2016.csv")
