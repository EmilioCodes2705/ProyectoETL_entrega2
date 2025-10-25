from pathlib import Path
import os
import pandas as pd
from sodapy import Socrata

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Rutas compatibles (primero nuevas, luego legacy)
CANDIDATES = [
    DATA_DIR / "pib_api_raw.csv",
    DATA_DIR / "raw" / "pib_api_raw.csv",
]

def run(domain: str = "www.datos.gov.co",
        dataset_id: str = "kgyi-qc7j",
        app_token: str = None,
        username: str = None,
        password: str = None,
        out_csv: str = None,
        page_limit: int = 50000,
        **filters) -> dict:

    # 1) OFFLINE primero: si el CSV ya existe en alguna ruta conocida, úsalo
    for p in CANDIDATES:
        if p.exists():
            print(f"[extract_api] Reutilizando CSV local: {p}")
            # si out_csv apunta a otra ruta, lo copiamos/normalizamos
            out_path = Path(out_csv) if out_csv else p
            if out_path != p:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(p.read_text(encoding="utf-8"), encoding="utf-8")
            return {"raw_csv": str(out_path)}

    # 2) Si no existe local, intenta descargar (ONLINE)
    out_path = Path(out_csv) if out_csv else CANDIDATES[0]
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[extract_api] Conectando a Socrata: domain={domain}, dataset={dataset_id}")
    client = Socrata(domain, app_token, username=username, password=password)

    offset = 0
    all_rows = []
    while True:
        batch = client.get(dataset_id, limit=page_limit, offset=offset, **filters)
        if not batch:
            break
        all_rows.extend(batch)
        offset += page_limit
        print(f"[extract_api] Bajados {len(all_rows)} registros...")

    if not all_rows:
        raise RuntimeError("[extract_api] La API devolvió 0 filas.")

    df = pd.DataFrame.from_records(all_rows)

    # Normalización mínima esperada por el pipeline
    rename_map = {
        "a_o": "anio",
        "c_digo_departamento_divipola": "depto_divipola",
        "departamento": "departamento",
        "valor_miles_de_millones_de": "valor_api_mm",
    }
    for k, v in rename_map.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})

    # Tipos clave
    if "anio" in df.columns:
        df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    if "depto_divipola" in df.columns:
        df["depto_divipola"] = pd.to_numeric(df["depto_divipola"], errors="coerce").astype("Int64")
    if "valor_api_mm" in df.columns:
        df["valor_api_mm"] = pd.to_numeric(df["valor_api_mm"], errors="coerce")

    # Guardar CSV
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[extract_api] OK -> {out_path} ({len(df):,} filas)")
    return {"raw_csv": str(out_path)}
