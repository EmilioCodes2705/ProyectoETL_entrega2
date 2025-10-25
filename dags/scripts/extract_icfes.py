# -*- coding: utf-8 -*-
from pathlib import Path
from .paths import DATA_DIR, ICFES_GLOB

def run() -> dict:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    csvs = sorted(Path(DATA_DIR).glob(ICFES_GLOB.name))
    if not csvs:
        raise FileNotFoundError(
            f"No se encontraron CSV ICFES en {DATA_DIR}. Esperado patrón: {ICFES_GLOB.name}"
        )
    
    return {"raw_csvs": [str(p) for p in sorted(csvs)]}

