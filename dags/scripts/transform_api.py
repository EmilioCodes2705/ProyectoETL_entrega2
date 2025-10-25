# -*- coding: utf-8 -*-
"""
Normaliza el CSV crudo de la API (PIB) y genera un CSV limpio agregado por año y departamento.
- Entrada: CSV crudo (param raw_csv) en data/...
- Salida:  data/pib_by_depto_year.csv (sin compresión)
- Sin dependencias de parquet. Solo CSV.
"""

from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_OUT = DATA_DIR / "pib_by_depto_year.csv"

# Columnas esperadas y renombres desde la API
RENAME_MAP = {
    # nombres originales frecuentes -> nombres estándar del pipeline
    "a_o": "anio",
    "actividad": "actividad",
    "sector": "sector",
    "tipo_de_precios": "tipo_de_precios",
    "c_digo_departamento_divipola": "depto_divipola",
    "departamento": "departamento",
    "valor_miles_de_millones_de": "valor_api_mm",
}

NEEDED = ["anio", "depto_divipola", "departamento", "valor_api_mm"]

def _safe_numeric(s):
    # Convierte a numérico tolerando comas, puntos y texto
    if s is None:
        return pd.NA
    return pd.to_numeric(
        pd.Series(s, dtype="string")
          .str.replace(r"[^\d\-,\.]", "", regex=True)
          .str.replace(",", ".", regex=False),  # por si viene con coma decimal
        errors="coerce"
    )

def run(raw_csv: str, out_csv: str = None) -> dict:
    """
    Lee el CSV crudo (raw_csv), normaliza campos y agrega por (anio, depto_divipola).
    Devuelve {"pib_csv": <ruta_salida>}.
    """
    if not raw_csv:
        raise RuntimeError("transform_api.run(): falta parámetro raw_csv.")
    in_path = Path(raw_csv)
    if not in_path.exists():
        raise FileNotFoundError(f"No existe el CSV crudo de API: {in_path}")

    out_path = Path(out_csv) if out_csv else DEFAULT_OUT
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 1) Carga
    df = pd.read_csv(in_path, dtype=str).fillna("")
    print(f"[transform_api] input={in_path} filas={len(df):,}")

    # 2) Renombrar columnas al estándar
    for src, dst in RENAME_MAP.items():
        if src in df.columns and dst not in df.columns:
            df = df.rename(columns={src: dst})

    # 3) Asegurar columnas necesarias (si faltan, error claro)
    missing = [c for c in ["anio", "depto_divipola", "valor_api_mm"] if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"[transform_api] Faltan columnas clave en el crudo: {missing}. "
            f"Columnas disponibles: {list(df.columns)}"
        )

    # 4) Tipos
    df["anio"] = _safe_numeric(df["anio"]).astype("Int64")
    df["depto_divipola"] = _safe_numeric(df["depto_divipola"]).astype("Int64")
    df["valor_api_mm"] = _safe_numeric(df["valor_api_mm"]).astype("Float64")

    # 5) Limpieza mínima (descartar registros inválidos)
    before = len(df)
    df = df.dropna(subset=["anio", "depto_divipola", "valor_api_mm"])
    after = len(df)
    if after == 0:
        raise RuntimeError("[transform_api] Todas las filas quedaron inválidas tras limpieza.")
    print(f"[transform_api] limpiadas={before-after:,} filas inválidas")

    # 6) Agregar por (anio, depto_divipola); mantén nombre de dpto (primero no nulo)
    #    - Si 'departamento' no existe, la creamos vacía para no fallar el merge posterior.
    if "departamento" not in df.columns:
        df["departamento"] = ""

    grp = (
        df.groupby(["anio", "depto_divipola"], dropna=False)
          .agg(
              valor_api_mm=("valor_api_mm", "sum"),
              departamento=("departamento", lambda s: next((x for x in s if str(x).strip() != ""), "")),
          )
          .reset_index()
    )

    # 7) Seleccionar orden de columnas y guardar CSV
    out_df = grp[["anio", "depto_divipola", "departamento", "valor_api_mm"]]
    out_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[transform_api] OK -> {out_path} filas={len(out_df):,}")

    return {"pib_csv": str(out_path)}