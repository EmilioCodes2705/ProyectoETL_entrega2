# -*- coding: utf-8 -*-
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"     # <-- ÚNICA carpeta de trabajo

# Entradas crudas (pon aquí tus archivos)
ICFES_GLOB       = DATA_DIR / "Examen_Saber_11_*.csv"        # ej: icfes_2019.csv, icfes_2020.csv, etc.
PIB_API_RAW_CSV  = DATA_DIR / "pib_api_raw.csv"    # CSV crudo descargado de la API

# Salidas intermedias / finales (siempre CSV)
ICFES_MERGED_CSV = DATA_DIR / "icfes_merged.csv"
PIB_T_BY_DY_CSV  = DATA_DIR / "pib_by_depto_year.csv"
DDM_CSV          = DATA_DIR / "ddm_icfes_pib.csv"
