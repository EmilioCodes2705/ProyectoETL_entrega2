# run_local.py
import argparse
from pathlib import Path

# Importa tus scripts
from scripts import (
    extract_icfes,
    transform_icfes,
    extract_api,
    transform_api,
    merge,
    load_dw,
)

def main():
    p = argparse.ArgumentParser(description="ETL ICFES + PIB por dpto (local)")
    p.add_argument(
        "--steps",
        default="extract_icfes,transform_icfes,extract_api,transform_api,merge,load",
        help="Comma-separated steps to run. "
             "Options: extract_icfes,transform_icfes,extract_api,transform_api,merge,load",
    )
    args = p.parse_args()
    steps = [s.strip() for s in args.steps.split(",") if s.strip()]

    state = {}

    if "extract_icfes" in steps:
        print("\n[STEP] extract_icfes")
        state.update(extract_icfes.run(pattern="Examen_Saber_11_*.csv"))


    if "transform_icfes" in steps:
        print("\n[STEP] transform_icfes")
        # si tu transform requiere paths, los pasa desde state
        state.update(transform_icfes.run(raw_paths=state.get("raw_paths")))

    if "extract_api" in steps:
        print("\n[STEP] extract_api")
        state.update(extract_api.run())

    if "transform_api" in steps:
        print("\n[STEP] transform_api")
        state.update(transform_api.run())

    if "merge" in steps:
        print("\n[STEP] merge (ICFES + PIB por dpto)")
        # si tu merge necesita rutas específicas, agrégalas aquí desde state
        state.update(merge.run())

    if "load" in steps:
        print("\n[STEP] load_dw")
        # por defecto carga el unificado ICFES; cambia a enriched si así lo definiste
        state.update(load_dw.run(validated_path=state.get("validated_path")))

    print("\n[DONE]", state)

if __name__ == "__main__":
    main