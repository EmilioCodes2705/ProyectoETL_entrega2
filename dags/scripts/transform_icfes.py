# -*- coding: utf-8 -*-
"""
Transforma y consolida archivos CSV de ICFES (Examen Saber 11).
Optimizado para bajo uso de memoria mediante escritura incremental.
"""
from pathlib import Path
import pandas as pd
import csv
import re
import gc

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUT_CSV = DATA_DIR / "icfes_merged.csv"


def _read_csv_robusto(path: Path) -> pd.DataFrame:
    """Lee CSV con detección automática de delimitador"""
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        muestra = fh.read(65536)
    try:
        dialect = csv.Sniffer().sniff(muestra, delimiters=",;\t|")
        sep = dialect.delimiter
    except Exception:
        sep = ","

    df = pd.read_csv(
        path,
        dtype=str,
        sep=sep,
        engine="python",
        quotechar='"',
        escapechar="\\",
        on_bad_lines="skip",
        low_memory=True
    ).fillna("")

    # Limpiar nombres de columnas
    df.columns = [c.strip() for c in df.columns]
    
    # Eliminar columnas duplicadas
    if df.columns.duplicated().any():
        duplicadas = df.columns[df.columns.duplicated()].unique().tolist()
        print(f"     ⚠️ Columnas duplicadas: {len(duplicadas)}")
        df = df.loc[:, ~df.columns.duplicated()]
    
    return df


def _extraer_anio_de_nombre(path: Path) -> int:
    """Extrae el año del nombre del archivo."""
    nombre = path.stem
    match = re.search(r'(20\d{2})', nombre)
    if match:
        return int(match.group(1))
    match = re.search(r'(\d{4})', nombre)
    if match:
        anio = int(match.group(1))
        if 2014 <= anio <= 2025:
            return anio
    return 9999


def _normalizar_departamento(depto_str):
    """
    Normaliza códigos de departamento a formato de 2 dígitos con cero inicial.
    Ej: '5' -> '05', '11' -> '11', '05' -> '05'
    """
    if pd.isna(depto_str) or str(depto_str).strip() == '':
        return None
    
    # Convertir a string y limpiar
    depto = str(depto_str).strip()
    
    # Si es numérico, formatear a 2 dígitos
    try:
        codigo = int(float(depto))
        return f"{codigo:02d}"
    except (ValueError, TypeError):
        return None


def run(raw_csvs=None, out_csv: str = None) -> dict:
    """
    Fusiona archivos CSV de ICFES ordenados por año.
    Renombra la columna 'periodo' a 'año'.
    FILTRADO: Solo años 2015-2023 y columnas específicas.
    OPTIMIZADO para bajo uso de memoria.
    """
    BASE_DIR = Path(__file__).resolve().parents[1]
    DATA_DIR = BASE_DIR / "data"
    out_path = Path(out_csv) if out_csv else (DATA_DIR / "icfes_merged.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Buscar archivos CSV
    csvs = [Path(p) for p in (raw_csvs or [])]
    if not csvs:
        candidatos = (
            list(DATA_DIR.glob("Examen_Saber_11_*.csv")) + 
            list((DATA_DIR / "raw" / "icfes").glob("Examen_Saber_11_*.csv"))
        )
        csvs = [p for p in candidatos if p.is_file()]
    
    if not csvs:
        raise FileNotFoundError("No encontré CSV de ICFES")

    # Ordenar archivos por año y FILTRAR solo 2015-2023
    csvs_con_anio = [(p, _extraer_anio_de_nombre(p)) for p in csvs]
    csvs_con_anio = [(p, a) for p, a in csvs_con_anio if 2015 <= a <= 2023]  # FILTRO DE AÑOS
    csvs_con_anio.sort(key=lambda x: x[1])
    
    if not csvs_con_anio:
        raise FileNotFoundError("No encontré CSV de ICFES para años 2015-2023")

    print("\n📚 Procesando archivos ICFES (2015-2023):")
    
    # Columnas a mantener (patrones)
    columnas_mantener_patrones = [
        'punt',  # Todas las columnas con "punt"
        'depto', 'departamento',  # Columnas de departamento
        'estu_areareside',
        'cole_caracter',
        'cole_area_ubicacion',
    ]
    
    # Escribir directamente al CSV final
    primera_iteracion = True
    total_filas = 0
    
    for i, (p, anio) in enumerate(csvs_con_anio, 1):
        print(f"  {i}/{len(csvs_con_anio)}. {p.name} (año: {anio})")
        
        # Leer archivo
        df = _read_csv_robusto(p)
        
        # ===== RENOMBRAR 'periodo' A 'año' =====
        year_col = None
        for candidato in ['periodo', 'PERIODO', 'estu_anoterminobachiller']:
            if candidato in df.columns:
                year_col = candidato
                break
        
        if year_col:
            df = df.rename(columns={year_col: 'año'})
            print(f"     ✓ Renombrado '{year_col}' → 'año'")
        else:
            df['año'] = str(anio)
            print(f"     ⚠️ Usando año del archivo: {anio}")
        
        # ===== NORMALIZAR AÑO (extraer solo 4 dígitos) =====
        if 'año' in df.columns:
            df['año'] = df['año'].astype(str).str[:4]
            print(f"     ✓ Año normalizado a 4 dígitos")
        
        # Normalizar columnas a minúsculas ANTES de filtrar
        df.columns = [c.lower() for c in df.columns]
        
        # ===== NORMALIZAR CÓDIGO DE DEPARTAMENTO =====
        # Buscar columna de código de departamento
        depto_col = None
        for col in ['cole_cod_depto_ubicacion', 'estu_cod_depto_presentacion', 'estu_cod_reside_depto']:
            if col in df.columns:
                depto_col = col
                break
        
        if depto_col:
            df['depto_normalizado'] = df[depto_col].apply(_normalizar_departamento)
            print(f"     ✓ Departamento normalizado desde '{depto_col}'")
        else:
            print(f"     ⚠️ No se encontró columna de código de departamento")
        
        # ===== FILTRAR COLUMNAS =====
        # Seleccionar columnas que coincidan con los patrones
        columnas_seleccionadas = ['año']  # Siempre incluir año
        
        # Agregar depto_normalizado si existe
        if 'depto_normalizado' in df.columns:
            columnas_seleccionadas.append('depto_normalizado')
        
        for col in df.columns:
            # Verificar si la columna coincide con algún patrón
            for patron in columnas_mantener_patrones:
                if patron.lower() in col.lower():
                    columnas_seleccionadas.append(col)
                    break
        
        # Eliminar duplicados manteniendo orden
        columnas_seleccionadas = list(dict.fromkeys(columnas_seleccionadas))
        
        # Filtrar DataFrame
        df = df[columnas_seleccionadas]
        
        # ===== ELIMINAR FILAS CON VALORES FALTANTES CRÍTICOS =====
        filas_antes = len(df)
        
        # Dropna en año y departamento (críticos para merge)
        cols_criticas = ['año']
        if 'depto_normalizado' in df.columns:
            cols_criticas.append('depto_normalizado')
        
        df = df.dropna(subset=cols_criticas)
        
        filas_despues = len(df)
        filas_eliminadas = filas_antes - filas_despues
        
        if filas_eliminadas > 0:
            print(f"     🗑️ Eliminadas {filas_eliminadas:,} filas con año/depto nulo")
        
        print(f"     ✓ {len(df):,} filas, {len(df.columns)} columnas filtradas")
        
        # Escribir directamente al archivo final
        if primera_iteracion:
            df.to_csv(out_path, index=False, mode='w', encoding='utf-8')
            primera_iteracion = False
        else:
            df.to_csv(out_path, index=False, mode='a', header=False, encoding='utf-8')
        
        total_filas += len(df)
        
        # Liberar memoria inmediatamente
        del df
        gc.collect()
        
        print(f"     💾 Guardado (total acumulado: {total_filas:,})")
    
    print(f"\n✅ Consolidación completada:")
    print(f"   📄 Archivo: {out_path}")
    print(f"   📊 Total filas: {total_filas:,}")
    print(f"   📅 Años procesados: {sorted([a for _, a in csvs_con_anio])}")
    
    # Validación final
    print(f"\n🔍 Validando muestra del resultado...")
    try:
        df_sample = pd.read_csv(out_path, nrows=1000, dtype=str, low_memory=True)
        
        if 'año' in df_sample.columns:
            no_vacios = (df_sample['año'] != '') & (df_sample['año'].notna())
            pct = no_vacios.mean()
            print(f"   • Columna 'año': {pct:.1%} válidos (en muestra)")
        
        print(f"   • Columnas totales: {len(df_sample.columns)}")
        print(f"   • Columnas con 'punt': {sum(1 for c in df_sample.columns if 'punt' in c.lower())}")
        print(f"   • Columnas con 'depto': {sum(1 for c in df_sample.columns if 'depto' in c.lower())}")
        
        del df_sample
        gc.collect()
    except Exception as e:
        print(f"   ⚠️ No se pudo validar: {e}")
    
    return {
        "icfes_merged_csv": str(out_path), 
        "rows": int(total_filas),
        "columns": len(columnas_seleccionadas) if 'columnas_seleccionadas' in locals() else 0,
        "archivos_procesados": len(csvs_con_anio),
        "años": sorted([a for _, a in csvs_con_anio])
    }


if __name__ == "__main__":
    run()