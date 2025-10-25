# -*- coding: utf-8 -*-
"""
Merge de datos ICFES con PIB departamental.
Agrega la columna de PIB a cada registro de ICFES basándose en su departamento y año.
"""

from pathlib import Path
import pandas as pd
import gc

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_OUT = DATA_DIR / "icfes_con_pib.csv"


def _safe_numeric(series, name="columna"):
    """Convierte una serie a numérico de forma segura"""
    return pd.to_numeric(series, errors='coerce')


def run(icfes_merged_csv: str = None, pib_by_depto_csv: str = None, out_csv: str = None) -> dict:
    """
    Fusiona datos de ICFES con PIB departamental.
    
    Args:
        icfes_merged_csv: Ruta al CSV consolidado de ICFES
        pib_by_depto_csv: Ruta al CSV de PIB por departamento y año
        out_csv: Ruta de salida (opcional)
    
    Returns:
        Dict con información del resultado
    """
    
    # Rutas por defecto
    icfes_path = Path(icfes_merged_csv) if icfes_merged_csv else (DATA_DIR / "icfes_merged.csv")
    pib_path = Path(pib_by_depto_csv) if pib_by_depto_csv else (DATA_DIR / "pib_by_depto_year.csv")
    out_path = Path(out_csv) if out_csv else DEFAULT_OUT
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Validar que existan los archivos
    if not icfes_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo ICFES: {icfes_path}")
    if not pib_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo PIB: {pib_path}")
    
    print("\n📊 Iniciando merge de ICFES con PIB...")
    print(f"   📁 ICFES: {icfes_path}")
    print(f"   📁 PIB: {pib_path}")
    
    # ===== CARGAR PIB (pequeño, cabe en memoria) =====
    print("\n1️⃣ Cargando datos de PIB...")
    df_pib = pd.read_csv(pib_path, dtype=str)
    
    # Mantener todo como string para el merge (año y depto ya normalizados)
    # Convertir valor PIB a numérico
    df_pib['valor_api_mm'] = pd.to_numeric(df_pib['valor_api_mm'], errors='coerce')
    
    # Renombrar columnas de PIB para claridad en el merge
    df_pib = df_pib.rename(columns={
        'anio': 'año_pib',
        'depto_divipola': 'depto_codigo'
    })
    
    print(f"   ✓ PIB cargado: {len(df_pib):,} registros")
    
    # Para display: convertir a numérico temporalmente
    años_pib = sorted([int(x) for x in df_pib['año_pib'].dropna().unique() if str(x).replace('.','').replace('-','').isdigit()])
    print(f"   ✓ Años disponibles en PIB: {años_pib}")
    print(f"   ✓ Departamentos únicos: {df_pib['depto_codigo'].nunique()}")
    print(f"   ✓ Ejemplo códigos depto: {sorted(df_pib['depto_codigo'].dropna().unique()[:5].tolist())}")
    
    # ===== PROCESAR ICFES EN CHUNKS =====
    print("\n2️⃣ Procesando ICFES por chunks...")
    
    chunk_size = 50000  # Procesar de 50k en 50k
    primera_iteracion = True
    total_procesado = 0
    total_con_pib = 0
    
    for i, chunk in enumerate(pd.read_csv(icfes_path, chunksize=chunk_size, dtype=str, low_memory=True), 1):
        print(f"\n   Chunk {i}: {len(chunk):,} filas")
        
        # Normalizar nombres de columnas a minúsculas
        chunk.columns = [c.lower() for c in chunk.columns]
        
        # ===== PREPARAR COLUMNAS PARA EL MERGE =====
        
        # 1. Columna de año (ya debería estar como 'año' después de transform_icfes)
        if 'año' not in chunk.columns:
            print("   ⚠️ Columna 'año' no encontrada, buscando alternativas...")
            for col_candidato in ['periodo', 'estu_anoterminobachiller', 'anio_origen']:
                if col_candidato in chunk.columns:
                    chunk = chunk.rename(columns={col_candidato: 'año'})
                    print(f"   ✓ Usando '{col_candidato}' como 'año'")
                    break
        
        if 'año' not in chunk.columns:
            raise RuntimeError("No se pudo encontrar una columna de año en ICFES")
        
        # 2. Columna de código de departamento - USAR DEPTO_NORMALIZADO
        if 'depto_normalizado' in chunk.columns:
            depto_col = 'depto_normalizado'
            print(f"   ✓ Usando 'depto_normalizado' para el merge")
        else:
            # Fallback: buscar otra columna
            depto_col = None
            for candidato in ['cole_cod_depto_ubicacion', 'estu_cod_depto_presentacion', 'cole_depto_ubicacion']:
                if candidato in chunk.columns:
                    depto_col = candidato
                    break
            
            if not depto_col:
                print("   ⚠️ Columnas disponibles:", list(chunk.columns[:20]), "...")
                raise RuntimeError("No se encontró columna de código de departamento en ICFES")
            
            print(f"   ✓ Usando '{depto_col}' como código de departamento")
        
        # Convertir a string para el merge (ya están normalizados)
        chunk['año_merge'] = chunk['año'].astype(str)
        chunk['depto_merge'] = chunk[depto_col].astype(str)
        
        # ===== REALIZAR EL MERGE =====
        print(f"   🔗 Realizando LEFT JOIN...")
        
        # LEFT JOIN: mantiene todos los registros de ICFES
        chunk_merged = chunk.merge(
            df_pib,
            left_on=['año_merge', 'depto_merge'],
            right_on=['año_pib', 'depto_codigo'],
            how='left'
        )
        
        # Renombrar la columna de PIB para claridad
        if 'valor_api_mm' in chunk_merged.columns:
            chunk_merged = chunk_merged.rename(columns={'valor_api_mm': 'pib_departamental_mm'})
        
        # Contar registros con PIB
        con_pib = chunk_merged['pib_departamental_mm'].notna().sum()
        total_con_pib += con_pib
        pct_con_pib = (con_pib / len(chunk_merged)) * 100
        
        print(f"   ✓ Merge completado: {len(chunk_merged):,} registros")
        print(f"   ✓ Con PIB: {con_pib:,} ({pct_con_pib:.1f}%)")
        
        # Eliminar columnas auxiliares del merge
        cols_to_drop = ['año_merge', 'depto_merge', 'año_pib', 'depto_codigo']
        chunk_merged = chunk_merged.drop(columns=[c for c in cols_to_drop if c in chunk_merged.columns])
        
        # ===== IMPUTAR PIB FALTANTE =====
        # Para registros sin PIB, usar el promedio del departamento en ese año
        if 'pib_departamental_mm' in chunk_merged.columns:
            # Calcular promedios por departamento para imputación
            sin_pib_antes = chunk_merged['pib_departamental_mm'].isna().sum()
            
            if sin_pib_antes > 0 and 'depto_normalizado' in chunk_merged.columns:
                # Crear diccionario de promedios por departamento-año
                promedios = chunk_merged.groupby(['año', 'depto_normalizado'])['pib_departamental_mm'].mean()
                
                # Imputar valores faltantes
                def imputar_pib(row):
                    if pd.isna(row['pib_departamental_mm']):
                        key = (row['año'], row.get('depto_normalizado'))
                        if key in promedios.index:
                            return promedios[key]
                    return row['pib_departamental_mm']
                
                chunk_merged['pib_departamental_mm'] = chunk_merged.apply(imputar_pib, axis=1)
                
                sin_pib_despues = chunk_merged['pib_departamental_mm'].isna().sum()
                imputados = sin_pib_antes - sin_pib_despues
                
                if imputados > 0:
                    print(f"   📊 PIB imputado: {imputados:,} registros")
        
        # ===== ESCRIBIR AL ARCHIVO FINAL =====
        if primera_iteracion:
            chunk_merged.to_csv(out_path, index=False, mode='w', encoding='utf-8')
            primera_iteracion = False
        else:
            chunk_merged.to_csv(out_path, index=False, mode='a', header=False, encoding='utf-8')
        
        total_procesado += len(chunk_merged)
        
        # Liberar memoria
        del chunk, chunk_merged
        gc.collect()
        
        print(f"   💾 Guardado (total acumulado: {total_procesado:,})")
    
    # ===== RESUMEN FINAL =====
    print(f"\n✅ Merge completado exitosamente:")
    print(f"   📄 Archivo de salida: {out_path}")
    print(f"   📊 Total registros: {total_procesado:,}")
    print(f"   📈 Registros con PIB: {total_con_pib:,} ({(total_con_pib/total_procesado*100):.1f}%)")
    print(f"   ⚠️ Registros sin PIB: {total_procesado - total_con_pib:,} ({((total_procesado-total_con_pib)/total_procesado*100):.1f}%)")
    
    # Liberar memoria del PIB
    del df_pib
    gc.collect()
    
    # Calcular cobertura como decimal (para format_percentage del DAG)
    pib_coverage = (total_con_pib / total_procesado) if total_procesado > 0 else 0.0
    
    return {
        "ddm_csv": str(out_path),  # Nombre esperado por el DAG
        "merged_csv": str(out_path),  # Por compatibilidad
        "total_rows": int(total_procesado),
        "rows_with_pib": int(total_con_pib),
        "rows_without_pib": int(total_procesado - total_con_pib),
        "pct_with_pib": round((total_con_pib / total_procesado) * 100, 2),
        "pib_coverage": pib_coverage  # Decimal (0.75 = 75%)
    }


if __name__ == "__main__":
    result = run()
    print(f"\n🎉 Proceso completado: {result}")