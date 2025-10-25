# -*- coding: utf-8 -*-
"""
Carga el DDM CSV (merge ICFES + PIB) a Postgres con COPY FROM STDIN.
Detecta automáticamente las columnas del CSV para máxima flexibilidad.
"""

from pathlib import Path
import psycopg2
import csv
import json

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
STAGING = DATA_DIR / "staging"

DEFAULT_DDM = STAGING / "ddm_icfes_pib.csv"


def _ddl(table: str, columns: list) -> str:
    """
    Genera el DDL para crear la tabla con el esquema basado en las columnas del CSV.
    Todas las columnas son TEXT por defecto para máxima compatibilidad.
    """
    cols_def = []
    
    for col in columns:
        # Escapar comillas dobles en nombres de columna
        col_safe = col.replace('"', '""')
        cols_def.append(f'"{col_safe}" TEXT')
    
    newline_indent = ",\n  "
    cols_joined = newline_indent.join(cols_def)
    
    ddl = f'''CREATE TABLE IF NOT EXISTS "{table}" (
  id BIGSERIAL PRIMARY KEY,
  {cols_joined},
  created_at TIMESTAMP DEFAULT now()
);'''
    
    return ddl


def _create_indexes(table: str, cur, columns: list) -> None:
    """Crea índices útiles para consultas si las columnas existen"""
    
    # Índices comunes que pueden ser útiles
    potential_indexes = [
        ('año', 'anio'),  # Columna de año
        ('cole_cod_depto_ubicacion', 'depto_codigo'),  # Código departamento
        ('estu_cod_depto_presentacion', 'depto_presentacion'),
    ]
    
    created_count = 0
    
    for col_variants in potential_indexes:
        for col in col_variants:
            if col in columns:
                idx_name = f"idx_{table}_{col.replace(' ', '_')}"
                try:
                    # Usar savepoint para no abortar la transacción principal
                    cur.execute(f"SAVEPOINT sp_{idx_name};")
                    cur.execute(f'CREATE INDEX IF NOT EXISTS {idx_name} ON "{table}" ("{col}");')
                    cur.execute(f"RELEASE SAVEPOINT sp_{idx_name};")
                    print(f"   ✓ Índice creado: {col}")
                    created_count += 1
                    break  # Solo crear un índice por variante
                except Exception as e:
                    cur.execute(f"ROLLBACK TO SAVEPOINT sp_{idx_name};")
                    # No mostrar error, solo continuar
    
    # Índice compuesto si existen ambas columnas
    if 'año' in columns and 'cole_cod_depto_ubicacion' in columns:
        try:
            cur.execute(f"SAVEPOINT sp_idx_compuesto;")
            cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_año_depto ON "{table}" ("año", "cole_cod_depto_ubicacion");')
            cur.execute(f"RELEASE SAVEPOINT sp_idx_compuesto;")
            print(f"   ✓ Índice compuesto: año + departamento")
            created_count += 1
        except Exception as e:
            cur.execute(f"ROLLBACK TO SAVEPOINT sp_idx_compuesto;")
    
    if created_count == 0:
        print(f"   • Índices no creados (se usarán los del init_ddm.sql)")


def run(path: str = None, table: str = "ddm_icfes_pib", conn_uri: str = None, replace: bool = False) -> dict:
    """
    Carga el CSV del merge a Postgres.
    
    Args:
        path: Ruta al CSV (default: staging/ddm_icfes_pib.csv)
        table: Nombre de la tabla destino
        conn_uri: URI de conexión a Postgres
        replace: Si True, hace TRUNCATE antes de cargar
    
    Returns:
        Dict con información de la carga
    """
    print(f"\n{'='*70}")
    print(f"📥 CARGANDO DDM A POSTGRES")
    print(f"{'='*70}")
    
    # ==================== 1. VALIDAR RUTA DEL CSV ====================
    csv_path = Path(path) if path else DEFAULT_DDM
    tried = [str(csv_path)]
    
    if not csv_path.exists():
        # Intentar ubicaciones alternativas
        alt_paths = [
            DATA_DIR / csv_path.name,
            STAGING / csv_path.name,
            DATA_DIR / "ddm_icfes_pib.csv",
            STAGING / "ddm_icfes_pib.csv",
            DATA_DIR / "icfes_con_pib.csv",  # Nombre alternativo
        ]
        
        for alt in alt_paths:
            if str(alt) not in tried:
                tried.append(str(alt))
            if alt.exists():
                csv_path = alt
                break
        else:
            # No se encontró en ninguna ubicación
            debug_dir = DATA_DIR / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            
            (debug_dir / "load_dw_not_found.json").write_text(
                json.dumps({"tried_paths": tried}, ensure_ascii=False, indent=2)
            )
            
            raise FileNotFoundError(
                f"❌ No se encontró el CSV en ninguna ubicación.\n"
                f"Rutas intentadas:\n  - " + "\n  - ".join(tried) + "\n"
                f"Ver detalles en: {debug_dir / 'load_dw_not_found.json'}"
            )
    
    print(f"📂 CSV encontrado: {csv_path}")
    
    # Crear debug_dir basado en la ruta final
    debug_dir = csv_path.parent / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)

    # ==================== 2. LEER COLUMNAS DEL CSV ====================
    print(f"\n🔍 Leyendo estructura del CSV...")
    
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            csv_columns = next(reader)
        except StopIteration:
            raise RuntimeError(f"❌ CSV vacío: {csv_path}")
        
        try:
            first_row = next(reader)
        except StopIteration:
            first_row = []
        
        # Contar filas totales (sample)
        row_count = 2  # header + first_row
        for _ in reader:
            row_count += 1
            if row_count > 1000:  # Solo contar las primeras 1000
                break
    
    num_cols = len(csv_columns)
    
    print(f"   ✓ Columnas detectadas: {num_cols}")
    print(f"   ✓ Primeras 5 columnas: {csv_columns[:5]}")
    if 'pib_departamental_mm' in csv_columns:
        print(f"   ✓ Columna PIB encontrada: pib_departamental_mm")
    
    # Guardar diagnóstico
    validation_info = {
        "csv_path": str(csv_path),
        "total_columns": num_cols,
        "columns": csv_columns,
        "first_row_sample": first_row[:10] if len(first_row) > 10 else first_row,
        "rows_sample": row_count
    }
    
    (debug_dir / "load_dw_csv_validation.json").write_text(
        json.dumps(validation_info, ensure_ascii=False, indent=2)
    )

    # ==================== 3. CONECTAR A POSTGRES ====================
    if not conn_uri:
        raise RuntimeError("❌ Falta parámetro conn_uri")
    
    print(f"\n🔌 Conectando a Postgres...")
    
    try:
        with psycopg2.connect(conn_uri) as conn:
            # NO usar autocommit para tener control de transacciones
            conn.autocommit = False
            
            with conn.cursor() as cur:
                # ==================== 4. VERIFICAR Y CREAR TABLA ====================
                print(f"📋 Verificando tabla '{table}'...")
                
                # Verificar si la tabla existe y tiene las columnas correctas
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table}'
                    ORDER BY ordinal_position;
                """)
                existing_cols = [row[0] for row in cur.fetchall()]
                
                if existing_cols:
                    # Tabla existe - verificar si coinciden las columnas
                    if set(existing_cols) - {'id', 'created_at', 'updated_at'} != set(csv_columns):
                        print(f"   ⚠️ Tabla existe con columnas diferentes")
                        print(f"   🗑️ Eliminando tabla antigua...")
                        cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
                        conn.commit()
                        print(f"   ✓ Tabla eliminada")
                
                print(f"📋 Creando tabla '{table}'...")
                cur.execute(_ddl(table, csv_columns))
                conn.commit()
                print(f"   ✓ Tabla lista con {num_cols} columnas")
                
                # ==================== 5. TRUNCATE SI ES NECESARIO ====================
                if replace:
                    print(f"🗑️  Limpiando tabla (TRUNCATE)...")
                    cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY;')
                    conn.commit()
                    print(f"   ✓ Tabla limpiada")
                
                # ==================== 6. CREAR ÍNDICES (OPCIONAL - SKIP) ====================
                # Los índices se crean mejor desde init_ddm.sql o después de cargar
                print(f"📑 Índices: Se usarán los definidos en init_ddm.sql")
                
                # ==================== 7. CARGAR DATOS CON COPY ====================
                print(f"\n📤 Cargando datos...")
                
                cols_csv = ",".join([f'"{c}"' for c in csv_columns])
                copy_sql = (
                    f'COPY "{table}" ({cols_csv}) '
                    f"FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '\"', ESCAPE '\"')"
                )
                
                with open(csv_path, "r", encoding="utf-8", newline="") as f:
                    cur.copy_expert(copy_sql, f)
                
                conn.commit()
                
                # Obtener número de filas cargadas
                cur.execute(f'SELECT COUNT(*) FROM "{table}";')
                loaded_rows = cur.fetchone()[0]
                
                print(f"   ✓ Datos cargados: {loaded_rows:,} filas")
                
                # ==================== 8. ESTADÍSTICAS ====================
                print(f"\n📊 Obteniendo estadísticas...")
                
                stats = {
                    "table": table,
                    "total_rows": loaded_rows,
                    "total_columns": num_cols
                }
                
                # Años únicos (buscar columna de año)
                year_col = None
                for col in ['año', 'anio', 'periodo']:
                    if col in csv_columns:
                        year_col = col
                        break
                
                if year_col:
                    cur.execute(f'SELECT COUNT(DISTINCT "{year_col}") FROM "{table}" WHERE "{year_col}" IS NOT NULL AND "{year_col}" != \'\';')
                    unique_years = cur.fetchone()[0]
                    stats['unique_years'] = unique_years
                    print(f"   • Años únicos: {unique_years}")
                
                # Departamentos únicos (buscar columna de departamento)
                depto_col = None
                for col in ['cole_cod_depto_ubicacion', 'estu_cod_depto_presentacion', 'departamento']:
                    if col in csv_columns:
                        depto_col = col
                        break
                
                if depto_col:
                    cur.execute(f'SELECT COUNT(DISTINCT "{depto_col}") FROM "{table}" WHERE "{depto_col}" IS NOT NULL AND "{depto_col}" != \'\';')
                    unique_deptos = cur.fetchone()[0]
                    stats['unique_deptos'] = unique_deptos
                    print(f"   • Departamentos únicos: {unique_deptos}")
                
                # Filas con PIB (buscar columna de PIB)
                pib_col = None
                for col in ['pib_departamental_mm', 'valor_api_mm', 'pib']:
                    if col in csv_columns:
                        pib_col = col
                        break
                
                if pib_col:
                    cur.execute(f'SELECT COUNT(*) FROM "{table}" WHERE "{pib_col}" IS NOT NULL AND "{pib_col}" != \'\';')
                    rows_with_pib = cur.fetchone()[0]
                    pib_coverage = rows_with_pib / loaded_rows if loaded_rows > 0 else 0
                    
                    stats['rows_with_pib'] = rows_with_pib
                    stats['pib_coverage'] = pib_coverage
                    print(f"   • Cobertura PIB: {pib_coverage:.1%} ({rows_with_pib:,} registros)")
                
                # Guardar estadísticas
                (debug_dir / "load_dw_stats.json").write_text(
                    json.dumps(stats, ensure_ascii=False, indent=2)
                )
        
        print(f"\n{'='*70}")
        print(f"✅ CARGA COMPLETADA EXITOSAMENTE")
        print(f"{'='*70}\n")
        
        return {
            "table": table,
            "path": str(csv_path),
            "rows_loaded": loaded_rows,
            "pib_coverage": stats.get('pib_coverage', 0),
            "unique_years": stats.get('unique_years', 0),
            "unique_deptos": stats.get('unique_deptos', 0),
            "total_columns": num_cols
        }
    
    except psycopg2.Error as e:
        error_info = {
            "error_type": "PostgreSQL Error",
            "error_code": e.pgcode,
            "error_message": str(e),
            "csv_path": str(csv_path),
            "table": table
        }
        
        (debug_dir / "load_dw_error.json").write_text(
            json.dumps(error_info, ensure_ascii=False, indent=2)
        )
        
        print(f"\n❌ Error de PostgreSQL:")
        print(f"   Código: {e.pgcode}")
        print(f"   Mensaje: {e}")
        print(f"   Ver detalles en: {debug_dir / 'load_dw_error.json'}")
        
        raise


if __name__ == "__main__":
    # Testing standalone
    import sys
    
    # Requiere conn_uri como variable de entorno o argumento
    conn_uri = sys.argv[1] if len(sys.argv) > 1 else None
    
    if not conn_uri:
        print("❌ Uso: python load_dw.py 'postgresql://user:pass@host:port/db'")
        print("   O define la variable POSTGRES_CONN_URI")
        sys.exit(1)
    
    csv_file = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_DDM)
    
    print(f"Cargando:")
    print(f"  CSV: {csv_file}")
    print(f"  DB: {conn_uri.split('@')[1] if '@' in conn_uri else 'hidden'}")
    
    result = run(path=csv_file, conn_uri=conn_uri, replace=True)
    print(f"\n📋 Resultado: {json.dumps(result, indent=2)}")