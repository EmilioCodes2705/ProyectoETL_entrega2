# -*- coding: utf-8 -*-
"""
DAG ETL Proyecto 2: ICFES + PIB
Extrae datos de ICFES y API PIB, los transforma, hace merge y carga a Postgres.
"""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import os
from pathlib import Path

# Importar módulos de scripts
from scripts import (
    extract_icfes, 
    extract_api, 
    transform_icfes, 
    transform_api, 
    merge, 
    load_dw
)
from scripts.paths import DATA_DIR

# ============================================
# FUNCIONES HELPER
# ============================================

def format_number(value, default='N/A'):
    """
    Formatea un número con separador de miles.
    Si el valor no es numérico, devuelve el default.
    
    Args:
        value: Valor a formatear (int, float, o cualquier otro)
        default: Valor por defecto si no es número
        
    Returns:
        str: Número formateado o default
    """
    if value is None:
        return default
    
    if isinstance(value, (int, float)):
        return f"{value:,}"
    
    # Intentar convertir a número
    try:
        num = float(value)
        if num.is_integer():
            return f"{int(num):,}"
        return f"{num:,.2f}"
    except (ValueError, TypeError, AttributeError):
        return default


def format_percentage(value, default='N/A'):
    """
    Formatea un valor como porcentaje.
    
    Args:
        value: Valor decimal (0.75 = 75%)
        default: Valor por defecto si no es número
        
    Returns:
        str: Porcentaje formateado o default
    """
    if value is None:
        return default
    
    try:
        pct = float(value) * 100
        return f"{pct:.1f}%"
    except (ValueError, TypeError, AttributeError):
        return default


# ============================================
# CONFIGURACIÓN
# ============================================

# Directorio base del DAG
BASE_DIR = Path(__file__).resolve().parent

# Cadena de conexión a Postgres (prioridad: Variable Airflow > ENV)
def get_postgres_uri():
    """Obtiene la URI de Postgres desde Variable o ENV"""
    uri = Variable.get("POSTGRES_URI", default_var=None)
    if not uri:
        uri = os.getenv("POSTGRES_URI")
    return uri

# ============================================
# ARGUMENTOS POR DEFECTO DEL DAG
# ============================================

default_args = {
    'owner': 'etl_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ============================================
# DEFINICIÓN DEL DAG
# ============================================

@dag(
    dag_id="etl_icfes_pib_project2",
    description="Pipeline ETL completo: ICFES + PIB → Data Warehouse",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
    default_args=default_args,
    tags=["etl", "icfes", "pib", "postgres", "project2"],
    max_active_runs=1,  # Solo una ejecución a la vez
)
def etl_icfes_pib_project2():
    """
    Pipeline ETL completo para integración ICFES + PIB
    
    Flujo:
    1. Extracción ICFES (múltiples CSV por año)
    2. Transformación ICFES (merge de años)
    3. Extracción API PIB
    4. Transformación API PIB (normalización)
    5. Merge ICFES + PIB (LEFT JOIN)
    6. Inicialización DW (estructura en Postgres)
    7. Carga a DW (COPY masivo)
    """

    # ========================================
    # TAREA 1: EXTRAER ICFES
    # ========================================
    @task(task_id="extract_icfes")
    def extract_icfes_data():
        """
        Extrae archivos CSV de ICFES (2014-2023).
        Busca archivos existentes en data/ o descarga si es necesario.
        
        Returns:
            dict: {"raw_csvs": [lista de rutas a CSVs]}
        """
        print("=" * 60)
        print("📥 EXTRAYENDO DATOS ICFES")
        print("=" * 60)
        
        result = extract_icfes.run()
        
        if not result or not result.get("raw_csvs"):
            raise RuntimeError("❌ extract_icfes no devolvió archivos CSV")
        
        print(f"✅ {len(result['raw_csvs'])} archivos ICFES encontrados")
        return result

    # ========================================
    # TAREA 2: TRANSFORMAR ICFES
    # ========================================
    @task(task_id="transform_icfes")
    def transform_icfes_data(extract_result):
        """
        Transforma y consolida todos los CSV de ICFES en uno solo.
        Ordena por año y normaliza columnas.
        
        Args:
            extract_result: Output de extract_icfes_data
            
        Returns:
            dict: {"icfes_merged_csv": ruta al CSV consolidado}
        """
        print("=" * 60)
        print("🔄 TRANSFORMANDO DATOS ICFES")
        print("=" * 60)
        
        if not extract_result or not extract_result.get("raw_csvs"):
            raise RuntimeError("❌ No hay archivos ICFES para transformar")
        
        result = transform_icfes.run(raw_csvs=extract_result["raw_csvs"])
        
        if not result or not result.get("icfes_merged_csv"):
            raise RuntimeError("❌ transform_icfes no generó archivo consolidado")
        
        csv_path = Path(result["icfes_merged_csv"])
        if not csv_path.exists():
            raise RuntimeError(f"❌ Archivo ICFES consolidado no existe: {csv_path}")
        
        print(f"✅ ICFES consolidado: {csv_path}")
        print(f"   Filas: {format_number(result.get('rows'))}")
        return result

    # ========================================
    # TAREA 3: EXTRAER API PIB
    # ========================================
    @task(task_id="extract_api_pib")
    def extract_api_pib_data():
        """
        Extrae datos de PIB desde API o archivo local.
        
        Returns:
            dict: {"raw_csv": ruta al CSV crudo de PIB}
        """
        print("=" * 60)
        print("📥 EXTRAYENDO DATOS PIB (API)")
        print("=" * 60)
        
        result = extract_api.run()
        
        if not result or not result.get("raw_csv"):
            raise RuntimeError("❌ extract_api no devolvió archivo CSV")
        
        csv_path = Path(result["raw_csv"])
        if not csv_path.exists():
            raise RuntimeError(f"❌ Archivo PIB no existe: {csv_path}")
        
        print(f"✅ PIB extraído: {csv_path}")
        return result

    # ========================================
    # TAREA 4: TRANSFORMAR API PIB
    # ========================================
    @task(task_id="transform_api_pib")
    def transform_api_pib_data(extract_result):
        """
        Transforma datos de PIB: normaliza, agrega por año/depto.
        
        Args:
            extract_result: Output de extract_api_pib_data
            
        Returns:
            dict: {"pib_csv": ruta al CSV transformado}
        """
        print("=" * 60)
        print("🔄 TRANSFORMANDO DATOS PIB")
        print("=" * 60)
        
        if not extract_result or not extract_result.get("raw_csv"):
            raise RuntimeError("❌ No hay archivo PIB para transformar")
        
        result = transform_api.run(raw_csv=extract_result["raw_csv"])
        
        if not result or not result.get("pib_csv"):
            raise RuntimeError("❌ transform_api no generó archivo transformado")
        
        csv_path = Path(result["pib_csv"])
        if not csv_path.exists():
            raise RuntimeError(f"❌ Archivo PIB transformado no existe: {csv_path}")
        
        print(f"✅ PIB transformado: {csv_path}")
        print(f"   Filas: {format_number(result.get('rows'))}")
        return result

    # ========================================
    # TAREA 5: MERGE ICFES + PIB
    # ========================================
    @task(task_id="merge_icfes_pib")
    def merge_icfes_pib_data(icfes_result, pib_result):
        """
        Hace LEFT JOIN entre ICFES y PIB por (año, departamento).
        Cada estudiante recibe el PIB de su departamento/año.
        
        Args:
            icfes_result: Output de transform_icfes_data
            pib_result: Output de transform_api_pib_data
            
        Returns:
            dict: {"ddm_csv": ruta al DDM (Data Mart)}
        """
        print("=" * 60)
        print("🔀 MERGE: ICFES + PIB")
        print("=" * 60)
        
        # Validar inputs
        if not icfes_result or not icfes_result.get("icfes_merged_csv"):
            raise RuntimeError("❌ No hay archivo ICFES para merge")
        
        if not pib_result or not pib_result.get("pib_csv"):
            raise RuntimeError("❌ No hay archivo PIB para merge")
        
        icfes_csv = icfes_result["icfes_merged_csv"]
        pib_csv = pib_result["pib_csv"]
        
        # Ruta de salida del DDM
        out_csv = str((DATA_DIR / "ddm_icfes_pib.csv").resolve())
        
        print(f"📂 ICFES: {icfes_csv}")
        print(f"📂 PIB: {pib_csv}")
        print(f"📂 Output: {out_csv}")
        
        # Ejecutar merge
        result = merge.run(
            icfes_merged_csv=icfes_csv,
            pib_by_depto_csv=pib_csv,
            out_csv=out_csv
        )
        
        if not result or not result.get("ddm_csv"):
            raise RuntimeError("❌ merge.run() no devolvió ddm_csv")
        
        ddm_path = Path(result["ddm_csv"])
        if not ddm_path.exists():
            raise RuntimeError(f"❌ DDM no existe: {ddm_path}")
        
        print(f"✅ DDM creado: {ddm_path}")
        print(f"   Filas totales: {format_number(result.get('total_rows'))}")
        print(f"   Filas con PIB: {format_number(result.get('rows_with_pib'))}")
        print(f"   Cobertura PIB: {format_percentage(result.get('pib_coverage'))}")
        
        return result

    # ========================================
    # TAREA 6: INICIALIZAR DW (estructura)
    # ========================================
    @task(task_id="init_datawarehouse")
    def init_datawarehouse():
        """
        Inicializa la estructura del Data Warehouse en Postgres.
        Ejecuta init_ddm.sql (crea tabla, índices, vistas).
        
        Returns:
            dict: {"status": "ready"}
        """
        print("=" * 60)
        print("🏗️  INICIALIZANDO DATA WAREHOUSE")
        print("=" * 60)
        
        # Verificar que existe URI de conexión
        conn_uri = get_postgres_uri()
        
        if not conn_uri:
            # Intentar desde Connection de Airflow
            conn_id = os.getenv("POSTGRES_CONN_ID", "postgres_default")
            try:
                conn = BaseHook.get_connection(conn_id)
                conn_uri = conn.get_uri()
            except Exception as e:
                print(f"⚠️ No se pudo obtener conexión '{conn_id}': {e}")
        
        if not conn_uri:
            raise RuntimeError(
                "❌ No se encontró conexión a Postgres.\n"
                "Configura una de estas opciones:\n"
                "  1. Variable Airflow: airflow variables set POSTGRES_URI 'postgresql://...'\n"
                "  2. ENV var: export POSTGRES_URI='postgresql://...'\n"
                "  3. Airflow Connection: airflow connections add postgres_default --conn-uri 'postgresql://...'"
            )
        
        print(f"✅ Conexión Postgres configurada (URI length: {len(conn_uri)})")
        
        # Ejecutar script SQL de inicialización
        import psycopg2
        
        sql_file = BASE_DIR / "sql" / "init_ddm.sql"
        
        if not sql_file.exists():
            print(f"⚠️ Archivo SQL no encontrado: {sql_file}")
            print("   Usando DDL desde load_dw.py")
            return {"status": "ready", "method": "inline_ddl"}
        
        try:
            with psycopg2.connect(conn_uri) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    # Leer y ejecutar SQL
                    sql_content = sql_file.read_text(encoding="utf-8")
                    cur.execute(sql_content)
                    print(f"✅ Script SQL ejecutado: {sql_file.name}")
        except Exception as e:
            print(f"⚠️ Error ejecutando SQL: {e}")
            print("   load_dw.py creará la tabla si no existe")
        
        return {"status": "ready", "method": "sql_script"}

    # ========================================
    # TAREA 7: CARGAR A DW
    # ========================================
    @task(task_id="load_to_datawarehouse")
    def load_to_datawarehouse(ddm_result, init_result):
        """
        Carga el DDM (CSV) a Postgres usando COPY masivo.
        
        Args:
            ddm_result: Output de merge_icfes_pib_data
            init_result: Output de init_datawarehouse
            
        Returns:
            dict: Estadísticas de la carga
        """
        print("=" * 60)
        print("📤 CARGANDO DDM A POSTGRES")
        print("=" * 60)
        
        # Validar que DW está inicializado
        if not init_result or init_result.get("status") != "ready":
            raise RuntimeError("❌ Data Warehouse no está inicializado")
        
        # Validar que existe el DDM
        if not ddm_result or not ddm_result.get("ddm_csv"):
            raise RuntimeError("❌ No hay DDM para cargar")
        
        # Obtener ruta del DDM (preferir copia en DATA_DIR)
        ddm_csv = ddm_result.get("ddm_csv_data_dir") or ddm_result.get("ddm_csv")
        
        ddm_path = Path(ddm_csv)
        if not ddm_path.exists():
            raise RuntimeError(f"❌ DDM no existe: {ddm_path}")
        
        # Obtener URI de conexión
        conn_uri = get_postgres_uri()
        
        if not conn_uri:
            # Intentar desde Connection de Airflow
            conn_id = os.getenv("POSTGRES_CONN_ID", "postgres_default")
            try:
                conn = BaseHook.get_connection(conn_id)
                conn_uri = conn.get_uri()
            except Exception as e:
                raise RuntimeError(
                    f"❌ No se pudo obtener conexión Postgres: {e}\n"
                    "Configura POSTGRES_URI o crea una Connection en Airflow"
                )
        
        print(f"📂 DDM a cargar: {ddm_path}")
        print(f"🔌 Conexión: {conn_uri.split('@')[1] if '@' in conn_uri else 'configurada'}")
        
        # Ejecutar carga
        result = load_dw.run(
            path=str(ddm_path),
            conn_uri=conn_uri,
            replace=False  # No borrar datos existentes
        )
        
        if not result:
            raise RuntimeError("❌ load_dw.run() no devolvió resultado")
        
        print(f"✅ Carga completada:")
        print(f"   Tabla: {result.get('table', 'N/A')}")
        print(f"   Filas cargadas: {format_number(result.get('rows_loaded'))}")
        print(f"   Cobertura PIB: {format_percentage(result.get('pib_coverage'))}")
        
        return result

    # ========================================
    # ORQUESTACIÓN DEL DAG
    # ========================================
    
    # Extraer y transformar ICFES
    icfes_raw = extract_icfes_data()
    icfes_transformed = transform_icfes_data(icfes_raw)
    
    # Extraer y transformar PIB
    pib_raw = extract_api_pib_data()
    pib_transformed = transform_api_pib_data(pib_raw)
    
    # Merge de ambos
    ddm = merge_icfes_pib_data(icfes_transformed, pib_transformed)
    
    # Inicializar DW y cargar
    dw_init = init_datawarehouse()
    load_result = load_to_datawarehouse(ddm, dw_init)
    
    # Definir dependencias explícitas
    # ICFES y PIB se procesan en paralelo
    [icfes_transformed, pib_transformed] >> ddm
    
    # DW se inicializa independiente pero debe estar listo antes de cargar
    dw_init >> load_result
    ddm >> load_result

# Instanciar el DAG
dag_instance = etl_icfes_pib_project2()