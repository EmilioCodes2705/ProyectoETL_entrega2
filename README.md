# 📊 Proyecto ETL ICFES + PIB (Airflow + Postgres)

Este proyecto implementa un flujo **ETL (Extract, Transform, Load)** automatizado para combinar información de resultados del **ICFES** con datos del **PIB departamental**, generando una tabla lista para análisis y carga en un Data Warehouse (DW).

---

## 🚀 Objetivo General

Unir y limpiar los datos del **ICFES** y el **PIB por departamento y año**, garantizando:
- Integridad y consistencia de las llaves (`anio`, `depto_divipola`).
- Imputación de valores faltantes y nombres departamentales.
- Resultados en formato CSV listos para carga en **Postgres** y visualización en **Power BI**.

---

## 📂 Estructura del Proyecto

```
dags/
│
├── etl_icfes_postgres_local.py    # DAG principal de Airflow
│
├── scripts/
│   ├── extract_api.py             # Extrae datos desde API y los guarda en CSV
│   ├── transform_icfes.py         # Limpieza y normalización de datos ICFES
│   ├── transform_api.py           # Limpieza de datos PIB desde API
│   ├── merge.py                   # Une ICFES + PIB (usa año del ICFES)
│   └── load_dw.py                 # Carga final al DW en Postgres
│
└── data/
    ├── icfes_raw.csv              # Datos brutos del ICFES
    ├── pib_by_depto_year.csv      # PIB por depto/año
    ├── icfes_merged.csv           # Resultado del transform ICFES
    ├── ddm_icfes_pib.csv          # Resultado final del merge
    └── debug/                     # Muestras y validaciones
```

---

## ⚙️ Flujo ETL

1️⃣ **Extract** → Descarga datos del ICFES y PIB desde APIs o archivos CSV.  
2️⃣ **Transform** → Limpieza, validación y normalización de datos.  
3️⃣ **Merge** → Unión de ambos datasets, imputando valores nulos.  
4️⃣ **Load** → Carga final a Postgres y conexión a Power BI.

---

## 🧩 Cómo Ejecutar el Flujo Completo

### 1️⃣ Levantar los contenedores
```bash
docker compose up -d
```

### 2️⃣ Verificar la base de datos
```bash
docker exec -it etl_proyecto_entrega2-postgres-1 psql -U airflow
\l
\c airflow
\dt
```

### 3️⃣ Ejecutar el DAG en Airflow
Desde la interfaz de Airflow, ejecuta el DAG **etl_icfes_pib_project2**.

### 4️⃣ Verificar los resultados
```bash
docker exec -it etl_proyecto_entrega2-airflow-scheduler-1 bash
ls /opt/airflow/dags/data/
```

Debe mostrar:
```
icfes_merged.csv
pib_by_depto_year.csv
ddm_icfes_pib.csv
```

---

## 📊 Visualización en Power BI

1. En Power BI Desktop → Obtener datos → PostgreSQL Database  
2. Servidor: `localhost:5432`  
3. Base de datos: `airflow`  
4. Tabla: `dw_icfes_pib`  
5. Para excluir nulos:  
   - En **Transformar datos**, filtra `(blank)`  
   - O usa un filtro visual: “No está en blanco”

---

## ✅ Requisitos

| Recurso | Versión |
|----------|----------|
| Python | 3.7 – 3.11 |
| Airflow | 2.7+ |
| Pandas | 2.2+ |
| Postgres | 15+ |
| Docker | 24+ |

---

## 🧾 Créditos

Proyecto desarrollado por:
Autores: **Emilio Márquez, Samuel Uribe, Juan Pablo López**  
Fecha: 24 Octubre 2025
