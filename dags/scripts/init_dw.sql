-- dags/sql/init_ddm.sql
-- Inicializa el Data Warehouse para el proyecto ICFES + PIB
-- SIMPLIFICADO: Solo crea estructura básica, load_dw.py maneja las columnas dinámicamente

\echo ''
\echo '=================================================='
\echo '🔄 Iniciando configuración del Data Warehouse'
\echo '=================================================='
\echo ''

-- ============================================
-- 1. LIMPIAR OBJETOS EXISTENTES
-- ============================================

\echo '🧹 Limpiando objetos existentes...'

-- Eliminar vistas (primero porque dependen de la tabla)
DROP VIEW IF EXISTS public.v_correlacion_pib_puntajes CASCADE;
DROP VIEW IF EXISTS public.v_cobertura_pib CASCADE;
DROP VIEW IF EXISTS public.v_top_colegios_depto CASCADE;
DROP VIEW IF EXISTS public.v_promedios_depto_anio CASCADE;

\echo '   ✓ Vistas eliminadas'

-- Eliminar triggers
DROP TRIGGER IF EXISTS trigger_update_ddm_updated_at ON public.ddm_icfes_pib CASCADE;

\echo '   ✓ Triggers eliminados'

-- Eliminar funciones
DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;

\echo '   ✓ Funciones eliminadas'

-- Eliminar tablas (incluye nombres antiguos por si acaso)
DROP TABLE IF EXISTS public.ddm_icfes_pib CASCADE;
DROP TABLE IF EXISTS public.icfes_ddm CASCADE;
DROP TABLE IF EXISTS public.ddm_icfes CASCADE;

\echo '   ✓ Tablas eliminadas'
\echo ''

-- ============================================
-- 2. CREAR ROL DE ETL (si no existe)
-- ============================================

\echo '👤 Configurando rol de ETL...'

DO $$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_user') THEN
      CREATE ROLE etl_user LOGIN PASSWORD 'etl_password';
      RAISE NOTICE '   ✓ Rol etl_user creado';
   ELSE
      RAISE NOTICE '   ✓ Rol etl_user ya existe';
   END IF;
END$$;

\echo ''

-- ============================================
-- 3. PERMISOS EN ESQUEMA PUBLIC
-- ============================================

\echo '🔐 Configurando permisos en esquema...'

GRANT USAGE ON SCHEMA public TO etl_user;
GRANT CREATE ON SCHEMA public TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO etl_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO etl_user;

-- Permisos por defecto para objetos futuros
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE, SELECT ON SEQUENCES TO etl_user;

\echo '   ✓ Permisos configurados'
\echo ''

-- ============================================
-- NOTA IMPORTANTE
-- ============================================

\echo '📋 NOTA: La tabla ddm_icfes_pib se creará dinámicamente'
\echo '         por load_dw.py basándose en las columnas del CSV.'
\echo ''
\echo '         Las vistas analíticas se pueden crear después de'
\echo '         cargar los datos, cuando sepamos qué columnas existen.'
\echo ''

-- ============================================
-- 4. FUNCIÓN PARA UPDATED_AT (útil para futuro)
-- ============================================

\echo '⚙️  Creando función helper...'

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION update_updated_at_column() IS
  'Función de trigger que actualiza automáticamente la columna updated_at';

\echo '   ✓ Función update_updated_at_column creada'
\echo ''

-- ============================================
-- RESUMEN FINAL
-- ============================================

\echo '=================================================='
\echo '✅ INICIALIZACIÓN COMPLETADA'
\echo '=================================================='
\echo ''
\echo '📋 Objetos creados:'
\echo '   • Rol: etl_user'
\echo '   • Permisos: Configurados en schema public'
\echo '   • Función: update_updated_at_column()'
\echo ''
\echo '🔧 Próximos pasos:'
\echo '   1. Ejecutar load_dw.py para crear tabla y cargar datos'
\echo '   2. Crear vistas analíticas después de cargar'
\echo ''
\echo '📊 Para verificar:'
\echo '   \du'
\echo '   \dn+'
\echo ''
\echo '=================================================='
\echo ''