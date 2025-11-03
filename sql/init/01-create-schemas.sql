-- ============================================================================
-- Script: 01-create-schemas.sql
-- Descrição: Criação dos schemas e roles do Data Warehouse Credits Brasil
-- Versão: 2.1
-- Data: Novembro 2025
-- ============================================================================

-- ============================================================================
-- 1. CRIAÇÃO DOS SCHEMAS
-- ============================================================================

-- Schema Bronze: Dados brutos das fontes
CREATE SCHEMA IF NOT EXISTS bronze;

COMMENT ON SCHEMA bronze IS 'Camada Bronze - Dados brutos sem transformação';


-- ============================================================================
-- 2. CRIAÇÃO DE ROLES E PERMISSÕES
-- ============================================================================

-- Role: dw_admin (Administrador completo)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dw_admin') THEN
        CREATE ROLE dw_admin WITH LOGIN PASSWORD 'Admin@Credits2024!';
    END IF;
END
$$;

-- Role: dw_developer (Desenvolvimento - leitura/escrita)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dw_developer') THEN
        CREATE ROLE dw_developer WITH LOGIN PASSWORD 'Dev@Credits2024!';
    END IF;
END
$$;

-- ============================================================================
-- 3. CONCESSÃO DE PERMISSÕES
-- ============================================================================

-- Permissões dw_admin (SUPERUSER)
ALTER ROLE dw_admin WITH SUPERUSER CREATEDB CREATEROLE;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO dw_admin;

-- Permissões dw_developer (Leitura/Escrita em Bronze)
GRANT USAGE, CREATE ON SCHEMA bronze TO dw_developer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO dw_developer;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO dw_developer;

-- Permissões futuras para dw_developer
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO dw_developer;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON SEQUENCES TO dw_developer;


-- ============================================================================
-- 4. EXTENSÕES ÚTEIS
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";      -- Geração de UUIDs
CREATE EXTENSION IF NOT EXISTS "pg_trgm";        -- Busca por similaridade de texto
CREATE EXTENSION IF NOT EXISTS "btree_gin";      -- Índices GIN mais eficientes

-- ============================================================================
-- MENSAGEM DE CONCLUSÃO
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Schema BRONZE e roles criados com sucesso!';
    RAISE NOTICE '';
    RAISE NOTICE 'Schemas disponíveis:';
    RAISE NOTICE '  - bronze (dados brutos)';
    RAISE NOTICE '';
    RAISE NOTICE 'Roles criados:';
    RAISE NOTICE '  - dw_admin (administrador completo)';
    RAISE NOTICE '  - dw_developer (leitura/escrita bronze)';
    RAISE NOTICE '============================================================================';
END $$;
