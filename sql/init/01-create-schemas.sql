-- ============================================================================
-- Script: 01-create-schemas.sql
-- Descrição: Criação dos schemas e roles do Data Warehouse Credits Brasil
-- Versão: 1.0
-- Data: Outubro 2025
-- ============================================================================

-- ============================================================================
-- 1. CRIAÇÃO DOS SCHEMAS
-- ============================================================================

-- Schema Bronze: Dados brutos das fontes
CREATE SCHEMA IF NOT EXISTS bronze;

-- Schema Credits (Silver): Dados limpos e validados
CREATE SCHEMA IF NOT EXISTS credits;

COMMENT ON SCHEMA bronze IS 'Camada Bronze - Dados brutos sem transformação';
COMMENT ON SCHEMA credits IS 'Camada Silver/Gold - Dados transformados e views analíticas';

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

-- Role: dw_analyst (Analistas - leitura Silver/Gold)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dw_analyst') THEN
        CREATE ROLE dw_analyst WITH LOGIN PASSWORD 'Analyst@Credits2024!';
    END IF;
END
$$;

-- Role: dw_viewer (Visualização - somente leitura Gold)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dw_viewer') THEN
        CREATE ROLE dw_viewer WITH LOGIN PASSWORD 'Viewer@Credits2024!';
    END IF;
END
$$;

-- ============================================================================
-- 3. CONCESSÃO DE PERMISSÕES
-- ============================================================================

-- Permissões dw_admin (SUPERUSER)
ALTER ROLE dw_admin WITH SUPERUSER CREATEDB CREATEROLE;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO dw_admin;
GRANT ALL PRIVILEGES ON SCHEMA credits TO dw_admin;

-- Permissões dw_developer (Leitura/Escrita em Bronze e Silver)
GRANT USAGE, CREATE ON SCHEMA bronze TO dw_developer;
GRANT USAGE, CREATE ON SCHEMA credits TO dw_developer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO dw_developer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA credits TO dw_developer;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO dw_developer;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA credits TO dw_developer;

-- Permissões futuras para dw_developer
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO dw_developer;
ALTER DEFAULT PRIVILEGES IN SCHEMA credits GRANT ALL ON TABLES TO dw_developer;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON SEQUENCES TO dw_developer;
ALTER DEFAULT PRIVILEGES IN SCHEMA credits GRANT ALL ON SEQUENCES TO dw_developer;

-- Permissões dw_analyst (Leitura em Credits)
GRANT USAGE ON SCHEMA credits TO dw_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA credits TO dw_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA credits GRANT SELECT ON TABLES TO dw_analyst;

-- Permissões dw_viewer (Somente views Gold)
GRANT USAGE ON SCHEMA credits TO dw_viewer;
-- Permissões específicas serão concedidas apenas para views (vw_*)

-- ============================================================================
-- 4. TABELA DE AUDITORIA
-- ============================================================================

CREATE TABLE IF NOT EXISTS credits.historico_atualizacoes (
    id SERIAL PRIMARY KEY,
    script_nome VARCHAR(255) NOT NULL,
    camada VARCHAR(10) CHECK (camada IN ('bronze', 'silver', 'gold')),
    tabela_origem VARCHAR(255),
    tabela_destino VARCHAR(255),
    data_inicio TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_fim TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('em_execucao', 'sucesso', 'erro', 'cancelado')),
    linhas_processadas INTEGER,
    linhas_inseridas INTEGER,
    linhas_atualizadas INTEGER,
    linhas_erro INTEGER,
    mensagem_erro TEXT,
    usuario VARCHAR(100) DEFAULT CURRENT_USER,
    CONSTRAINT chk_datas CHECK (data_fim IS NULL OR data_fim >= data_inicio)
);

CREATE INDEX idx_historico_data_inicio ON credits.historico_atualizacoes(data_inicio DESC);
CREATE INDEX idx_historico_status ON credits.historico_atualizacoes(status);
CREATE INDEX idx_historico_camada ON credits.historico_atualizacoes(camada);

COMMENT ON TABLE credits.historico_atualizacoes IS 'Registro de todas as execuções de ETL';
COMMENT ON COLUMN credits.historico_atualizacoes.script_nome IS 'Nome do script Python ou SQL executado';
COMMENT ON COLUMN credits.historico_atualizacoes.camada IS 'Camada do DW (bronze, silver ou gold)';

-- ============================================================================
-- 5. FUNÇÃO AUXILIAR PARA REGISTRO DE EXECUÇÃO
-- ============================================================================

CREATE OR REPLACE FUNCTION credits.registrar_execucao_etl(
    p_script_nome VARCHAR,
    p_camada VARCHAR,
    p_tabela_origem VARCHAR DEFAULT NULL,
    p_tabela_destino VARCHAR DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO credits.historico_atualizacoes (
        script_nome,
        camada,
        tabela_origem,
        tabela_destino,
        data_inicio,
        status
    ) VALUES (
        p_script_nome,
        p_camada,
        p_tabela_origem,
        p_tabela_destino,
        CURRENT_TIMESTAMP,
        'em_execucao'
    ) RETURNING id INTO v_id;
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION credits.finalizar_execucao_etl(
    p_id INTEGER,
    p_status VARCHAR,
    p_linhas_processadas INTEGER DEFAULT 0,
    p_linhas_inseridas INTEGER DEFAULT 0,
    p_linhas_atualizadas INTEGER DEFAULT 0,
    p_linhas_erro INTEGER DEFAULT 0,
    p_mensagem_erro TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE credits.historico_atualizacoes
    SET 
        data_fim = CURRENT_TIMESTAMP,
        status = p_status,
        linhas_processadas = p_linhas_processadas,
        linhas_inseridas = p_linhas_inseridas,
        linhas_atualizadas = p_linhas_atualizadas,
        linhas_erro = p_linhas_erro,
        mensagem_erro = p_mensagem_erro
    WHERE id = p_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 6. EXTENSÕES ÚTEIS
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
    RAISE NOTICE 'Schemas e roles criados com sucesso!';
    RAISE NOTICE '';
    RAISE NOTICE 'Schemas disponíveis:';
    RAISE NOTICE '  - bronze (dados brutos)';
    RAISE NOTICE '  - credits (dados limpos + views)';
    RAISE NOTICE '';
    RAISE NOTICE 'Roles criados:';
    RAISE NOTICE '  - dw_admin (administrador completo)';
    RAISE NOTICE '  - dw_developer (leitura/escrita bronze e silver)';
    RAISE NOTICE '  - dw_analyst (leitura credits)';
    RAISE NOTICE '  - dw_viewer (leitura apenas views gold)';
    RAISE NOTICE '============================================================================';
END $$;
