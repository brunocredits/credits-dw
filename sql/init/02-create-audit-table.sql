
-- Descrição: Criação do schema 'credits' e da tabela de auditoria 'historico_atualizacoes'
-- Data: Novembro 2025
-- ============================================================================

-- 1. CRIAÇÃO DO SCHEMA 'credits'
CREATE SCHEMA IF NOT EXISTS credits;

COMMENT ON SCHEMA credits IS 'Schema para tabelas de controle e auditoria do DW';

-- 2. CRIAÇÃO DA TABELA 'historico_atualizacoes'
CREATE TABLE IF NOT EXISTS credits.historico_atualizacoes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    script_nome VARCHAR(255) NOT NULL,
    camada VARCHAR(50) NOT NULL,
    tabela_origem VARCHAR(255),
    tabela_destino VARCHAR(255),
    data_inicio TIMESTAMP NOT NULL,
    data_fim TIMESTAMP,
    status VARCHAR(50) NOT NULL, -- 'em_execucao', 'sucesso', 'erro', 'cancelado'
    linhas_processadas INTEGER,
    linhas_inseridas INTEGER,
    linhas_atualizadas INTEGER,
    linhas_erro INTEGER,
    mensagem_erro TEXT
);

COMMENT ON TABLE credits.historico_atualizacoes IS 'Histórico de execuções dos scripts ETL';
COMMENT ON COLUMN credits.historico_atualizacoes.id IS 'Identificador único da execução';
COMMENT ON COLUMN credits.historico_atualizacoes.script_nome IS 'Nome do script ETL executado';
COMMENT ON COLUMN credits.historico_atualizacoes.camada IS 'Camada do Data Warehouse (bronze, silver, gold)';
COMMENT ON COLUMN credits.historico_atualizacoes.tabela_origem IS 'Nome da tabela ou fonte de dados de origem';
COMMENT ON COLUMN credits.historico_atualizacoes.tabela_destino IS 'Nome da tabela de destino no DW';
COMMENT ON COLUMN credits.historico_atualizacoes.data_inicio IS 'Timestamp de início da execução';
COMMENT ON COLUMN credits.historico_atualizacoes.data_fim IS 'Timestamp de fim da execução';
COMMENT ON COLUMN credits.historico_atualizacoes.status IS 'Status da execução (em_execucao, sucesso, erro, cancelado)';
COMMENT ON COLUMN credits.historico_atualizacoes.linhas_processadas IS 'Número total de linhas processadas';
COMMENT ON COLUMN credits.historico_atualizacoes.linhas_inseridas IS 'Número de linhas inseridas na tabela de destino';
COMMENT ON COLUMN credits.historico_atualizacoes.linhas_atualizadas IS 'Número de linhas atualizadas na tabela de destino';
COMMENT ON COLUMN credits.historico_atualizacoes.linhas_erro IS 'Número de linhas com erro durante o processamento';
COMMENT ON COLUMN credits.historico_atualizacoes.mensagem_erro IS 'Mensagem de erro, se a execução falhou';

-- 3. CONCESSÃO DE PERMISSÕES PARA AS ROLES EXISTENTES
GRANT USAGE ON SCHEMA credits TO dw_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA credits TO dw_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA credits TO dw_admin;

GRANT USAGE ON SCHEMA credits TO dw_developer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA credits TO dw_developer;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA credits TO dw_developer;

-- Permissões futuras para dw_developer
ALTER DEFAULT PRIVILEGES IN SCHEMA credits GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO dw_developer;
ALTER DEFAULT PRIVILEGES IN SCHEMA credits GRANT USAGE ON SEQUENCES TO dw_developer;

-- MENSAGEM DE CONCLUSÃO
DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Schema CREDITS e tabela historico_atualizacoes criados com sucesso!';
    RAISE NOTICE '============================================================================';
END $$;
