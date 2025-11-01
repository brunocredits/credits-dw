-- ============================================================================
-- Script: 01-create-bronze-tables.sql
-- Descrição: Criação de TODAS as 16 tabelas da camada BRONZE
-- Camada: Bronze (Raw Data Layer)
-- Versão: 1.0
-- Data: Outubro 2025
-- ============================================================================
-- 
-- PRINCÍPIOS DA CAMADA BRONZE:
-- 1. Dados brutos EXATAMENTE como vieram das fontes
-- 2. TODOS os campos como VARCHAR ou TEXT
-- 3. Payload JSON completo salvo em campo JSONB
-- 4. Metadados obrigatórios: data_carga_bronze, nome_arquivo_origem
-- 5. NENHUMA validação ou transformação aplicada
--
-- ============================================================================

-- ============================================================================
-- FONTE: OneDrive (CSV)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Tabela: bronze.onedrive_clientes
-- Fonte: OneDrive/Clientes.csv
-- Frequência: Diária (6h)
-- Descrição: Dados cadastrais de clientes
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.onedrive_clientes (
    -- Identificadores
    cliente_id VARCHAR(50),
    codigo_cliente VARCHAR(50),
    cnpj_cpf VARCHAR(20),
    
    -- Dados cadastrais
    razao_social TEXT,
    nome_fantasia TEXT,
    tipo_pessoa VARCHAR(20),
    
    -- Contatos
    email TEXT,
    telefone VARCHAR(50),
    celular VARCHAR(50),
    
    -- Endereço
    logradouro TEXT,
    numero VARCHAR(20),
    complemento TEXT,
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    estado VARCHAR(2),
    cep VARCHAR(10),
    
    -- Classificação comercial
    segmento TEXT,
    porte_empresa VARCHAR(50),
    consultor_responsavel VARCHAR(100),
    status_cliente VARCHAR(50),
    
    -- Metadados de auditoria (OBRIGATÓRIOS)
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_onedrive_clientes_carga ON bronze.onedrive_clientes(data_carga_bronze DESC);
COMMENT ON TABLE bronze.onedrive_clientes IS 'Bronze - Clientes do OneDrive sem transformação';

-- ----------------------------------------------------------------------------
-- Tabela: bronze.onedrive_contratos
-- Fonte: OneDrive/Contratos.csv
-- Frequência: Diária (6h)
-- Descrição: Contratos vigentes e históricos
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.onedrive_contratos (
    contrato_id VARCHAR(50),
    numero_contrato VARCHAR(50),
    cliente_id VARCHAR(50),
    
    -- Datas (mantidas como texto na Bronze)
    data_inicio VARCHAR(20),
    data_fim VARCHAR(20),
    data_assinatura VARCHAR(20),
    
    -- Valores (mantidos como texto na Bronze)
    valor_total VARCHAR(50),
    valor_mensal VARCHAR(50),
    moeda VARCHAR(10),
    
    -- Status e metadados
    status_contrato VARCHAR(50),
    tipo_contrato VARCHAR(50),
    modalidade_pagamento VARCHAR(50),
    observacoes TEXT,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_onedrive_contratos_carga ON bronze.onedrive_contratos(data_carga_bronze DESC);
COMMENT ON TABLE bronze.onedrive_contratos IS 'Bronze - Contratos do OneDrive sem transformação';

-- ----------------------------------------------------------------------------
-- Tabela: bronze.onedrive_produtos
-- Fonte: OneDrive/Produtos.csv
-- Frequência: Semanal (segunda-feira 6h)
-- Descrição: Catálogo de produtos e serviços
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.onedrive_produtos (
    produto_id VARCHAR(50),
    codigo_produto VARCHAR(50),
    nome_produto TEXT,
    descricao TEXT,
    categoria VARCHAR(100),
    subcategoria VARCHAR(100),
    preco_base VARCHAR(50),
    unidade_medida VARCHAR(20),
    status_produto VARCHAR(50),
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_onedrive_produtos_carga ON bronze.onedrive_produtos(data_carga_bronze DESC);
COMMENT ON TABLE bronze.onedrive_produtos IS 'Bronze - Produtos do OneDrive sem transformação';

-- ----------------------------------------------------------------------------
-- Tabela: bronze.onedrive_precificacao
-- Fonte: OneDrive/Precificacao.csv
-- Frequência: Mensal (1º dia útil do mês, 6h)
-- Descrição: Tabela de preços negociados por cliente
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.onedrive_precificacao (
    precificacao_id VARCHAR(50),
    cliente_id VARCHAR(50),
    produto_id VARCHAR(50),
    preco_negociado VARCHAR(50),
    desconto_percentual VARCHAR(20),
    data_inicio_vigencia VARCHAR(20),
    data_fim_vigencia VARCHAR(20),
    observacoes TEXT,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_onedrive_precif_carga ON bronze.onedrive_precificacao(data_carga_bronze DESC);
COMMENT ON TABLE bronze.onedrive_precificacao IS 'Bronze - Precificação do OneDrive sem transformação';

-- ============================================================================
-- FONTE: Ploomes CRM (API REST/JSON)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Tabela: bronze.ploomes_deals
-- Fonte: API Ploomes /Deals
-- Frequência: A cada 4 horas (6h, 10h, 14h, 18h)
-- Descrição: Oportunidades de venda (pipeline comercial)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.ploomes_deals (
    deal_id VARCHAR(50),
    deal_title TEXT,
    deal_amount VARCHAR(50),
    deal_stage VARCHAR(100),
    deal_status VARCHAR(50),
    
    contact_id VARCHAR(50),
    contact_name TEXT,
    
    organization_id VARCHAR(50),
    organization_name TEXT,
    
    owner_id VARCHAR(50),
    owner_name TEXT,
    
    created_date VARCHAR(30),
    last_modified_date VARCHAR(30),
    expected_close_date VARCHAR(30),
    actual_close_date VARCHAR(30),
    
    win_probability VARCHAR(10),
    loss_reason TEXT,
    
    -- Payload JSON completo
    json_completo JSONB,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_ploomes_deals_carga ON bronze.ploomes_deals(data_carga_bronze DESC);
CREATE INDEX idx_bronze_ploomes_deals_json ON bronze.ploomes_deals USING GIN (json_completo);
COMMENT ON TABLE bronze.ploomes_deals IS 'Bronze - Deals do Ploomes CRM sem transformação';

-- ----------------------------------------------------------------------------
-- Tabela: bronze.ploomes_contacts
-- Fonte: API Ploomes /Contacts
-- Frequência: Diária (7h)
-- Descrição: Contatos do CRM
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.ploomes_contacts (
    contact_id VARCHAR(50),
    contact_name TEXT,
    email TEXT,
    telefone VARCHAR(50),
    celular VARCHAR(50),
    cargo VARCHAR(100),
    
    organization_id VARCHAR(50),
    organization_name TEXT,
    
    created_date VARCHAR(30),
    last_contact_date VARCHAR(30),
    
    -- Payload JSON completo
    json_completo JSONB,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_ploomes_contacts_carga ON bronze.ploomes_contacts(data_carga_bronze DESC);
CREATE INDEX idx_bronze_ploomes_contacts_json ON bronze.ploomes_contacts USING GIN (json_completo);
COMMENT ON TABLE bronze.ploomes_contacts IS 'Bronze - Contacts do Ploomes CRM sem transformação';

-- ----------------------------------------------------------------------------
-- Tabela: bronze.ploomes_organizations
-- Fonte: API Ploomes /Organizations
-- Frequência: Diária (7h)
-- Descrição: Empresas/Organizações do CRM
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.ploomes_organizations (
    organization_id VARCHAR(50),
    organization_name TEXT,
    cnpj VARCHAR(20),
    segmento VARCHAR(100),
    porte VARCHAR(50),
    website TEXT,
    telefone VARCHAR(50),
    
    logradouro TEXT,
    cidade VARCHAR(100),
    estado VARCHAR(2),
    cep VARCHAR(10),
    
    owner_id VARCHAR(50),
    created_date VARCHAR(30),
    
    -- Payload JSON completo
    json_completo JSONB,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_ploomes_orgs_carga ON bronze.ploomes_organizations(data_carga_bronze DESC);
CREATE INDEX idx_bronze_ploomes_orgs_json ON bronze.ploomes_organizations USING GIN (json_completo);
COMMENT ON TABLE bronze.ploomes_organizations IS 'Bronze - Organizations do Ploomes CRM sem transformação';

-- ============================================================================
-- FONTE: Omie ERP (API REST/JSON)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Tabela: bronze.omie_notas_fiscais
-- Fonte: API Omie /NotasFiscais
-- Frequência: A cada 2 horas (6h, 8h, 10h, 12h, 14h, 16h, 18h)
-- Descrição: Notas fiscais emitidas
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.omie_notas_fiscais (
    nf_id VARCHAR(50),
    numero_nf VARCHAR(50),
    serie_nf VARCHAR(10),
    
    cliente_id VARCHAR(50),
    cliente_nome TEXT,
    cnpj_cliente VARCHAR(20),
    
    data_emissao VARCHAR(20),
    data_vencimento VARCHAR(20),
    
    valor_total VARCHAR(50),
    valor_produtos VARCHAR(50),
    valor_servicos VARCHAR(50),
    valor_desconto VARCHAR(50),
    valor_impostos VARCHAR(50),
    
    status_nf VARCHAR(50),
    tipo_operacao VARCHAR(50),
    chave_acesso VARCHAR(100),
    
    -- Payload JSON completo
    json_completo JSONB,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_omie_nf_carga ON bronze.omie_notas_fiscais(data_carga_bronze DESC);
CREATE INDEX idx_bronze_omie_nf_json ON bronze.omie_notas_fiscais USING GIN (json_completo);
COMMENT ON TABLE bronze.omie_notas_fiscais IS 'Bronze - Notas Fiscais do Omie sem transformação';

-- ----------------------------------------------------------------------------
-- Tabela: bronze.omie_contas_receber
-- Fonte: API Omie /ContasReceber
-- Frequência: A cada 4 horas (6h, 10h, 14h, 18h)
-- Descrição: Contas a receber
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.omie_contas_receber (
    conta_receber_id VARCHAR(50),
    numero_documento VARCHAR(50),
    
    cliente_id VARCHAR(50),
    cliente_nome TEXT,
    nf_id VARCHAR(50),
    
    data_emissao VARCHAR(20),
    data_vencimento VARCHAR(20),
    data_recebimento VARCHAR(20),
    
    valor_documento VARCHAR(50),
    valor_recebido VARCHAR(50),
    valor_juros VARCHAR(50),
    valor_desconto VARCHAR(50),
    
    status_titulo VARCHAR(50),
    forma_pagamento VARCHAR(50),
    
    -- Payload JSON completo
    json_completo JSONB,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_omie_cr_carga ON bronze.omie_contas_receber(data_carga_bronze DESC);
CREATE INDEX idx_bronze_omie_cr_json ON bronze.omie_contas_receber USING GIN (json_completo);
COMMENT ON TABLE bronze.omie_contas_receber IS 'Bronze - Contas a Receber do Omie sem transformação';

-- ----------------------------------------------------------------------------
-- Tabela: bronze.omie_contas_pagar
-- Fonte: API Omie /ContasPagar
-- Frequência: A cada 4 horas (6h, 10h, 14h, 18h)
-- Descrição: Contas a pagar (despesas)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.omie_contas_pagar (
    conta_pagar_id VARCHAR(50),
    numero_documento VARCHAR(50),
    
    fornecedor_id VARCHAR(50),
    fornecedor_nome TEXT,
    
    data_emissao VARCHAR(20),
    data_vencimento VARCHAR(20),
    data_pagamento VARCHAR(20),
    
    valor_documento VARCHAR(50),
    valor_pago VARCHAR(50),
    valor_juros VARCHAR(50),
    valor_desconto VARCHAR(50),
    
    status_titulo VARCHAR(50),
    categoria_despesa VARCHAR(100),
    
    -- Payload JSON completo
    json_completo JSONB,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_omie_cp_carga ON bronze.omie_contas_pagar(data_carga_bronze DESC);
CREATE INDEX idx_bronze_omie_cp_json ON bronze.omie_contas_pagar USING GIN (json_completo);
COMMENT ON TABLE bronze.omie_contas_pagar IS 'Bronze - Contas a Pagar do Omie sem transformação';

-- ============================================================================
-- FONTE: Movidesk (API REST/JSON)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Tabela: bronze.movidesk_tickets
-- Fonte: API Movidesk /Tickets
-- Frequência: A cada 1 hora (7h às 19h)
-- Descrição: Tickets de atendimento/suporte
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.movidesk_tickets (
    ticket_id VARCHAR(50),
    numero_ticket VARCHAR(50),
    
    cliente_id VARCHAR(50),
    cliente_nome TEXT,
    
    assunto TEXT,
    categoria VARCHAR(100),
    urgencia VARCHAR(50),
    prioridade VARCHAR(50),
    status_ticket VARCHAR(50),
    
    agente_responsavel TEXT,
    
    data_criacao VARCHAR(30),
    data_atualizacao VARCHAR(30),
    data_fechamento VARCHAR(30),
    
    sla_primeira_resposta VARCHAR(50),
    sla_resolucao VARCHAR(50),
    tempo_resolucao_horas VARCHAR(20),
    
    satisfacao_cliente VARCHAR(20),
    
    -- Payload JSON completo
    json_completo JSONB,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_movidesk_tickets_carga ON bronze.movidesk_tickets(data_carga_bronze DESC);
CREATE INDEX idx_bronze_movidesk_tickets_json ON bronze.movidesk_tickets USING GIN (json_completo);
COMMENT ON TABLE bronze.movidesk_tickets IS 'Bronze - Tickets do Movidesk sem transformação';

-- ============================================================================
-- FONTE: Parceiros (APIs REST/JSON)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Tabela: bronze.finqi_consumo
-- Fonte: API Finqi /Consumo
-- Frequência: Diária (7h)
-- Descrição: Consumo de serviços Finqi pelos clientes
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.finqi_consumo (
    consumo_id VARCHAR(50),
    cliente_id VARCHAR(50),
    produto_servico TEXT,
    data_consumo VARCHAR(20),
    quantidade VARCHAR(50),
    valor_unitario VARCHAR(50),
    valor_total VARCHAR(50),
    tipo_transacao VARCHAR(100),
    
    -- Payload JSON completo
    json_completo JSONB,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_finqi_carga ON bronze.finqi_consumo(data_carga_bronze DESC);
CREATE INDEX idx_bronze_finqi_json ON bronze.finqi_consumo USING GIN (json_completo);
COMMENT ON TABLE bronze.finqi_consumo IS 'Bronze - Consumo Finqi sem transformação';

-- ----------------------------------------------------------------------------
-- Tabela: bronze.salesbox_consumo
-- Fonte: API Salesbox /Uso
-- Frequência: Diária (7h)
-- Descrição: Consumo/uso de produtos Salesbox
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.salesbox_consumo (
    uso_id VARCHAR(50),
    cliente_id VARCHAR(50),
    produto_servico TEXT,
    data_uso VARCHAR(20),
    quantidade_uso VARCHAR(50),
    valor_unitario VARCHAR(50),
    valor_total VARCHAR(50),
    
    -- Payload JSON completo
    json_completo JSONB,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_salesbox_carga ON bronze.salesbox_consumo(data_carga_bronze DESC);
CREATE INDEX idx_bronze_salesbox_json ON bronze.salesbox_consumo USING GIN (json_completo);
COMMENT ON TABLE bronze.salesbox_consumo IS 'Bronze - Consumo Salesbox sem transformação';

-- ----------------------------------------------------------------------------
-- Tabela: bronze.acertpix_transacoes
-- Fonte: API Acertpix /Transacoes
-- Frequência: A cada 4 horas (6h, 10h, 14h, 18h)
-- Descrição: Transações PIX processadas
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.acertpix_transacoes (
    transacao_id VARCHAR(50),
    cliente_id VARCHAR(50),
    tipo_transacao VARCHAR(50),
    data_transacao VARCHAR(30),
    valor_transacao VARCHAR(50),
    status_transacao VARCHAR(50),
    chave_pix VARCHAR(100),
    
    -- Payload JSON completo
    json_completo JSONB,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_acertpix_carga ON bronze.acertpix_transacoes(data_carga_bronze DESC);
CREATE INDEX idx_bronze_acertpix_json ON bronze.acertpix_transacoes USING GIN (json_completo);
COMMENT ON TABLE bronze.acertpix_transacoes IS 'Bronze - Transações Acertpix sem transformação';

-- ----------------------------------------------------------------------------
-- Tabela: bronze.spc_consultas
-- Fonte: API SPC Brasil /Consultas
-- Frequência: Sob demanda (tempo real)
-- Descrição: Consultas de crédito realizadas
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.spc_consultas (
    consulta_id VARCHAR(50),
    cliente_id VARCHAR(50),
    cpf_cnpj_consultado VARCHAR(20),
    data_consulta VARCHAR(30),
    score_credito VARCHAR(10),
    situacao_credito VARCHAR(50),
    restricoes TEXT,
    
    -- Payload JSON completo
    json_completo JSONB,
    
    -- Metadados de auditoria
    data_carga_bronze TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nome_arquivo_origem VARCHAR(255)
);

CREATE INDEX idx_bronze_spc_carga ON bronze.spc_consultas(data_carga_bronze DESC);
CREATE INDEX idx_bronze_spc_json ON bronze.spc_consultas USING GIN (json_completo);
COMMENT ON TABLE bronze.spc_consultas IS 'Bronze - Consultas SPC Brasil sem transformação';

-- ============================================================================
-- MENSAGEM DE CONCLUSÃO
-- ============================================================================

DO $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM information_schema.tables
    WHERE table_schema = 'bronze';
    
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Camada BRONZE criada com sucesso!';
    RAISE NOTICE '';
    RAISE NOTICE 'Total de tabelas criadas: %', v_count;
    RAISE NOTICE '';
    RAISE NOTICE 'Tabelas OneDrive (4):';
    RAISE NOTICE '  ✓ bronze.onedrive_clientes';
    RAISE NOTICE '  ✓ bronze.onedrive_contratos';
    RAISE NOTICE '  ✓ bronze.onedrive_produtos';
    RAISE NOTICE '  ✓ bronze.onedrive_precificacao';
    RAISE NOTICE '';
    RAISE NOTICE 'Tabelas Ploomes CRM (3):';
    RAISE NOTICE '  ✓ bronze.ploomes_deals';
    RAISE NOTICE '  ✓ bronze.ploomes_contacts';
    RAISE NOTICE '  ✓ bronze.ploomes_organizations';
    RAISE NOTICE '';
    RAISE NOTICE 'Tabelas Omie ERP (3):';
    RAISE NOTICE '  ✓ bronze.omie_notas_fiscais';
    RAISE NOTICE '  ✓ bronze.omie_contas_receber';
    RAISE NOTICE '  ✓ bronze.omie_contas_pagar';
    RAISE NOTICE '';
    RAISE NOTICE 'Tabelas Movidesk (1):';
    RAISE NOTICE '  ✓ bronze.movidesk_tickets';
    RAISE NOTICE '';
    RAISE NOTICE 'Tabelas Parceiros (4):';
    RAISE NOTICE '  ✓ bronze.finqi_consumo';
    RAISE NOTICE '  ✓ bronze.salesbox_consumo';
    RAISE NOTICE '  ✓ bronze.acertpix_transacoes';
    RAISE NOTICE '  ✓ bronze.spc_consultas';
    RAISE NOTICE '============================================================================';
END $$;
