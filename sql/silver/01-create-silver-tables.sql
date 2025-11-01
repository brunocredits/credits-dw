-- ============================================================================
-- Script: 01-create-silver-tables.sql
-- Descrição: Criação de TODAS as tabelas da camada SILVER (credits.*)
-- Camada: Silver (Curated Data Layer)
-- Versão: 1.0
-- Data: Outubro 2025
-- ============================================================================
--
-- PRINCÍPIOS DA CAMADA SILVER:
-- 1. Dados LIMPOS e VALIDADOS
-- 2. Tipos de dados CORRETOS (DATE, NUMERIC, BOOLEAN)
-- 3. Chaves primárias (PKs) e estrangeiras (FKs)
-- 4. Deduplicação aplicada
-- 5. Padronização de categorias e status
-- 6. Merge de múltiplas fontes quando necessário
--
-- ============================================================================

-- ============================================================================
-- TABELA CENTRAL: credits.clientes
-- Origem: bronze.onedrive_clientes + bronze.ploomes_organizations (MERGE)
-- Descrição: Tabela mestre de clientes com dados consolidados
-- ============================================================================

CREATE TABLE IF NOT EXISTS credits.clientes (
    -- Chave primária
    cliente_pk SERIAL PRIMARY KEY,
    
    -- Identificadores
    cliente_id VARCHAR(50) UNIQUE NOT NULL,
    codigo_cliente VARCHAR(50),
    cnpj_cpf VARCHAR(20),
    
    -- Dados cadastrais (TIPOS CONVERTIDOS)
    razao_social VARCHAR(255) NOT NULL,
    nome_fantasia VARCHAR(255),
    tipo_pessoa CHAR(2) CHECK (tipo_pessoa IN ('PF', 'PJ')),
    
    -- Contatos (VALIDADOS)
    email VARCHAR(255),
    telefone VARCHAR(20),
    celular VARCHAR(20),
    website VARCHAR(255),
    
    -- Endereço (PADRONIZADO)
    logradouro VARCHAR(255),
    numero VARCHAR(10),
    complemento VARCHAR(100),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    estado CHAR(2),
    cep CHAR(8),  -- Sem máscara (somente números)
    
    -- Classificação
    segmento VARCHAR(100),
    porte_empresa VARCHAR(20) CHECK (porte_empresa IN ('MEI', 'ME', 'EPP', 'MEDIO', 'GRANDE')),
    consultor_responsavel VARCHAR(100),
    status_cliente VARCHAR(20) CHECK (status_cliente IN ('ATIVO', 'INATIVO', 'PROSPECT', 'SUSPENSO')),
    
    -- Flags de validação
    cpf_cnpj_valido BOOLEAN DEFAULT FALSE,
    email_valido BOOLEAN DEFAULT FALSE,
    
    -- Origem dos dados
    fonte_primaria VARCHAR(50),  -- ONEDRIVE ou PLOOMES
    
    -- Auditoria
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario_atualizacao VARCHAR(100) DEFAULT CURRENT_USER
);

-- Índices para performance
CREATE INDEX idx_clientes_cnpj ON credits.clientes(cnpj_cpf) WHERE cnpj_cpf IS NOT NULL;
CREATE INDEX idx_clientes_status ON credits.clientes(status_cliente);
CREATE INDEX idx_clientes_consultor ON credits.clientes(consultor_responsavel);
CREATE INDEX idx_clientes_segmento ON credits.clientes(segmento);
CREATE INDEX idx_clientes_razao_social ON credits.clientes USING GIN (to_tsvector('portuguese', razao_social));

COMMENT ON TABLE credits.clientes IS 'Silver - Tabela central de clientes com dados consolidados e validados';
COMMENT ON COLUMN credits.clientes.fonte_primaria IS 'Fonte de onde vieram os dados (ONEDRIVE ou PLOOMES)';

-- ============================================================================
-- TABELA: credits.contratos
-- Origem: bronze.onedrive_contratos
-- Descrição: Contratos com datas e valores convertidos
-- ============================================================================

CREATE TABLE IF NOT EXISTS credits.contratos (
    -- Chave primária
    contrato_pk SERIAL PRIMARY KEY,
    
    -- Identificadores
    contrato_id VARCHAR(50) UNIQUE NOT NULL,
    numero_contrato VARCHAR(50) UNIQUE,
    
    -- FK para clientes
    cliente_pk INTEGER NOT NULL REFERENCES credits.clientes(cliente_pk) ON DELETE CASCADE,
    
    -- Datas (CONVERTIDAS PARA DATE)
    data_inicio DATE NOT NULL,
    data_fim DATE,
    data_assinatura DATE,
    
    -- Valores (CONVERTIDOS PARA NUMERIC)
    valor_total NUMERIC(15,2),
    valor_mensal NUMERIC(15,2),
    moeda CHAR(3) DEFAULT 'BRL',
    
    -- Status
    status_contrato VARCHAR(20) CHECK (status_contrato IN ('ATIVO', 'INATIVO', 'SUSPENSO', 'CANCELADO')),
    tipo_contrato VARCHAR(50),
    modalidade_pagamento VARCHAR(50),
    observacoes TEXT,
    
    -- Campos calculados
    vigencia_meses INTEGER GENERATED ALWAYS AS (
        CASE 
            WHEN data_fim IS NOT NULL THEN 
                EXTRACT(YEAR FROM AGE(data_fim, data_inicio)) * 12 + 
                EXTRACT(MONTH FROM AGE(data_fim, data_inicio))
            ELSE NULL 
        END
    ) STORED,
    
    -- Auditoria
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario_atualizacao VARCHAR(100) DEFAULT CURRENT_USER,
    
    -- Constraints
    CONSTRAINT chk_contratos_datas CHECK (data_fim IS NULL OR data_fim >= data_inicio),
    CONSTRAINT chk_contratos_valores CHECK (valor_total IS NULL OR valor_total >= 0)
);

CREATE INDEX idx_contratos_cliente ON credits.contratos(cliente_pk);
CREATE INDEX idx_contratos_status ON credits.contratos(status_contrato);
CREATE INDEX idx_contratos_data_inicio ON credits.contratos(data_inicio DESC);
CREATE INDEX idx_contratos_data_fim ON credits.contratos(data_fim) WHERE data_fim IS NOT NULL;

COMMENT ON TABLE credits.contratos IS 'Silver - Contratos com datas e valores validados';

-- ============================================================================
-- TABELA: credits.produtos
-- Origem: bronze.onedrive_produtos
-- Descrição: Catálogo de produtos com categorização
-- ============================================================================

CREATE TABLE IF NOT EXISTS credits.produtos (
    -- Chave primária
    produto_pk SERIAL PRIMARY KEY,
    
    -- Identificadores
    produto_id VARCHAR(50) UNIQUE NOT NULL,
    codigo_produto VARCHAR(50) UNIQUE,
    
    -- Informações do produto
    nome_produto VARCHAR(255) NOT NULL,
    descricao TEXT,
    categoria VARCHAR(100),
    subcategoria VARCHAR(100),
    
    -- Preço e medida
    preco_base NUMERIC(15,2),
    unidade_medida VARCHAR(20),
    
    -- Status
    status_produto VARCHAR(20) CHECK (status_produto IN ('ATIVO', 'INATIVO', 'DESCONTINUADO')),
    
    -- Auditoria
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario_atualizacao VARCHAR(100) DEFAULT CURRENT_USER
);

CREATE INDEX idx_produtos_categoria ON credits.produtos(categoria);
CREATE INDEX idx_produtos_status ON credits.produtos(status_produto);
CREATE INDEX idx_produtos_nome ON credits.produtos USING GIN (to_tsvector('portuguese', nome_produto));

COMMENT ON TABLE credits.produtos IS 'Silver - Catálogo de produtos com categorização padronizada';

-- ============================================================================
-- TABELA: credits.precificacao
-- Origem: bronze.onedrive_precificacao
-- Descrição: Preços negociados por cliente
-- ============================================================================

CREATE TABLE IF NOT EXISTS credits.precificacao (
    -- Chave primária
    precificacao_pk SERIAL PRIMARY KEY,
    
    -- Identificadores
    precificacao_id VARCHAR(50) UNIQUE NOT NULL,
    
    -- FKs
    cliente_pk INTEGER NOT NULL REFERENCES credits.clientes(cliente_pk) ON DELETE CASCADE,
    produto_pk INTEGER NOT NULL REFERENCES credits.produtos(produto_pk) ON DELETE CASCADE,
    
    -- Preço negociado
    preco_negociado NUMERIC(15,2) NOT NULL,
    desconto_percentual NUMERIC(5,2),
    
    -- Vigência
    data_inicio_vigencia DATE NOT NULL,
    data_fim_vigencia DATE,
    vigencia_ativa BOOLEAN GENERATED ALWAYS AS (
        data_fim_vigencia IS NULL OR data_fim_vigencia >= CURRENT_DATE
    ) STORED,
    
    observacoes TEXT,
    
    -- Auditoria
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_precif_vigencia CHECK (data_fim_vigencia IS NULL OR data_fim_vigencia >= data_inicio_vigencia),
    CONSTRAINT chk_precif_desconto CHECK (desconto_percentual IS NULL OR (desconto_percentual >= 0 AND desconto_percentual <= 100))
);

CREATE INDEX idx_precificacao_cliente ON credits.precificacao(cliente_pk);
CREATE INDEX idx_precificacao_produto ON credits.precificacao(produto_pk);
CREATE INDEX idx_precificacao_vigencia ON credits.precificacao(data_inicio_vigencia, data_fim_vigencia);
CREATE INDEX idx_precificacao_ativa ON credits.precificacao(vigencia_ativa) WHERE vigencia_ativa = TRUE;

COMMENT ON TABLE credits.precificacao IS 'Silver - Preços negociados por cliente e produto';

-- ============================================================================
-- TABELA: credits.notas_fiscais
-- Origem: bronze.omie_notas_fiscais
-- Descrição: Notas fiscais do Omie com valores consolidados
-- ============================================================================

CREATE TABLE IF NOT EXISTS credits.notas_fiscais (
    -- Chave primária
    nota_fiscal_pk SERIAL PRIMARY KEY,
    
    -- Identificadores
    nf_id VARCHAR(50) UNIQUE NOT NULL,
    numero_nf VARCHAR(50),
    serie_nf VARCHAR(10),
    chave_acesso VARCHAR(100) UNIQUE,
    
    -- FK para clientes
    cliente_pk INTEGER REFERENCES credits.clientes(cliente_pk),
    
    -- Datas
    data_emissao DATE NOT NULL,
    data_vencimento DATE,
    
    -- Valores
    valor_total NUMERIC(15,2) NOT NULL,
    valor_produtos NUMERIC(15,2),
    valor_servicos NUMERIC(15,2),
    valor_desconto NUMERIC(15,2),
    valor_impostos NUMERIC(15,2),
    
    -- Status e chave
    status_nota VARCHAR(20) CHECK (status_nota IN ('AUTORIZADA', 'CANCELADA', 'DENEGADA', 'PENDENTE')),
    tipo_operacao VARCHAR(50),
    
    -- Auditoria
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_nf_valores CHECK (valor_total >= 0),
    CONSTRAINT chk_nf_datas CHECK (data_vencimento IS NULL OR data_vencimento >= data_emissao)
);

CREATE INDEX idx_nf_cliente ON credits.notas_fiscais(cliente_pk);
CREATE INDEX idx_nf_data_emissao ON credits.notas_fiscais(data_emissao DESC);
CREATE INDEX idx_nf_status ON credits.notas_fiscais(status_nota);
CREATE INDEX idx_nf_valor ON credits.notas_fiscais(valor_total);

COMMENT ON TABLE credits.notas_fiscais IS 'Silver - Notas fiscais do Omie validadas';

-- ============================================================================
-- TABELA: credits.contas_receber
-- Origem: bronze.omie_contas_receber
-- Descrição: Contas a receber
-- ============================================================================

CREATE TABLE IF NOT EXISTS credits.contas_receber (
    -- Chave primária
    conta_receber_pk SERIAL PRIMARY KEY,
    
    -- Identificadores
    conta_receber_id VARCHAR(50) UNIQUE NOT NULL,
    numero_documento VARCHAR(50),
    
    -- FKs
    cliente_pk INTEGER REFERENCES credits.clientes(cliente_pk),
    nota_fiscal_pk INTEGER REFERENCES credits.notas_fiscais(nota_fiscal_pk),
    
    -- Datas
    data_emissao DATE NOT NULL,
    data_vencimento DATE NOT NULL,
    data_recebimento DATE,
    
    -- Valores
    valor_documento NUMERIC(15,2) NOT NULL,
    valor_recebido NUMERIC(15,2),
    valor_juros NUMERIC(15,2),
    valor_desconto NUMERIC(15,2),
    
    -- Status
    status_titulo VARCHAR(20) CHECK (status_titulo IN ('ABERTO', 'PAGO', 'VENCIDO', 'CANCELADO')),
    forma_pagamento VARCHAR(50),
    
    -- Campos calculados
    dias_atraso INTEGER GENERATED ALWAYS AS (
        CASE 
            WHEN status_titulo = 'ABERTO' AND data_vencimento < CURRENT_DATE THEN
                CURRENT_DATE - data_vencimento
            WHEN status_titulo = 'PAGO' AND data_recebimento > data_vencimento THEN
                data_recebimento - data_vencimento
            ELSE 0
        END
    ) STORED,
    
    -- Auditoria
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cr_cliente ON credits.contas_receber(cliente_pk);
CREATE INDEX idx_cr_vencimento ON credits.contas_receber(data_vencimento);
CREATE INDEX idx_cr_status ON credits.contas_receber(status_titulo);
CREATE INDEX idx_cr_atraso ON credits.contas_receber(dias_atraso) WHERE dias_atraso > 0;

COMMENT ON TABLE credits.contas_receber IS 'Silver - Contas a receber do Omie';

-- ============================================================================
-- TABELA: credits.contas_pagar
-- Origem: bronze.omie_contas_pagar
-- Descrição: Contas a pagar (despesas)
-- ============================================================================

CREATE TABLE IF NOT EXISTS credits.contas_pagar (
    -- Chave primária
    conta_pagar_pk SERIAL PRIMARY KEY,
    
    -- Identificadores
    conta_pagar_id VARCHAR(50) UNIQUE NOT NULL,
    numero_documento VARCHAR(50),
    
    -- Fornecedor
    fornecedor_id VARCHAR(50),
    fornecedor_nome VARCHAR(255),
    
    -- Datas
    data_emissao DATE NOT NULL,
    data_vencimento DATE NOT NULL,
    data_pagamento DATE,
    
    -- Valores
    valor_documento NUMERIC(15,2) NOT NULL,
    valor_pago NUMERIC(15,2),
    valor_juros NUMERIC(15,2),
    valor_desconto NUMERIC(15,2),
    
    -- Classificação
    status_titulo VARCHAR(20) CHECK (status_titulo IN ('ABERTO', 'PAGO', 'VENCIDO', 'CANCELADO')),
    categoria_despesa VARCHAR(100),
    
    -- Auditoria
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cp_vencimento ON credits.contas_pagar(data_vencimento);
CREATE INDEX idx_cp_status ON credits.contas_pagar(status_titulo);
CREATE INDEX idx_cp_categoria ON credits.contas_pagar(categoria_despesa);

COMMENT ON TABLE credits.contas_pagar IS 'Silver - Contas a pagar do Omie';

-- ============================================================================
-- TABELA: credits.tickets
-- Origem: bronze.movidesk_tickets
-- Descrição: Tickets de atendimento
-- ============================================================================

CREATE TABLE IF NOT EXISTS credits.tickets (
    -- Chave primária
    ticket_pk SERIAL PRIMARY KEY,
    
    -- Identificadores
    ticket_id VARCHAR(50) UNIQUE NOT NULL,
    numero_ticket VARCHAR(50),
    
    -- FK cliente
    cliente_pk INTEGER REFERENCES credits.clientes(cliente_pk),
    
    -- Dados do ticket
    assunto TEXT NOT NULL,
    categoria VARCHAR(100),
    urgencia VARCHAR(20) CHECK (urgencia IN ('BAIXA', 'NORMAL', 'ALTA', 'CRITICA')),
    prioridade VARCHAR(20) CHECK (prioridade IN ('BAIXA', 'NORMAL', 'ALTA', 'CRITICA')),
    status_ticket VARCHAR(20) CHECK (status_ticket IN ('NOVO', 'EM_ATENDIMENTO', 'AGUARDANDO', 'FECHADO', 'CANCELADO')),
    
    -- Responsável
    agente_responsavel VARCHAR(100),
    
    -- Datas
    data_criacao TIMESTAMP NOT NULL,
    data_atualizacao TIMESTAMP,
    data_fechamento TIMESTAMP,
    
    -- SLA e tempo de resolução
    sla_primeira_resposta INTERVAL,
    sla_resolucao INTERVAL,
    tempo_resolucao_horas NUMERIC(10,2),
    
    -- Satisfação
    satisfacao_cliente NUMERIC(2,1),  -- 1.0 a 5.0
    
    -- Auditoria
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_ticket_satisfacao CHECK (satisfacao_cliente IS NULL OR (satisfacao_cliente >= 1.0 AND satisfacao_cliente <= 5.0))
);

CREATE INDEX idx_tickets_cliente ON credits.tickets(cliente_pk);
CREATE INDEX idx_tickets_status ON credits.tickets(status_ticket);
CREATE INDEX idx_tickets_agente ON credits.tickets(agente_responsavel);
CREATE INDEX idx_tickets_data_criacao ON credits.tickets(data_criacao DESC);
CREATE INDEX idx_tickets_categoria ON credits.tickets(categoria);

COMMENT ON TABLE credits.tickets IS 'Silver - Tickets do Movidesk validados';

-- ============================================================================
-- TABELA CONSOLIDADA: credits.consumo_parceiros
-- Origem: bronze.finqi_consumo + bronze.salesbox_consumo + bronze.acertpix_transacoes
-- Descrição: União de consumo de TODOS os parceiros
-- ============================================================================

CREATE TABLE IF NOT EXISTS credits.consumo_parceiros (
    -- Chave primária
    consumo_pk SERIAL PRIMARY KEY,
    
    -- Identificadores
    consumo_id VARCHAR(50) UNIQUE NOT NULL,
    
    -- FK para clientes
    cliente_pk INTEGER NOT NULL REFERENCES credits.clientes(cliente_pk) ON DELETE CASCADE,
    
    -- Dados do consumo
    parceiro VARCHAR(50) NOT NULL CHECK (parceiro IN ('FINQI', 'SALESBOX', 'ACERTPIX', 'OUTROS')),
    produto_servico VARCHAR(255) NOT NULL,
    data_consumo DATE NOT NULL,
    
    -- Quantidades e valores
    quantidade NUMERIC(15,4),
    valor_unitario NUMERIC(15,2),
    valor_total NUMERIC(15,2) NOT NULL,
    
    -- Tipo de transação
    tipo_transacao VARCHAR(100),
    
    -- Auditoria
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_consumo_valores CHECK (valor_total >= 0)
);

CREATE INDEX idx_consumo_cliente ON credits.consumo_parceiros(cliente_pk);
CREATE INDEX idx_consumo_data ON credits.consumo_parceiros(data_consumo DESC);
CREATE INDEX idx_consumo_parceiro ON credits.consumo_parceiros(parceiro);
CREATE INDEX idx_consumo_produto ON credits.consumo_parceiros(produto_servico);
CREATE INDEX idx_consumo_mes ON credits.consumo_parceiros(DATE_TRUNC('month', data_consumo));

COMMENT ON TABLE credits.consumo_parceiros IS 'Silver - Consumo consolidado de todos os parceiros';

-- ============================================================================
-- TABELA: credits.consultas_credito
-- Origem: bronze.spc_consultas
-- Descrição: Consultas de crédito SPC Brasil
-- ============================================================================

CREATE TABLE IF NOT EXISTS credits.consultas_credito (
    -- Chave primária
    consulta_pk SERIAL PRIMARY KEY,
    
    -- Identificadores
    consulta_id VARCHAR(50) UNIQUE NOT NULL,
    
    -- FK cliente (opcional, pode ser consulta avulsa)
    cliente_pk INTEGER REFERENCES credits.clientes(cliente_pk),
    
    -- CPF/CNPJ consultado
    cpf_cnpj_consultado VARCHAR(20) NOT NULL,
    
    -- Data da consulta
    data_consulta TIMESTAMP NOT NULL,
    
    -- Resultado
    score_credito INTEGER,
    situacao_credito VARCHAR(50),
    possui_restricoes BOOLEAN DEFAULT FALSE,
    restricoes TEXT,
    
    -- Auditoria
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_score CHECK (score_credito IS NULL OR (score_credito >= 0 AND score_credito <= 1000))
);

CREATE INDEX idx_consultas_cliente ON credits.consultas_credito(cliente_pk);
CREATE INDEX idx_consultas_cpf_cnpj ON credits.consultas_credito(cpf_cnpj_consultado);
CREATE INDEX idx_consultas_data ON credits.consultas_credito(data_consulta DESC);
CREATE INDEX idx_consultas_score ON credits.consultas_credito(score_credito);

COMMENT ON TABLE credits.consultas_credito IS 'Silver - Consultas de crédito SPC Brasil';

-- ============================================================================
-- TRIGGER: Atualizar data_atualizacao automaticamente
-- ============================================================================

CREATE OR REPLACE FUNCTION credits.atualizar_timestamp_atualizacao()
RETURNS TRIGGER AS $$
BEGIN
    NEW.data_atualizacao = CURRENT_TIMESTAMP;
    NEW.usuario_atualizacao = CURRENT_USER;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Aplicar trigger em todas as tabelas relevantes
CREATE TRIGGER trg_clientes_atualizacao
    BEFORE UPDATE ON credits.clientes
    FOR EACH ROW EXECUTE FUNCTION credits.atualizar_timestamp_atualizacao();

CREATE TRIGGER trg_contratos_atualizacao
    BEFORE UPDATE ON credits.contratos
    FOR EACH ROW EXECUTE FUNCTION credits.atualizar_timestamp_atualizacao();

CREATE TRIGGER trg_produtos_atualizacao
    BEFORE UPDATE ON credits.produtos
    FOR EACH ROW EXECUTE FUNCTION credits.atualizar_timestamp_atualizacao();

CREATE TRIGGER trg_notas_fiscais_atualizacao
    BEFORE UPDATE ON credits.notas_fiscais
    FOR EACH ROW EXECUTE FUNCTION credits.atualizar_timestamp_atualizacao();

-- ============================================================================
-- MENSAGEM DE CONCLUSÃO
-- ============================================================================

DO $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM information_schema.tables
    WHERE table_schema = 'credits' AND table_type = 'BASE TABLE';
    
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Camada SILVER criada com sucesso!';
    RAISE NOTICE '';
    RAISE NOTICE 'Total de tabelas criadas: %', v_count - 1; -- Menos a tabela de auditoria
    RAISE NOTICE '';
    RAISE NOTICE 'Tabelas principais:';
    RAISE NOTICE '  ✓ credits.clientes (tabela central com merge OneDrive + Ploomes)';
    RAISE NOTICE '  ✓ credits.contratos';
    RAISE NOTICE '  ✓ credits.produtos';
    RAISE NOTICE '  ✓ credits.precificacao';
    RAISE NOTICE '  ✓ credits.notas_fiscais';
    RAISE NOTICE '  ✓ credits.contas_receber';
    RAISE NOTICE '  ✓ credits.contas_pagar';
    RAISE NOTICE '  ✓ credits.tickets';
    RAISE NOTICE '  ✓ credits.consumo_parceiros (consolidado)';
    RAISE NOTICE '  ✓ credits.consultas_credito';
    RAISE NOTICE '';
    RAISE NOTICE 'Recursos implementados:';
    RAISE NOTICE '  ✓ Chaves primárias e estrangeiras';
    RAISE NOTICE '  ✓ Índices para performance';
    RAISE NOTICE '  ✓ Constraints de validação';
    RAISE NOTICE '  ✓ Campos calculados (Generated Columns)';
    RAISE NOTICE '  ✓ Triggers de auditoria';
    RAISE NOTICE '============================================================================';
END $$;
