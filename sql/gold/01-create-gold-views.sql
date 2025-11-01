-- ============================================================================
-- Script: 01-create-gold-views.sql
-- Descrição: Criação de TODAS as views da camada GOLD (credits.vw_*)
-- Camada: Gold (Analytics Layer)
-- Versão: 1.0
-- Data: Outubro 2025
-- ============================================================================
--
-- PRINCÍPIOS DA CAMADA GOLD:
-- 1. Views prontas para consumo em BI
-- 2. Agregações pré-calculadas (SUM, AVG, COUNT)
-- 3. Métricas de negócio (KPIs, faturamento, pipeline)
-- 4. Joins complexos simplificados
-- 5. Performance otimizada com índices nas tabelas base
--
-- ============================================================================

-- ============================================================================
-- GESTÃO DE CARTEIRA - FATURAMENTO
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: credits.vw_faturamento_mensal
-- Descrição: Consolidação de faturamento por mês, cliente e segmento
-- Uso: Dashboard de faturamento, análise de receita
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_faturamento_mensal AS
SELECT
    DATE_TRUNC('month', nf.data_emissao)::DATE AS mes,
    EXTRACT(YEAR FROM nf.data_emissao)::INTEGER AS ano,
    EXTRACT(MONTH FROM nf.data_emissao)::INTEGER AS mes_numero,
    TO_CHAR(nf.data_emissao, 'TMMonth/YYYY') AS mes_ano_nome,
    
    -- Dados do cliente
    c.cliente_pk,
    c.razao_social,
    c.nome_fantasia,
    c.segmento,
    c.porte_empresa,
    c.consultor_responsavel,
    
    -- Métricas de faturamento
    COUNT(DISTINCT nf.nota_fiscal_pk) AS qtd_notas,
    SUM(nf.valor_total) AS faturamento_total,
    SUM(nf.valor_produtos) AS faturamento_produtos,
    SUM(nf.valor_servicos) AS faturamento_servicos,
    SUM(nf.valor_desconto) AS total_descontos,
    SUM(nf.valor_impostos) AS total_impostos,
    AVG(nf.valor_total) AS ticket_medio,
    MIN(nf.valor_total) AS menor_nota,
    MAX(nf.valor_total) AS maior_nota
    
FROM credits.notas_fiscais nf
INNER JOIN credits.clientes c ON nf.cliente_pk = c.cliente_pk
WHERE nf.status_nota = 'AUTORIZADA'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY 1 DESC, 11 DESC;

COMMENT ON VIEW credits.vw_faturamento_mensal IS 'Gold - Faturamento consolidado mensal por cliente';

-- ----------------------------------------------------------------------------
-- View: credits.vw_faturamento_anual
-- Descrição: Faturamento anual com comparativo ano anterior
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_faturamento_anual AS
WITH faturamento_ano AS (
    SELECT
        EXTRACT(YEAR FROM data_emissao)::INTEGER AS ano,
        cliente_pk,
        SUM(valor_total) AS faturamento_ano
    FROM credits.notas_fiscais
    WHERE status_nota = 'AUTORIZADA'
    GROUP BY 1, 2
)
SELECT
    fa.ano,
    c.razao_social,
    c.segmento,
    c.consultor_responsavel,
    fa.faturamento_ano AS faturamento_ano_atual,
    LAG(fa.faturamento_ano) OVER (PARTITION BY fa.cliente_pk ORDER BY fa.ano) AS faturamento_ano_anterior,
    fa.faturamento_ano - LAG(fa.faturamento_ano) OVER (PARTITION BY fa.cliente_pk ORDER BY fa.ano) AS variacao_absoluta,
    CASE 
        WHEN LAG(fa.faturamento_ano) OVER (PARTITION BY fa.cliente_pk ORDER BY fa.ano) > 0 THEN
            ROUND(((fa.faturamento_ano / LAG(fa.faturamento_ano) OVER (PARTITION BY fa.cliente_pk ORDER BY fa.ano)) - 1) * 100, 2)
        ELSE NULL
    END AS variacao_percentual
FROM faturamento_ano fa
INNER JOIN credits.clientes c ON fa.cliente_pk = c.cliente_pk
ORDER BY fa.ano DESC, fa.faturamento_ano DESC;

COMMENT ON VIEW credits.vw_faturamento_anual IS 'Gold - Faturamento anual com comparativo';

-- ============================================================================
-- GESTÃO DE CARTEIRA - CONSUMO
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: credits.vw_consumo_mensal_parceiros
-- Descrição: Consumo consolidado por parceiro, mês e cliente
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_consumo_mensal_parceiros AS
SELECT
    DATE_TRUNC('month', cp.data_consumo)::DATE AS mes,
    EXTRACT(YEAR FROM cp.data_consumo)::INTEGER AS ano,
    EXTRACT(MONTH FROM cp.data_consumo)::INTEGER AS mes_numero,
    
    -- Dados do cliente
    c.cliente_pk,
    c.razao_social,
    c.segmento,
    c.consultor_responsavel,
    
    -- Parceiro e produto
    cp.parceiro,
    cp.produto_servico,
    
    -- Métricas de consumo
    SUM(cp.quantidade) AS quantidade_total,
    SUM(cp.valor_total) AS valor_consumido,
    AVG(cp.valor_unitario) AS valor_unitario_medio,
    COUNT(DISTINCT cp.consumo_pk) AS qtd_transacoes
    
FROM credits.consumo_parceiros cp
INNER JOIN credits.clientes c ON cp.cliente_pk = c.cliente_pk
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY 1 DESC, 11 DESC;

COMMENT ON VIEW credits.vw_consumo_mensal_parceiros IS 'Gold - Consumo mensal por parceiro e cliente';

-- ----------------------------------------------------------------------------
-- View: credits.vw_consumo_6_meses
-- Descrição: Histórico dos últimos 6 meses de consumo
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_consumo_6_meses AS
SELECT
    DATE_TRUNC('month', cp.data_consumo)::DATE AS mes,
    c.razao_social,
    c.segmento,
    cp.parceiro,
    cp.produto_servico,
    SUM(cp.valor_total) AS valor_consumido,
    SUM(cp.quantidade) AS quantidade_total
FROM credits.consumo_parceiros cp
INNER JOIN credits.clientes c ON cp.cliente_pk = c.cliente_pk
WHERE cp.data_consumo >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC, 6 DESC;

COMMENT ON VIEW credits.vw_consumo_6_meses IS 'Gold - Consumo dos últimos 6 meses';

-- ----------------------------------------------------------------------------
-- View: credits.vw_consumo_semanal
-- Descrição: Análise semanal de consumo
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_consumo_semanal AS
SELECT
    DATE_TRUNC('week', cp.data_consumo)::DATE AS semana,
    EXTRACT(YEAR FROM cp.data_consumo)::INTEGER AS ano,
    EXTRACT(WEEK FROM cp.data_consumo)::INTEGER AS numero_semana,
    c.razao_social,
    cp.parceiro,
    SUM(cp.valor_total) AS valor_consumido,
    COUNT(DISTINCT cp.consumo_pk) AS qtd_transacoes
FROM credits.consumo_parceiros cp
INNER JOIN credits.clientes c ON cp.cliente_pk = c.cliente_pk
WHERE cp.data_consumo >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC, 6 DESC;

COMMENT ON VIEW credits.vw_consumo_semanal IS 'Gold - Consumo semanal dos últimos 3 meses';

-- ============================================================================
-- GESTÃO DE VENDAS - PIPELINE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: credits.vw_pipeline_anual
-- Descrição: Funil de vendas consolidado anual do Ploomes
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_pipeline_anual AS
WITH deals_recentes AS (
    SELECT 
        deal_id,
        deal_title,
        deal_stage,
        deal_status,
        owner_name AS vendedor,
        organization_name AS cliente,
        CAST(deal_amount AS NUMERIC(15,2)) AS valor_deal,
        CAST(win_probability AS NUMERIC(5,2)) AS probabilidade,
        TO_DATE(created_date, 'YYYY-MM-DD') AS data_criacao,
        TO_DATE(expected_close_date, 'YYYY-MM-DD') AS data_fechamento_prevista,
        EXTRACT(YEAR FROM TO_DATE(created_date, 'YYYY-MM-DD')) AS ano
    FROM bronze.ploomes_deals
    WHERE deal_status != 'CANCELLED'
      AND created_date IS NOT NULL
)
SELECT
    ano,
    vendedor,
    deal_stage AS estagio,
    deal_status AS status,
    COUNT(*) AS qtd_deals,
    SUM(valor_deal) AS valor_total,
    AVG(valor_deal) AS valor_medio_deal,
    AVG(probabilidade) AS prob_media_ganho,
    SUM(CASE WHEN deal_status = 'WON' THEN valor_deal ELSE 0 END) AS valor_ganho,
    SUM(CASE WHEN deal_status = 'LOST' THEN valor_deal ELSE 0 END) AS valor_perdido
FROM deals_recentes
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 5 DESC;

COMMENT ON VIEW credits.vw_pipeline_anual IS 'Gold - Pipeline de vendas anual do Ploomes';

-- ----------------------------------------------------------------------------
-- View: credits.vw_pipeline_3_meses
-- Descrição: Pipeline trimestral com taxa de conversão
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_pipeline_3_meses AS
WITH deals_trimestre AS (
    SELECT 
        deal_id,
        deal_stage,
        deal_status,
        owner_name AS vendedor,
        CAST(deal_amount AS NUMERIC(15,2)) AS valor_deal,
        TO_DATE(created_date, 'YYYY-MM-DD') AS data_criacao
    FROM bronze.ploomes_deals
    WHERE TO_DATE(created_date, 'YYYY-MM-DD') >= CURRENT_DATE - INTERVAL '3 months'
      AND deal_status != 'CANCELLED'
)
SELECT
    vendedor,
    deal_stage AS estagio,
    COUNT(*) AS qtd_deals,
    SUM(valor_deal) AS valor_total_pipeline,
    SUM(CASE WHEN deal_status = 'WON' THEN 1 ELSE 0 END) AS deals_ganhos,
    SUM(CASE WHEN deal_status = 'LOST' THEN 1 ELSE 0 END) AS deals_perdidos,
    ROUND(
        (SUM(CASE WHEN deal_status = 'WON' THEN 1 ELSE 0 END)::NUMERIC / 
        NULLIF(SUM(CASE WHEN deal_status IN ('WON', 'LOST') THEN 1 ELSE 0 END), 0)) * 100, 
        2
    ) AS taxa_conversao_percentual
FROM deals_trimestre
GROUP BY 1, 2
ORDER BY 4 DESC;

COMMENT ON VIEW credits.vw_pipeline_3_meses IS 'Gold - Pipeline trimestral com taxa de conversão';

-- ============================================================================
-- GESTÃO FINANCEIRA - CONTAS A RECEBER
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: credits.vw_contas_receber_status
-- Descrição: Status de recebíveis por cliente e vencimento
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_contas_receber_status AS
SELECT
    c.razao_social,
    c.segmento,
    cr.status_titulo,
    COUNT(*) AS qtd_titulos,
    SUM(cr.valor_documento) AS valor_total,
    SUM(cr.valor_recebido) AS valor_recebido,
    SUM(cr.valor_documento) - COALESCE(SUM(cr.valor_recebido), 0) AS saldo_aberto,
    SUM(CASE WHEN cr.dias_atraso > 0 THEN 1 ELSE 0 END) AS titulos_em_atraso,
    AVG(cr.dias_atraso) AS media_dias_atraso
FROM credits.contas_receber cr
INNER JOIN credits.clientes c ON cr.cliente_pk = c.cliente_pk
GROUP BY 1, 2, 3
ORDER BY 5 DESC;

COMMENT ON VIEW credits.vw_contas_receber_status IS 'Gold - Status de contas a receber';

-- ----------------------------------------------------------------------------
-- View: credits.vw_inadimplencia
-- Descrição: Análise de inadimplência por cliente
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_inadimplencia AS
SELECT
    c.cliente_pk,
    c.razao_social,
    c.cnpj_cpf,
    c.segmento,
    COUNT(*) AS qtd_titulos_vencidos,
    SUM(cr.valor_documento) AS valor_total_vencido,
    MAX(cr.dias_atraso) AS maior_atraso_dias,
    AVG(cr.dias_atraso) AS media_atraso_dias,
    MIN(cr.data_vencimento) AS vencimento_mais_antigo
FROM credits.contas_receber cr
INNER JOIN credits.clientes c ON cr.cliente_pk = c.cliente_pk
WHERE cr.status_titulo IN ('VENCIDO', 'ABERTO') 
  AND cr.dias_atraso > 0
GROUP BY 1, 2, 3, 4
ORDER BY 6 DESC, 7 DESC;

COMMENT ON VIEW credits.vw_inadimplencia IS 'Gold - Análise de inadimplência';

-- ============================================================================
-- GESTÃO DE ATENDIMENTO - TICKETS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: credits.vw_performance_atendimento
-- Descrição: Métricas de performance de atendimento
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_performance_atendimento AS
SELECT
    DATE_TRUNC('month', t.data_criacao)::DATE AS mes,
    t.agente_responsavel,
    t.categoria,
    
    -- Quantidade
    COUNT(*) AS total_tickets,
    COUNT(*) FILTER (WHERE t.status_ticket = 'FECHADO') AS tickets_fechados,
    COUNT(*) FILTER (WHERE t.status_ticket IN ('NOVO', 'EM_ATENDIMENTO')) AS tickets_abertos,
    
    -- Tempo de resolução
    AVG(t.tempo_resolucao_horas) FILTER (WHERE t.status_ticket = 'FECHADO') AS tempo_medio_resolucao_horas,
    MIN(t.tempo_resolucao_horas) FILTER (WHERE t.status_ticket = 'FECHADO') AS tempo_minimo_resolucao,
    MAX(t.tempo_resolucao_horas) FILTER (WHERE t.status_ticket = 'FECHADO') AS tempo_maximo_resolucao,
    
    -- Satisfação
    AVG(t.satisfacao_cliente) AS nota_satisfacao_media,
    COUNT(*) FILTER (WHERE t.satisfacao_cliente >= 4.0) AS tickets_satisfeitos,
    COUNT(*) FILTER (WHERE t.satisfacao_cliente < 3.0) AS tickets_insatisfeitos,
    
    -- Taxa de resolução
    ROUND(
        (COUNT(*) FILTER (WHERE t.status_ticket = 'FECHADO')::NUMERIC / COUNT(*)) * 100,
        2
    ) AS taxa_resolucao_percentual
    
FROM credits.tickets t
WHERE t.data_criacao >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;

COMMENT ON VIEW credits.vw_performance_atendimento IS 'Gold - Performance de atendimento mensal';

-- ----------------------------------------------------------------------------
-- View: credits.vw_tickets_por_cliente
-- Descrição: Volume de tickets por cliente
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_tickets_por_cliente AS
SELECT
    c.cliente_pk,
    c.razao_social,
    c.segmento,
    COUNT(*) AS total_tickets,
    COUNT(*) FILTER (WHERE t.status_ticket = 'FECHADO') AS tickets_fechados,
    COUNT(*) FILTER (WHERE t.status_ticket IN ('NOVO', 'EM_ATENDIMENTO')) AS tickets_abertos,
    AVG(t.satisfacao_cliente) AS satisfacao_media,
    MAX(t.data_criacao) AS ultimo_ticket
FROM credits.tickets t
INNER JOIN credits.clientes c ON t.cliente_pk = c.cliente_pk
GROUP BY 1, 2, 3
ORDER BY 4 DESC;

COMMENT ON VIEW credits.vw_tickets_por_cliente IS 'Gold - Volume de tickets por cliente';

-- ============================================================================
-- GESTÃO DE RESULTADOS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: credits.vw_metas_consultores
-- Descrição: Acompanhamento de metas por consultor (exemplo)
-- Nota: Esta view precisa ser ajustada quando houver tabela de metas
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_metas_consultores AS
WITH faturamento_consultor AS (
    SELECT
        c.consultor_responsavel,
        DATE_TRUNC('month', nf.data_emissao)::DATE AS mes,
        SUM(nf.valor_total) AS faturamento_realizado,
        COUNT(DISTINCT c.cliente_pk) AS clientes_ativos
    FROM credits.notas_fiscais nf
    INNER JOIN credits.clientes c ON nf.cliente_pk = c.cliente_pk
    WHERE nf.status_nota = 'AUTORIZADA'
      AND c.consultor_responsavel IS NOT NULL
      AND nf.data_emissao >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY 1, 2
)
SELECT
    mes,
    consultor_responsavel,
    faturamento_realizado,
    clientes_ativos,
    -- Meta fixa de exemplo (deve vir de tabela de metas)
    500000.00 AS meta_mensal,
    ROUND((faturamento_realizado / 500000.00) * 100, 2) AS percentual_meta
FROM faturamento_consultor
ORDER BY mes DESC, faturamento_realizado DESC;

COMMENT ON VIEW credits.vw_metas_consultores IS 'Gold - Acompanhamento de metas por consultor';

-- ----------------------------------------------------------------------------
-- View: credits.vw_baseline_comissoes
-- Descrição: Cálculo de baseline e comissões (exemplo)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_baseline_comissoes AS
WITH faturamento_mensal AS (
    SELECT
        c.consultor_responsavel,
        DATE_TRUNC('month', nf.data_emissao)::DATE AS mes,
        SUM(nf.valor_total) AS faturamento
    FROM credits.notas_fiscais nf
    INNER JOIN credits.clientes c ON nf.cliente_pk = c.cliente_pk
    WHERE nf.status_nota = 'AUTORIZADA'
      AND c.consultor_responsavel IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    mes,
    consultor_responsavel,
    faturamento,
    -- Comissão de 3% sobre o faturamento (exemplo)
    ROUND(faturamento * 0.03, 2) AS comissao_calculada,
    -- Baseline de 200k (exemplo)
    200000.00 AS baseline,
    CASE 
        WHEN faturamento >= 200000.00 THEN 'ATINGIU'
        ELSE 'NAO_ATINGIU'
    END AS status_baseline
FROM faturamento_mensal
ORDER BY mes DESC, faturamento DESC;

COMMENT ON VIEW credits.vw_baseline_comissoes IS 'Gold - Baseline e comissões dos consultores';

-- ============================================================================
-- DASHBOARDS EXECUTIVOS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: credits.vw_dashboard_executivo
-- Descrição: KPIs principais para dashboard executivo
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW credits.vw_dashboard_executivo AS
WITH mes_atual AS (
    SELECT DATE_TRUNC('month', CURRENT_DATE)::DATE AS mes
),
kpis_faturamento AS (
    SELECT
        COUNT(DISTINCT nf.cliente_pk) AS clientes_faturados,
        SUM(nf.valor_total) AS faturamento_mes,
        COUNT(nf.nota_fiscal_pk) AS qtd_notas
    FROM credits.notas_fiscais nf
    CROSS JOIN mes_atual ma
    WHERE DATE_TRUNC('month', nf.data_emissao) = ma.mes
      AND nf.status_nota = 'AUTORIZADA'
),
kpis_pipeline AS (
    SELECT
        COUNT(*) FILTER (WHERE deal_status = 'OPEN') AS deals_abertos,
        SUM(CAST(deal_amount AS NUMERIC)) FILTER (WHERE deal_status = 'OPEN') AS valor_pipeline
    FROM bronze.ploomes_deals
    WHERE TO_DATE(created_date, 'YYYY-MM-DD') >= CURRENT_DATE - INTERVAL '3 months'
),
kpis_tickets AS (
    SELECT
        COUNT(*) FILTER (WHERE status_ticket IN ('NOVO', 'EM_ATENDIMENTO')) AS tickets_abertos,
        AVG(satisfacao_cliente) AS satisfacao_media
    FROM credits.tickets
    WHERE data_criacao >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    (SELECT mes FROM mes_atual) AS mes_referencia,
    kf.clientes_faturados,
    kf.faturamento_mes,
    kf.qtd_notas,
    ROUND(kf.faturamento_mes / NULLIF(kf.qtd_notas, 0), 2) AS ticket_medio,
    kp.deals_abertos,
    kp.valor_pipeline,
    kt.tickets_abertos,
    ROUND(kt.satisfacao_media, 2) AS satisfacao_media
FROM kpis_faturamento kf
CROSS JOIN kpis_pipeline kp
CROSS JOIN kpis_tickets kt;

COMMENT ON VIEW credits.vw_dashboard_executivo IS 'Gold - KPIs principais para dashboard executivo';

-- ============================================================================
-- PERMISSÕES
-- ============================================================================

-- Conceder permissão de SELECT em todas as views para dw_viewer
DO $$
DECLARE
    view_name TEXT;
BEGIN
    FOR view_name IN 
        SELECT table_name 
        FROM information_schema.views 
        WHERE table_schema = 'credits' 
          AND table_name LIKE 'vw_%'
    LOOP
        EXECUTE format('GRANT SELECT ON credits.%I TO dw_viewer', view_name);
        EXECUTE format('GRANT SELECT ON credits.%I TO dw_analyst', view_name);
    END LOOP;
    
    RAISE NOTICE 'Permissões concedidas para todas as views Gold';
END $$;

-- ============================================================================
-- MENSAGEM DE CONCLUSÃO
-- ============================================================================

DO $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM information_schema.views
    WHERE table_schema = 'credits' AND table_name LIKE 'vw_%';
    
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Camada GOLD criada com sucesso!';
    RAISE NOTICE '';
    RAISE NOTICE 'Total de views criadas: %', v_count;
    RAISE NOTICE '';
    RAISE NOTICE 'Views de Faturamento:';
    RAISE NOTICE '  ✓ credits.vw_faturamento_mensal';
    RAISE NOTICE '  ✓ credits.vw_faturamento_anual';
    RAISE NOTICE '';
    RAISE NOTICE 'Views de Consumo:';
    RAISE NOTICE '  ✓ credits.vw_consumo_mensal_parceiros';
    RAISE NOTICE '  ✓ credits.vw_consumo_6_meses';
    RAISE NOTICE '  ✓ credits.vw_consumo_semanal';
    RAISE NOTICE '';
    RAISE NOTICE 'Views de Pipeline de Vendas:';
    RAISE NOTICE '  ✓ credits.vw_pipeline_anual';
    RAISE NOTICE '  ✓ credits.vw_pipeline_3_meses';
    RAISE NOTICE '';
    RAISE NOTICE 'Views Financeiras:';
    RAISE NOTICE '  ✓ credits.vw_contas_receber_status';
    RAISE NOTICE '  ✓ credits.vw_inadimplencia';
    RAISE NOTICE '';
    RAISE NOTICE 'Views de Atendimento:';
    RAISE NOTICE '  ✓ credits.vw_performance_atendimento';
    RAISE NOTICE '  ✓ credits.vw_tickets_por_cliente';
    RAISE NOTICE '';
    RAISE NOTICE 'Views de Gestão:';
    RAISE NOTICE '  ✓ credits.vw_metas_consultores';
    RAISE NOTICE '  ✓ credits.vw_baseline_comissoes';
    RAISE NOTICE '  ✓ credits.vw_dashboard_executivo';
    RAISE NOTICE '';
    RAISE NOTICE 'Pronto para integração com Power BI!';
    RAISE NOTICE '============================================================================';
END $$;
