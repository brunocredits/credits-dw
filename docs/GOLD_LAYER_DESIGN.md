# Gold Layer - Design e Planejamento

## 📋 Contexto do Negócio

### Necessidades Identificadas (2025-11-27)

**Relatórios Power BI Existentes:**
- Faturamento
- Consumo 6 meses / Consumo semanal
- Carteira (Gestão de vendas)
- Pipeline anual / Pipeline 3 meses
- Relatório de vendas
- Planos de contas
- Oportunidades
- Penetração portfolio
- Análise de contratos
- Contas e perguntas
- Metas liderança
- Baseline
- Comissões
- Tabelas e contratos
- Aumentos e quedas
- Detalhamento de Fatura
- Motivos de Perda
- Detalhamento Vendas
- Produtividade

**Características:**
- 20+ dashboards diferentes
- Filtro por consultor de vendas (usuário)
- Arquivos de entrada grandes (volume a confirmar)
- Necessidade de atualização frequente
- Múltiplas análises temporais (semanal, 3 meses, 6 meses, anual)

## 🎯 Decisão: Gold Layer é Recomendada

### Justificativas

1. **Múltiplos relatórios complexos** → Evita processamento duplicado
2. **Atualização frequente** → Snapshots + DirectQuery híbrido
3. **Filtro por consultor** → Tabelas pré-filtradas para performance
4. **Análises temporais** → Snapshots mensais para comparações históricas
5. **Métricas calculadas** → Metas, comissões, produtividade pré-calculadas

## 🏗️ Arquitetura Proposta

```
Power BI
  ├─ Gold (Import) → Snapshots, agregações, métricas
  ├─ Silver (DirectQuery) → Dados em tempo real
  └─ Silver (Import) → Dimensões estáveis
        ↓
    Silver Layer (Star Schema)
        ↓
    Bronze Layer (Raw Validated)
```

## 📊 Estrutura Gold Sugerida

### 1. gold.carteira_snapshot_diario
**Propósito:** Snapshot diário da carteira de cada consultor
**Atualização:** Diária (6h da manhã)
**Uso:** Dashboards de Gestão de Vendas, Carteira

```sql
CREATE TABLE gold.carteira_snapshot_diario (
    -- Chaves
    snapshot_id BIGSERIAL PRIMARY KEY,
    data_snapshot DATE NOT NULL,
    consultor_sk INTEGER NOT NULL,
    cliente_sk INTEGER NOT NULL,

    -- Métricas de Faturamento
    faturamento_total NUMERIC(15,2),
    faturamento_6_meses NUMERIC(15,2),
    faturamento_3_meses NUMERIC(15,2),
    faturamento_mes_atual NUMERIC(15,2),
    faturamento_semana_atual NUMERIC(15,2),

    -- Métricas de Consumo
    consumo_medio_mensal NUMERIC(15,2),
    tendencia_consumo VARCHAR(20), -- CRESCENDO, ESTAVEL, DECRESCENDO

    -- Pipeline
    valor_pipeline_cliente NUMERIC(15,2),
    num_oportunidades_abertas INTEGER,

    -- Status
    status_cliente VARCHAR(50),
    dias_sem_faturamento INTEGER,
    risco_churn VARCHAR(20), -- BAIXO, MEDIO, ALTO

    -- Metadata
    data_carga TIMESTAMP DEFAULT NOW(),

    -- Índices e Constraints
    CONSTRAINT uk_carteira_snapshot UNIQUE (data_snapshot, consultor_sk, cliente_sk)
);

CREATE INDEX idx_carteira_snapshot_data ON gold.carteira_snapshot_diario(data_snapshot);
CREATE INDEX idx_carteira_snapshot_consultor ON gold.carteira_snapshot_diario(consultor_sk);
```

### 2. gold.vendas_consolidado_semanal
**Propósito:** Métricas consolidadas de vendas por semana e consultor
**Atualização:** Semanal (segunda-feira 6h)
**Uso:** Relatório de Vendas, Produtividade

```sql
CREATE TABLE gold.vendas_consolidado_semanal (
    -- Chaves
    consolidado_id BIGSERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    semana_ano INTEGER NOT NULL,
    inicio_semana DATE NOT NULL,
    consultor_sk INTEGER NOT NULL,
    gestor_sk INTEGER,

    -- Métricas de Vendas
    num_vendas_fechadas INTEGER DEFAULT 0,
    valor_vendas_total NUMERIC(15,2) DEFAULT 0,
    ticket_medio NUMERIC(15,2),
    maior_venda NUMERIC(15,2),

    -- Pipeline
    valor_pipeline_aberto NUMERIC(15,2),
    num_oportunidades_abertas INTEGER,
    num_oportunidades_ganhas INTEGER,
    num_oportunidades_perdidas INTEGER,
    taxa_conversao NUMERIC(5,2), -- Percentual

    -- Produtividade
    num_reunioes INTEGER,
    num_propostas_enviadas INTEGER,
    tempo_medio_fechamento_dias INTEGER,

    -- Comparações
    variacao_vs_semana_anterior NUMERIC(5,2), -- Percentual
    variacao_vs_media_mensal NUMERIC(5,2),

    -- Metadata
    data_carga TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uk_vendas_semanal UNIQUE (ano, semana_ano, consultor_sk)
);

CREATE INDEX idx_vendas_semanal_periodo ON gold.vendas_consolidado_semanal(ano, semana_ano);
CREATE INDEX idx_vendas_semanal_consultor ON gold.vendas_consolidado_semanal(consultor_sk);
```

### 3. gold.metas_performance_mensal
**Propósito:** Metas, realizado e comissões por mês e consultor
**Atualização:** Mensal (dia 1 do mês seguinte)
**Uso:** Metas Liderança, Comissões, Baseline

```sql
CREATE TABLE gold.metas_performance_mensal (
    -- Chaves
    performance_id BIGSERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    consultor_sk INTEGER NOT NULL,
    gestor_sk INTEGER,

    -- Metas
    meta_faturamento NUMERIC(15,2),
    meta_num_vendas INTEGER,
    meta_novos_clientes INTEGER,

    -- Realizado
    realizado_faturamento NUMERIC(15,2),
    realizado_num_vendas INTEGER,
    realizado_novos_clientes INTEGER,

    -- Atingimento (%)
    atingimento_faturamento NUMERIC(5,2),
    atingimento_vendas NUMERIC(5,2),
    atingimento_clientes NUMERIC(5,2),
    atingimento_geral NUMERIC(5,2),

    -- Comissões
    comissao_base NUMERIC(10,2),
    comissao_bonus NUMERIC(10,2),
    comissao_total NUMERIC(10,2),

    -- Ranking
    ranking_equipe INTEGER,
    ranking_regional INTEGER,
    ranking_nacional INTEGER,

    -- Baseline (comparação histórica)
    baseline_faturamento NUMERIC(15,2), -- Média 6 meses anteriores
    variacao_vs_baseline NUMERIC(5,2),

    -- Metadata
    data_carga TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uk_metas_mensal UNIQUE (ano, mes, consultor_sk)
);

CREATE INDEX idx_metas_periodo ON gold.metas_performance_mensal(ano, mes);
CREATE INDEX idx_metas_consultor ON gold.metas_performance_mensal(consultor_sk);
```

### 4. gold.pipeline_snapshot_diario
**Propósito:** Snapshot diário do pipeline de vendas
**Atualização:** Diária (23h - final do dia)
**Uso:** Pipeline Anual, Pipeline 3 Meses, Oportunidades

```sql
CREATE TABLE gold.pipeline_snapshot_diario (
    -- Chaves
    snapshot_id BIGSERIAL PRIMARY KEY,
    data_snapshot DATE NOT NULL,
    consultor_sk INTEGER NOT NULL,

    -- Pipeline Total
    valor_pipeline_total NUMERIC(15,2),
    num_oportunidades_total INTEGER,

    -- Pipeline por Período Esperado de Fechamento
    valor_pipeline_30_dias NUMERIC(15,2),
    valor_pipeline_90_dias NUMERIC(15,2),
    valor_pipeline_180_dias NUMERIC(15,2),
    valor_pipeline_anual NUMERIC(15,2),

    -- Pipeline por Estágio
    valor_pipeline_prospeccao NUMERIC(15,2),
    valor_pipeline_qualificacao NUMERIC(15,2),
    valor_pipeline_proposta NUMERIC(15,2),
    valor_pipeline_negociacao NUMERIC(15,2),

    -- Probabilidade de Conversão
    valor_pipeline_ponderado NUMERIC(15,2), -- Valor * Probabilidade
    taxa_conversao_esperada NUMERIC(5,2),

    -- Movimentação
    valor_adicionado_dia NUMERIC(15,2),
    valor_removido_dia NUMERIC(15,2),
    num_oportunidades_ganhas_dia INTEGER,
    num_oportunidades_perdidas_dia INTEGER,

    -- Metadata
    data_carga TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uk_pipeline_snapshot UNIQUE (data_snapshot, consultor_sk)
);

CREATE INDEX idx_pipeline_snapshot_data ON gold.pipeline_snapshot_diario(data_snapshot);
CREATE INDEX idx_pipeline_snapshot_consultor ON gold.pipeline_snapshot_diario(consultor_sk);
```

### 5. gold.contratos_analise_mensal
**Propósito:** Análise de contratos, aumentos, quedas e tendências
**Atualização:** Mensal (dia 1)
**Uso:** Análise de Contratos, Aumentos e Quedas, Tabelas e Contratos

```sql
CREATE TABLE gold.contratos_analise_mensal (
    -- Chaves
    analise_id BIGSERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    cliente_sk INTEGER NOT NULL,
    consultor_sk INTEGER,

    -- Contrato Atual
    valor_contrato_atual NUMERIC(15,2),
    valor_contrato_mes_anterior NUMERIC(15,2),

    -- Movimentação
    tipo_movimentacao VARCHAR(20), -- AUMENTO, QUEDA, ESTAVEL, NOVO, CANCELADO
    valor_movimentacao NUMERIC(15,2),
    percentual_movimentacao NUMERIC(5,2),

    -- Motivo (se aplicável)
    motivo_movimentacao VARCHAR(200),
    categoria_motivo VARCHAR(50), -- PRECO, SERVICO, CONCORRENCIA, etc.

    -- Análise de Risco
    meses_consecutivos_queda INTEGER,
    alerta_churn BOOLEAN DEFAULT FALSE,

    -- Penetração de Portfolio
    num_produtos_contratados INTEGER,
    percentual_portfolio NUMERIC(5,2),
    produtos_adicionais_possiveis INTEGER,

    -- Histórico
    valor_historico_12_meses NUMERIC(15,2),
    tendencia_12_meses VARCHAR(20), -- CRESCENTE, DECRESCENTE, ESTAVEL

    -- Metadata
    data_carga TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uk_contratos_mensal UNIQUE (ano, mes, cliente_sk)
);

CREATE INDEX idx_contratos_periodo ON gold.contratos_analise_mensal(ano, mes);
CREATE INDEX idx_contratos_cliente ON gold.contratos_analise_mensal(cliente_sk);
CREATE INDEX idx_contratos_movimentacao ON gold.contratos_analise_mensal(tipo_movimentacao);
```

### 6. gold.motivos_perda_consolidado
**Propósito:** Consolidação de motivos de perda de oportunidades
**Atualização:** Semanal
**Uso:** Motivos de Perda, Análise de Pipeline

```sql
CREATE TABLE gold.motivos_perda_consolidado (
    -- Chaves
    consolidado_id BIGSERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,

    -- Motivo
    motivo_perda VARCHAR(200) NOT NULL,
    categoria_motivo VARCHAR(50), -- PRECO, TIMING, CONCORRENCIA, PRODUTO, OUTROS

    -- Agregações
    num_oportunidades_perdidas INTEGER,
    valor_total_perdido NUMERIC(15,2),
    valor_medio_perdido NUMERIC(15,2),

    -- Segmentação
    por_consultor JSONB, -- {consultor_sk: count}
    por_segmento_cliente JSONB, -- {segmento: count}
    por_produto JSONB, -- {produto: count}

    -- Tendência
    variacao_vs_mes_anterior NUMERIC(5,2),

    -- Metadata
    data_carga TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uk_motivos_perda UNIQUE (ano, mes, motivo_perda)
);

CREATE INDEX idx_motivos_perda_periodo ON gold.motivos_perda_consolidado(ano, mes);
CREATE INDEX idx_motivos_perda_categoria ON gold.motivos_perda_consolidado(categoria_motivo);
```

## 🔄 Estratégia de ETL para Gold

### Scripts de Transformação

```
python/transformers/gold/
├── transform_carteira_snapshot.py      # Snapshot diário da carteira
├── transform_vendas_semanal.py         # Consolidado semanal de vendas
├── transform_metas_performance.py      # Metas e performance mensal
├── transform_pipeline_snapshot.py      # Snapshot diário do pipeline
├── transform_contratos_mensal.py       # Análise mensal de contratos
└── transform_motivos_perda.py          # Consolidado de motivos de perda
```

### Scheduler Sugerido

```yaml
# Cron jobs sugeridos
jobs:
  - name: "Gold - Carteira Snapshot"
    schedule: "0 6 * * *"  # Diário às 6h
    script: "python/transformers/gold/transform_carteira_snapshot.py"

  - name: "Gold - Pipeline Snapshot"
    schedule: "0 23 * * *"  # Diário às 23h
    script: "python/transformers/gold/transform_pipeline_snapshot.py"

  - name: "Gold - Vendas Semanal"
    schedule: "0 6 * * 1"  # Segunda-feira às 6h
    script: "python/transformers/gold/transform_vendas_semanal.py"

  - name: "Gold - Metas Performance"
    schedule: "0 7 1 * *"  # Dia 1 de cada mês às 7h
    script: "python/transformers/gold/transform_metas_performance.py"

  - name: "Gold - Contratos Mensal"
    schedule: "0 8 1 * *"  # Dia 1 de cada mês às 8h
    script: "python/transformers/gold/transform_contratos_mensal.py"

  - name: "Gold - Motivos Perda"
    schedule: "0 7 * * 1"  # Segunda-feira às 7h
    script: "python/transformers/gold/transform_motivos_perda.py"
```

## 📊 Estratégia Power BI

### Modelo Híbrido Recomendado

| Tabela | Camada | Modo | Refresh | Uso |
|--------|--------|------|---------|-----|
| dim_data | Silver | Import | Mensal | Dimensão estável |
| dim_cliente | Silver | Import | Diário | Dimensão com SCD2 |
| dim_usuario | Silver | Import | Diário | Dimensão com SCD2 |
| fato_faturamento | Silver | DirectQuery | Real-time | Transações atuais |
| carteira_snapshot_diario | Gold | Import | Diário 6h | Dashboards carteira |
| vendas_consolidado_semanal | Gold | Import | Semanal | Dashboards vendas |
| metas_performance_mensal | Gold | Import | Mensal | Dashboards metas |
| pipeline_snapshot_diario | Gold | Import | Diário 23h | Dashboards pipeline |

### Relacionamentos Power BI

```
carteira_snapshot_diario
  ├─ consultor_sk → dim_usuario.usuario_sk
  ├─ cliente_sk → dim_cliente.cliente_sk
  └─ data_snapshot → dim_data.data_completa

vendas_consolidado_semanal
  ├─ consultor_sk → dim_usuario.usuario_sk
  ├─ gestor_sk → dim_usuario.usuario_sk
  └─ inicio_semana → dim_data.data_completa

metas_performance_mensal
  ├─ consultor_sk → dim_usuario.usuario_sk
  └─ (ano, mes) → dim_data.(ano, mes)
```

## 📈 Benefícios Esperados

1. **Performance:** Dashboards < 3 segundos (vs > 20 segundos sem Gold)
2. **Histórico:** Análises de tendência confiáveis
3. **Métricas Calculadas:** Comissões, metas, KPIs pré-calculados
4. **Escalabilidade:** Suporta crescimento de volume
5. **Governança:** Única fonte da verdade para métricas críticas

## 🎯 Fases de Implementação

### Fase 1 (MVP): Tabelas Essenciais
- ✅ gold.carteira_snapshot_diario
- ✅ gold.vendas_consolidado_semanal
- ✅ gold.pipeline_snapshot_diario

### Fase 2: Gestão e Análise
- ⏳ gold.metas_performance_mensal
- ⏳ gold.contratos_analise_mensal

### Fase 3: Análises Avançadas
- ⏳ gold.motivos_perda_consolidado
- ⏳ gold.penetracao_portfolio
- ⏳ gold.detalhamento_produtividade

## 📝 Próximos Passos

Quando decidir implementar:

1. **Criar schema gold no PostgreSQL**
2. **Implementar 2-3 tabelas prioritárias** (carteira, vendas, pipeline)
3. **Criar transformers Gold** (baseados no template Silver)
4. **Configurar refresh automático** (cron jobs)
5. **Conectar Power BI na Gold** (Import mode)
6. **Migrar dashboards prioritários** para consumir Gold
7. **Monitorar performance** e ajustar conforme necessário
8. **Expandir com novas tabelas** conforme demanda

---

**Documento criado em:** 2025-11-27
**Status:** DESIGN - Aguardando implementação
**Prioridade:** ALTA - Benefícios significativos identificados
