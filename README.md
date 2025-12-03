# Credits DW - Data Warehouse Pipeline

Pipeline de ingestão de dados para a camada Bronze do Data Warehouse, implementado com arquitetura modular e otimizado para alta performance.

## 🏗️ Arquitetura

### Camadas do Data Warehouse
- **Bronze (RAW)**: Dados brutos validados e limpos
- **Silver**: Dados transformados e enriquecidos *(próxima etapa)*
- **Gold**: Dados agregados para consumo *(próxima etapa)*

### Componentes Principais

```
python/
├── core/
│   ├── base_ingestor.py    # Orquestrador principal
│   ├── data_cleaner.py     # Limpeza de dados
│   ├── file_handler.py     # Gerenciamento de arquivos
│   └── validator.py        # Validação de estrutura
├── ingestors/
│   ├── ingest_faturamento.py
│   ├── ingest_usuarios.py
│   └── ingest_base_oficial.py
└── utils/
    ├── db_connection.py    # Conexão com PostgreSQL
    └── audit.py            # Sistema de auditoria
```

## 🚀 Quick Start

### Pré-requisitos
- Docker e Docker Compose
- PostgreSQL (Azure Database for PostgreSQL)
- Python 3.9+

### Configuração

1. **Clone o repositório**
```bash
git clone https://github.com/brunocredits/credits-dw.git
cd credits-dw
```

2. **Configure variáveis de ambiente**
```bash
cp .env.example .env
# Edite .env com suas credenciais
```

3. **Execute o pipeline**
```bash
./run_pipeline.sh
```

### Reset do Ambiente
Para limpar dados e preparar nova carga:
```bash
./reset_env.sh
```

## 📊 Funcionalidades

### ✅ Implementado

- **Detecção de Duplicatas**: Hash MD5 para evitar reprocessamento
- **Validação de Headers**: Comparação com templates oficiais
- **Limpeza de Dados**:
  - Conversão de formato brasileiro (1.000,00 → 1000.00)
  - Tratamento de hífen como zero
  - Validação de datas (DD/MM/YYYY)
- **Idempotência**: Delete-before-load por arquivo
- **Auditoria Completa**: Rastreamento de execuções e erros
- **Alta Performance**: PostgreSQL COPY para carga em massa

### 🎯 Características Técnicas

- **Arquitetura Modular**: Princípios SOLID (SRP, DIP)
- **Clean Code**: Código documentado em português
- **Segurança**: Validação rigorosa, parâmetros bind SQL
- **Performance**: Operações vetorizadas (Pandas/Numpy)

## 📁 Estrutura de Dados

### Tabelas Bronze

| Tabela | Colunas | Registros | Descrição |
|--------|---------|-----------|------------|
| `bronze.faturamento` | 36 | 213.403 | Dados de faturamento e recebíveis |
| `bronze.base_oficial` | 15 | 3.037 | Cadastro de clientes ativos |
| `bronze.usuarios` | 13 | 40 | Cadastro de consultores/vendedores |
| `bronze.data` | 17 | 4.018 | Dimensão temporal |

### Índices de Performance

A camada bronze possui **17 índices** (~11.6MB) para otimizar queries:

**faturamento (6 índices)**
- `idx_faturamento_cnpj` - Join com base_oficial
- `idx_faturamento_vendedor` - Join com usuarios
- `idx_faturamento_data_fat` - Filtros temporais
- `idx_faturamento_empresa_data` - Análises por empresa/período
- `idx_faturamento_status` - Filtros por status
- `faturamento_pkey` - Chave primária

**base_oficial (5 índices)**
- `idx_base_oficial_cnpj` (UNIQUE) - Chave natural, join com faturamento
- `idx_base_oficial_lider` - Join com usuarios
- `idx_base_oficial_responsavel` - Join com usuarios
- `idx_base_oficial_status` - Filtros por status
- `base_oficial_pkey` - Chave primária

**usuarios (5 índices)**
- `idx_usuarios_consultor` (UNIQUE) - Chave natural
- `idx_usuarios_cargo` - Filtros
- `idx_usuarios_time` - Filtros
- `idx_usuarios_status` - Filtros
- `usuarios_pkey` - Chave primária

**data (1 índice)**
- `data_pkey` - Chave primária (data)

### Views de Monitoramento
- `bronze.v_index_usage` - Monitoramento de uso e performance dos índices

### Auditoria
- `auditoria.historico_execucao` - Log de execuções
- `auditoria.log_rejeicao` - Linhas rejeitadas com motivo

## 🔍 Queries de Monitoramento

### Verificar última execução
```sql
SELECT script_nome, data_inicio, status, 
       linhas_processadas, linhas_inseridas, linhas_erro
FROM auditoria.historico_execucao
ORDER BY data_inicio DESC
LIMIT 10;
```

### Analisar rejeições
```sql
SELECT tabela_destino, motivo_rejeicao, COUNT(*) as qtd
FROM auditoria.log_rejeicao
GROUP BY tabela_destino, motivo_rejeicao
ORDER BY qtd DESC;
```

### Verificar duplicatas detectadas
```sql
SELECT COUNT(*) as arquivos_duplicados
FROM auditoria.historico_execucao
WHERE status = 'sucesso' 
  AND file_hash IN (
    SELECT file_hash 
    FROM auditoria.historico_execucao 
    GROUP BY file_hash 
    HAVING COUNT(*) > 1
  );
```

### Monitorar uso dos índices
```sql
-- Ver quais índices estão sendo mais utilizados
SELECT * FROM bronze.v_index_usage;

-- Verificar tamanho dos índices
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as size
FROM pg_stat_user_indexes
WHERE schemaname = 'bronze'
ORDER BY pg_relation_size(indexrelid) DESC;
```

## 📋 Roadmap de Desenvolvimento

### 🔄 Fase 2: Camada Silver (Transformação e Qualidade)

**Objetivo**: Dados limpos, deduplicados e enriquecidos prontos para análise

#### 2.1 Arquitetura Silver
```
silver/
├── dim_clientes          # Dimensão de clientes (SCD Type 2)
├── dim_usuarios          # Dimensão de usuários
├── dim_tempo             # Dimensão temporal (já existe em bronze.data)
├── fato_faturamento      # Fatos de faturamento transformados
└── metricas_qualidade    # Métricas de qualidade de dados
```

#### 2.2 Transformações Planejadas

**Deduplicação Inteligente**
- [ ] Implementar algoritmo de matching fuzzy para clientes
- [ ] Criar regras de merge baseadas em:
  - CNPJ (chave primária)
  - Razão social (similaridade > 85%)
  - Endereço e telefone (dados auxiliares)
- [ ] Manter histórico de merges na auditoria

**Enriquecimento de Dados**
- [ ] Integrar API da Receita Federal (validação CNPJ)
- [ ] Adicionar geolocalização (CEP → lat/long)
- [ ] Calcular métricas derivadas:
  - Aging de recebíveis (dias em atraso)
  - Score de inadimplência
  - Ticket médio por cliente
  - Lifetime Value (LTV)

**Slowly Changing Dimensions (SCD Type 2)**
- [ ] Implementar versionamento de clientes
- [ ] Campos de controle:
  - `valid_from` - Data início vigência
  - `valid_to` - Data fim vigência
  - `is_current` - Flag de versão atual
  - `version` - Número da versão
- [ ] Trigger automático para criar nova versão em mudanças

**Validações de Negócio**
- [ ] Regras de consistência:
  - Valor a receber > 0
  - Data vencimento >= Data faturamento
  - Cliente existe na base oficial
  - Vendedor ativo no sistema
- [ ] Quarentena para dados suspeitos
- [ ] Alertas automáticos para anomalias

#### 2.3 Cronograma Silver (Estimativa: 3-4 semanas)

**Semana 1**: Estrutura e Deduplicação
- Criar schema `silver` no banco
- Implementar `SilverTransformer` base
- Desenvolver algoritmo de deduplicação
- Testes unitários de matching

**Semana 2**: Enriquecimento
- Integrar APIs externas (Receita Federal)
- Implementar cálculo de métricas derivadas
- Adicionar geolocalização
- Criar pipeline de enriquecimento

**Semana 3**: SCD Type 2
- Implementar versionamento de dimensões
- Criar triggers de atualização
- Desenvolver queries de consulta histórica
- Testes de integridade temporal

**Semana 4**: Validações e Qualidade
- Implementar regras de negócio
- Criar sistema de quarentena
- Desenvolver dashboard de qualidade
- Documentação e testes de integração

---

### 📊 Fase 3: Camada Gold (Analytics e BI)

**Objetivo**: Dados agregados e otimizados para consumo em dashboards e relatórios

#### 3.1 Arquitetura Gold
```
gold/
├── fato_faturamento_mensal    # Agregação mensal
├── fato_faturamento_diario    # Agregação diária
├── metricas_vendedores        # Performance de vendedores
├── metricas_clientes          # Análise de clientes
├── metricas_produtos          # Análise de produtos/serviços
└── kpis_executivos            # KPIs consolidados
```

#### 3.2 Métricas e KPIs Planejados

**Faturamento**
- [ ] Receita total (MRR - Monthly Recurring Revenue)
- [ ] Receita por canal de vendas
- [ ] Receita por região geográfica
- [ ] Taxa de crescimento (MoM, YoY)
- [ ] Forecast de recebimento (próximos 30/60/90 dias)

**Inadimplência**
- [ ] Taxa de inadimplência (%)
- [ ] Valor em atraso por faixa (0-30, 31-60, 61-90, 90+ dias)
- [ ] Top 10 clientes inadimplentes
- [ ] Provisão para devedores duvidosos (PDD)

**Performance de Vendedores**
- [ ] Ranking de vendedores (por volume e valor)
- [ ] Taxa de conversão
- [ ] Ticket médio por vendedor
- [ ] Churn de clientes por vendedor

**Análise de Clientes**
- [ ] Segmentação RFM (Recency, Frequency, Monetary)
- [ ] Customer Lifetime Value (CLV)
- [ ] Taxa de retenção/churn
- [ ] Net Promoter Score (NPS) - se disponível

**Análise Temporal**
- [ ] Sazonalidade de vendas
- [ ] Tendências de crescimento
- [ ] Previsão de demanda (ML)

#### 3.3 Views Materializadas

**Refresh Automático**
```sql
-- Exemplo: Atualização incremental diária
CREATE MATERIALIZED VIEW gold.fato_faturamento_diario AS
SELECT 
    d.data,
    COUNT(DISTINCT f.cliente_id) as clientes_ativos,
    SUM(f.valor_a_receber) as receita_total,
    AVG(f.valor_a_receber) as ticket_medio,
    COUNT(*) as num_transacoes
FROM silver.fato_faturamento f
JOIN silver.dim_tempo d ON f.data_faturamento = d.data
GROUP BY d.data;

-- Refresh diário às 2h da manhã
CREATE INDEX idx_gold_fat_diario_data ON gold.fato_faturamento_diario(data);
REFRESH MATERIALIZED VIEW CONCURRENTLY gold.fato_faturamento_diario;
```

#### 3.4 Otimizações de Performance

**Particionamento**
- [ ] Particionar tabelas por data (mensal)
- [ ] Implementar partition pruning
- [ ] Configurar auto-vacuum por partição

**Índices Estratégicos**
- [ ] Índices compostos para queries frequentes
- [ ] Índices parciais para filtros comuns
- [ ] BRIN indexes para colunas temporais

**Agregações Pré-calculadas**
- [ ] Cubos OLAP para análise multidimensional
- [ ] Rollup tables para diferentes granularidades
- [ ] Cache de queries complexas (Redis)

#### 3.5 Cronograma Gold (Estimativa: 4-5 semanas)

**Semana 1**: Estrutura Base
- Criar schema `gold` e tabelas de fatos
- Implementar agregações básicas (diário/mensal)
- Desenvolver `GoldAggregator` base
- Testes de performance iniciais

**Semana 2**: Métricas de Negócio
- Implementar KPIs de faturamento
- Desenvolver métricas de inadimplência
- Criar análises de vendedores
- Dashboard de métricas em tempo real

**Semana 3**: Analytics Avançado
- Implementar segmentação RFM
- Desenvolver análise de cohort
- Criar previsões com ML (Prophet/ARIMA)
- Análise de sazonalidade

**Semana 4**: Otimização
- Implementar particionamento
- Criar índices otimizados
- Configurar views materializadas
- Testes de carga e performance

**Semana 5**: Integração BI
- Conectar Power BI / Metabase
- Criar dashboards executivos
- Desenvolver relatórios automatizados
- Documentação de uso

---

### 🔧 Melhorias Técnicas Paralelas

**Testes e Qualidade**
- [ ] Cobertura de testes > 80%
- [ ] Testes de carga (Apache JMeter)
- [ ] Testes de regressão automatizados

**DevOps e Infraestrutura**
- [ ] CI/CD com GitHub Actions
- [ ] Deploy automatizado (staging → prod)
- [ ] Rollback automático em falhas
- [ ] Blue-green deployment

**Observabilidade**
- [ ] Logs estruturados (JSON)
- [ ] Métricas customizadas (Prometheus)
- [ ] Dashboards de monitoramento (Grafana)
- [ ] Alertas inteligentes (PagerDuty)

**Segurança**
- [ ] Criptografia de dados em repouso
- [ ] Auditoria de acessos (quem viu o quê)
- [ ] Mascaramento de dados sensíveis
- [ ] Compliance LGPD

### 🔐 Segurança
- [ ] Implementar criptografia de dados sensíveis
- [ ] Adicionar auditoria de acessos
- [ ] Configurar backup automático
- [ ] Implementar disaster recovery

### 📱 Observabilidade
- [ ] Dashboard de métricas em tempo real
- [ ] Logs centralizados (ELK Stack)
- [ ] Rastreamento distribuído (OpenTelemetry)
- [ ] SLA monitoring

## 🛠️ Desenvolvimento

### Estrutura de Branches
- `main` - Produção
- `develop` - Desenvolvimento
- `feature/*` - Novas funcionalidades
- `hotfix/*` - Correções urgentes

### Padrões de Commit
```
feat: Nova funcionalidade
fix: Correção de bug
docs: Documentação
refactor: Refatoração
perf: Melhoria de performance
test: Testes
chore: Tarefas gerais
```

## 📚 Documentação Adicional

- [Guia de Demonstração](/.gemini/antigravity/brain/.../DEMO_GUIDE.md)
- [Regras de Validação](/.gemini/antigravity/brain/.../regras_validacao_faturamento.md)
- [Estrutura do Banco](/.gemini/antigravity/brain/.../estrutura_banco_dados.md)

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'feat: Add AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📝 Licença

Projeto proprietário - Credits Brasil

## 👥 Time

- **Desenvolvedor**: Bruno Pires
- **Organização**: Credits Brasil

---

**Status**: ✅ Bronze Layer - Produção  
**Próxima Milestone**: 🔄 Silver Layer - Em Planejamento