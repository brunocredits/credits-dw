# Camada Gold - Views Analíticas

**Versão:** 1.0
**Data:** 27/11/2025
**Status:** Produção

---

## Visão Geral

A camada Gold implementa **views analíticas simples** baseadas APENAS em dados reais disponíveis nas camadas Bronze e Silver.

**Decisão de Design:** Após análise completa, identificamos que tabelas Gold com transformers inventavam campos que não existem nos dados de origem (pipeline, oportunidades, metas, etc). A solução foi **substituir tabelas por views** que refletem exatamente o que temos.

---

## Arquitetura

```
┌─────────────────────┐
│   SILVER LAYER      │
│  (Star Schema)      │
│                     │
│  • dim_data         │
│  • dim_cliente      │
│  • dim_usuario      │
│  • fato_faturamento │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│    GOLD LAYER       │
│   (5 Views)         │
│                     │
│  • vendas_diarias   │
│  • vendas_semanais  │
│  • vendas_mensais   │
│  • carteira_clientes│
│  • performance_     │
│    consultores      │
└─────────────────────┘
```

**Características:**
- ✅ Views SQL (não materializado)
- ✅ Apenas dados reais (sem NULL/placeholders)
- ✅ Performance: queries diretas na Silver
- ✅ Manutenção: zero (sem ETL)
- ✅ Atualização: automática (reflete Silver em real-time)

---

## Views Disponíveis

### 1. vendas_diarias

**Descrição:** Agregação diária de vendas por consultor

**Granularidade:** 1 linha = 1 consultor + 1 dia

**Campos principais:**
- `data_completa`, `ano`, `mes`, `dia`, `semana_ano`
- `consultor_sk`, `consultor_nome`, `consultor_email`
- `gestor_sk`, `gestor_nome`
- `num_vendas`, `num_clientes`
- `valor_bruto_total`, `valor_desconto_total`, `valor_liquido_total`
- `valor_imposto_total`, `valor_comissao_total`
- `ticket_medio`, `menor_venda`, `maior_venda`
- `vendas_brl`, `vendas_usd`, `vendas_eur`

**Exemplo de uso:**
```sql
-- Vendas do último mês por consultor
SELECT
    consultor_nome,
    SUM(num_vendas) as total_vendas,
    SUM(valor_liquido_total) as faturamento_total
FROM gold.vendas_diarias
WHERE data_completa >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY consultor_nome
ORDER BY faturamento_total DESC;
```

**Registros:** 13

---

### 2. vendas_semanais

**Descrição:** Agregação semanal de vendas por consultor

**Granularidade:** 1 linha = 1 consultor + 1 semana

**Campos principais:**
- `ano`, `semana_ano`, `inicio_semana`, `fim_semana`
- `consultor_sk`, `consultor_nome`, `consultor_email`
- `gestor_sk`, `gestor_nome`
- `num_vendas`, `num_clientes`, `dias_com_venda`
- `valor_bruto_total`, `valor_desconto_total`, `valor_liquido_total`
- `ticket_medio`, `menor_venda`, `maior_venda`

**Exemplo de uso:**
```sql
-- Top 5 consultores da semana atual
SELECT
    consultor_nome,
    num_vendas,
    valor_liquido_total,
    ticket_medio
FROM gold.vendas_semanais
WHERE ano = EXTRACT(YEAR FROM CURRENT_DATE)
  AND semana_ano = EXTRACT(WEEK FROM CURRENT_DATE)
ORDER BY valor_liquido_total DESC
LIMIT 5;
```

**Registros:** 13

---

### 3. vendas_mensais

**Descrição:** Agregação mensal de vendas por consultor com breakdown por moeda

**Granularidade:** 1 linha = 1 consultor + 1 mês

**Campos principais:**
- `ano`, `mes`, `nome_mes`
- `consultor_sk`, `consultor_nome`, `consultor_email`
- `consultor_area`, `consultor_senioridade`
- `gestor_sk`, `gestor_nome`
- `num_vendas`, `num_clientes`, `dias_com_venda`
- `valor_bruto_total`, `valor_desconto_total`, `valor_liquido_total`
- `faturamento_brl`, `faturamento_usd`, `faturamento_eur`

**Exemplo de uso:**
```sql
-- Faturamento mensal por área
SELECT
    ano,
    mes,
    consultor_area,
    SUM(valor_liquido_total) as faturamento_total,
    SUM(num_vendas) as total_vendas
FROM gold.vendas_mensais
GROUP BY ano, mes, consultor_area
ORDER BY ano DESC, mes DESC, faturamento_total DESC;
```

**Registros:** 12

---

### 4. carteira_clientes

**Descrição:** Carteira de clientes com histórico de faturamento e recência

**Granularidade:** 1 linha = 1 cliente + 1 consultor

**Campos principais:**
- `cliente_sk`, `cnpj_cpf_nk`, `razao_social`
- `tipo_pessoa`, `status_cliente`, `porte_empresa`, `categoria_risco`
- `consultor_sk`, `consultor_nome`, `consultor_email`
- `gestor_sk`, `gestor_nome`
- `primeira_compra`, `ultima_compra`
- `num_compras_total`, `dias_com_compra`
- `faturamento_total`, `ticket_medio`
- `faturamento_30_dias`, `faturamento_90_dias`, `faturamento_180_dias`
- `dias_sem_compra`

**Exemplo de uso:**
```sql
-- Clientes em risco de churn (sem compra há mais de 90 dias)
SELECT
    razao_social,
    consultor_nome,
    ultima_compra,
    dias_sem_compra,
    faturamento_total
FROM gold.carteira_clientes
WHERE dias_sem_compra > 90
ORDER BY faturamento_total DESC;
```

**Registros:** 13

---

### 5. performance_consultores

**Descrição:** Performance agregada de consultores (lifetime + recência)

**Granularidade:** 1 linha = 1 consultor

**Campos principais:**
- `consultor_sk`, `consultor_nome`, `consultor_email`
- `consultor_area`, `consultor_senioridade`
- `tipo_canal`, `canal_principal`
- `gestor_sk`, `gestor_nome`
- `num_vendas_total`, `num_clientes_total`, `meses_ativos`
- `primeira_venda`, `ultima_venda`
- `valor_bruto_total`, `valor_desconto_total`, `valor_liquido_total`
- `ticket_medio_geral`, `menor_venda`, `maior_venda`
- `faturamento_30_dias`, `vendas_30_dias`
- `faturamento_90_dias`, `vendas_90_dias`

**Exemplo de uso:**
```sql
-- Ranking de consultores por faturamento total
SELECT
    consultor_nome,
    consultor_area,
    num_vendas_total,
    valor_liquido_total,
    ticket_medio_geral,
    faturamento_30_dias
FROM gold.performance_consultores
WHERE num_vendas_total > 0
ORDER BY valor_liquido_total DESC;
```

**Registros:** 12

---

## Vantagens da Abordagem com Views

### ✅ **Dados Reais**
- Nenhum campo inventado (NULL, hardcoded, placeholder)
- Apenas agregações de dados existentes em Bronze/Silver
- Reflete exatamente o que temos

### ✅ **Manutenção Zero**
- Sem ETL para manter
- Sem transformers para debugar
- Sem schedule de refresh
- Atualização automática quando Silver atualiza

### ✅ **Performance**
- Views são leves (não ocupam espaço)
- PostgreSQL otimiza queries automaticamente
- Possibilidade de materializar futuramente se necessário

### ✅ **Simplicidade**
- Apenas SQL puro
- Fácil de entender e modificar
- Segue padrão do projeto (Bronze/Silver)

---

## Comparação: Tabelas vs Views

| Aspecto | Tabelas (Anterior) | Views (Atual) |
|---------|-------------------|---------------|
| **Dados** | Inventados (NULL/hardcoded) | 100% reais |
| **Manutenção** | ETL complexo | Zero |
| **Storage** | ~53 registros duplicados | 0 bytes |
| **Atualização** | Manual (transformers) | Automática |
| **Complexidade** | 6 transformers + runner | 1 arquivo SQL |
| **Alinhamento** | Desalinhado Bronze/Silver | Perfeitamente alinhado |

---

## Power BI Integration

### Modelo Recomendado

**Import Mode (Gold):**
```
gold.vendas_mensais         → Dashboard executivo mensal
gold.performance_consultores → Ranking de vendedores
gold.carteira_clientes       → Análise de carteira
```

**DirectQuery (Silver):**
```
silver.fato_faturamento → Drill-down transacional
silver.dim_data         → Filtros de data
silver.dim_usuario      → Filtros de consultor
silver.dim_cliente      → Filtros de cliente
```

### Relacionamentos

```
gold.vendas_mensais[consultor_sk] → dim_usuario[usuario_sk]
gold.carteira_clientes[cliente_sk] → dim_cliente[cliente_sk]
gold.performance_consultores[consultor_sk] → dim_usuario[usuario_sk]
```

### Exemplos DAX

```dax
// Faturamento Mês Atual
Faturamento Mês Atual =
CALCULATE(
    SUM(vendas_mensais[valor_liquido_total]),
    vendas_mensais[ano] = YEAR(TODAY()),
    vendas_mensais[mes] = MONTH(TODAY())
)

// Crescimento vs Mês Anterior
Crescimento MoM% =
VAR FaturamentoAtual = [Faturamento Mês Atual]
VAR FaturamentoAnterior =
    CALCULATE(
        SUM(vendas_mensais[valor_liquido_total]),
        DATEADD(vendas_mensais[data_completa], -1, MONTH)
    )
RETURN
    DIVIDE(FaturamentoAtual - FaturamentoAnterior, FaturamentoAnterior, 0)

// Clientes Ativos (compra nos últimos 90 dias)
Clientes Ativos =
CALCULATE(
    DISTINCTCOUNT(carteira_clientes[cliente_sk]),
    carteira_clientes[faturamento_90_dias] > 0
)
```

---

## Manutenção

### Atualização das Views

Views são atualizadas **automaticamente** quando:
- Ingestores Bronze executam
- Transformers Silver executam
- Dados são inseridos/atualizados na Silver

**Não é necessário nenhum processo ETL para Gold.**

### Modificar uma View

```sql
-- Exemplo: adicionar campo na view vendas_diarias
CREATE OR REPLACE VIEW gold.vendas_diarias AS
SELECT
    -- campos existentes
    ...,
    -- novo campo
    COUNT(DISTINCT CASE WHEN f.status_pagamento = 'PAGO'
        THEN f.faturamento_sk END) as vendas_pagas
FROM silver.fato_faturamento f
...;
```

### Materializar uma View (se necessário)

Se performance se tornar um problema com crescimento de dados:

```sql
-- Criar tabela materializada
CREATE MATERIALIZED VIEW gold.vendas_mensais_mat AS
SELECT * FROM gold.vendas_mensais;

-- Criar índices
CREATE INDEX idx_vendas_mensais_mat_ano_mes
ON gold.vendas_mensais_mat(ano, mes);

-- Refresh manual ou agendado
REFRESH MATERIALIZED VIEW gold.vendas_mensais_mat;
```

---

## Troubleshooting

### View retorna registros vazios

**Causa:** Fato_faturamento ou dimensões vazias
**Solução:** Executar ingestores Bronze + transformers Silver

```bash
docker compose exec etl-processor python python/run_bronze_ingestors.py
docker compose exec etl-processor python python/run_silver_transformers.py
```

### Performance lenta

**Causa:** Muitos joins ou agregações pesadas
**Solução:** Considerar materializar a view específica

### Dados desatualizados

**Causa:** Silver não foi atualizada
**Solução:** Views sempre refletem Silver em real-time. Atualizar Silver resolve.

---

## Próximos Passos (Futuro)

Quando tivermos dados adicionais disponíveis:

1. **Pipeline/Oportunidades:**
   - Criar tabela `bronze.oportunidades`
   - Criar dimensão `silver.dim_oportunidade`
   - Criar view `gold.pipeline_vendas`

2. **Metas:**
   - Criar tabela `bronze.metas_consultores`
   - Criar dimensão `silver.dim_meta`
   - Estender view `gold.performance_consultores` com campos de meta

3. **Motivos de Perda:**
   - Criar tabela `bronze.oportunidades_perdidas`
   - Criar view `gold.analise_perdas`

**Princípio:** Só adicionar quando dados reais estiverem disponíveis em Bronze/Silver.

---

## Conclusão

A camada Gold agora está **perfeitamente alinhada** com Bronze e Silver:
- ✅ Apenas dados reais
- ✅ Sem campos inventados
- ✅ Manutenção zero
- ✅ Performance adequada
- ✅ Pronta para Power BI

**Filosofia:** "Agregue o que existe, não invente o que falta."
