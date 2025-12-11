# Consultas Analíticas - Credits DW (Bronze Layer)

Este documento contém queries SQL essenciais para análise de faturamento.
**Métrica principal:** `valor_da_conta` na tabela `faturamento`.

---

## 📊 Queries Gerais

### 1. Faturamento Total por Mês
**Objetivo:** Visualizar a evolução do faturamento da empresa Credits ao longo dos meses.

**Retorna:**
- `mes`: Mês de referência (primeiro dia do mês)
- `faturamento_total`: Soma total de `valor_da_conta` para o mês
- `qtd_clientes`: Quantidade de clientes únicos (CNPJs distintos) que faturaram no mês
- `faturamento_ano`: Soma total de `valor_da_conta` para o ano completo
- `acumulado_ano`: Soma acumulada do ano até aquele mês

**Uso:** Identifica tendências de crescimento/queda e sazonalidade no faturamento.

```sql
SELECT 
    DATE_TRUNC('month', data_fat)::date as mes,
    SUM(valor_da_conta) as faturamento_total,
    COUNT(DISTINCT cnpj) as qtd_clientes,
    SUM(SUM(valor_da_conta)) OVER (
        PARTITION BY EXTRACT(YEAR FROM data_fat)
    ) as faturamento_ano,
    SUM(SUM(valor_da_conta)) OVER (
        PARTITION BY EXTRACT(YEAR FROM data_fat) 
        ORDER BY DATE_TRUNC('month', data_fat)
    ) as acumulado_ano
FROM bronze.faturamento
WHERE data_fat IS NOT NULL
  AND empresa = 'Credits'
GROUP BY DATE_TRUNC('month', data_fat), EXTRACT(YEAR FROM data_fat)
ORDER BY 1 DESC;
```

### 2. Top 10 Clientes por Valor Faturado
**Objetivo:** Identificar os 10 maiores clientes da empresa Credits por volume de faturamento.

**Retorna:**
- `cnpj`: CNPJ do cliente
- `cliente_nome_fantasia`: Nome fantasia do cliente
- `total_faturado`: Soma total de `valor_da_conta` para o cliente (todo período)
- `qtd_notas`: Quantidade de notas fiscais emitidas para o cliente

**Uso:** Análise de concentração de receita e identificação de clientes-chave (regra 80/20).

```sql
SELECT 
    cnpj,
    cliente_nome_fantasia,
    SUM(valor_da_conta) as total_faturado,
    COUNT(*) as qtd_notas
FROM bronze.faturamento
WHERE valor_da_conta IS NOT NULL 
  AND empresa = 'Credits'
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10;
```

---

## 🎯 Análise Específica: JEITTO MEIOS DE PAGAMENTO

### 3. Faturamento Mês a Mês da JEITTO
**Objetivo:** Analisar a evolução temporal do faturamento específico da JEITTO MEIOS DE PAGAMENTO LTDA, com métricas financeiras detalhadas.

**Retorna:**
- `mes`: Mês de referência
- `cliente_nome_fantasia`: Nome do cliente (JEITTO)
- `qtd_notas_fiscais`: Quantidade de notas emitidas no mês
- `faturamento_mes`: Valor total faturado no mês
- `recebido_mes`: Valor efetivamente recebido no mês
- `pendente_mes`: Valor ainda não recebido (a receber)
- `taxa_recebimento_pct`: Percentual de quanto foi recebido em relação ao faturado
- `ticket_medio`: Valor médio por nota fiscal

**Uso:** Acompanhar saúde financeira do cliente, identificar meses com inadimplência e padrões de comportamento.

```sql
SELECT 
    DATE_TRUNC('month', data_fat) as mes,
    cliente_nome_fantasia,
    COUNT(*) as qtd_notas_fiscais,
    SUM(valor_da_conta) as faturamento_mes,
    SUM(valor_recebido) as recebido_mes,
    SUM(valor_a_receber) as pendente_mes,
    ROUND(
        (SUM(valor_recebido) * 100.0) / NULLIF(SUM(valor_da_conta), 0), 
        2
    ) as taxa_recebimento_pct,
    AVG(valor_da_conta) as ticket_medio
FROM bronze.faturamento
WHERE cliente_nome_fantasia ILIKE '%JEITTO%'
  AND empresa = 'Credits'
  AND data_fat >= '2022-01-01'
GROUP BY 1, 2
ORDER BY 1 DESC;
```

### 4. JEITTO com Vendedor e Segmento (JOIN)
**Objetivo:** Análise 360° da JEITTO cruzando dados de faturamento com informações de vendedor e segmentação do cliente.

**Retorna:**
- `mes`: Mês de referência
- `cliente_nome_fantasia`: Nome do cliente (JEITTO)
- `vendedor`: Nome do vendedor responsável
- `time_vendedor`: Time ao qual o vendedor pertence
- `cargo`: Cargo do vendedor
- `segmento`: Segmento de mercado do cliente (da tabela `base_oficial`)
- `faturamento_total`: Valor total faturado no mês
- `recebido_total`: Valor total recebido no mês
- `qtd_notas`: Quantidade de notas fiscais
- `taxa_recebimento_pct`: Percentual de recebimento

**Uso:** Entender o contexto comercial completo - quem vende, qual time, segmento do cliente e performance financeira. Útil para análise de carteira e performance de vendedores.

```sql
SELECT 
    DATE_TRUNC('month', f.data_fat)::date as mes,
    f.cliente_nome_fantasia,
    f.vendedor,
    u.time as time_vendedor,
    u.cargo,
    bo.segmento,
    SUM(f.valor_da_conta) as faturamento_total,
    SUM(f.valor_recebido) as recebido_total,
    COUNT(*) as qtd_notas,
    ROUND(
        (SUM(f.valor_recebido) * 100.0) / NULLIF(SUM(f.valor_da_conta), 0),
        2
    ) as taxa_recebimento_pct
FROM bronze.faturamento f
LEFT JOIN bronze.usuarios u 
    ON UPPER(TRIM(SPLIT_PART(f.vendedor, '-', 1))) = UPPER(u.consultor)
LEFT JOIN bronze.base_oficial bo 
    ON f.cnpj = bo.cnpj
WHERE f.cliente_nome_fantasia ILIKE '%JEITTO%'
  AND f.empresa = 'Credits'
  AND f.data_fat >= '2022-01-01'
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1 DESC;
```

---

## 💡 Dicas

- Use índices em `cnpj`, `vendedor`, `data_fat`
- Sempre filtre por `empresa = 'Credits'` quando necessário
- Use `EXPLAIN ANALYZE` para otimizar queries
