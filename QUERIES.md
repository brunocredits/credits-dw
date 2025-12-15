# Consultas SQL Essenciais - Credits DW

Este documento contém 4 queries essenciais para análise dos dados da camada Bronze.

---

## Query 1: Análise de Base Oficial (Clientes)

**Objetivo:** Visão geral da carteira de clientes por segmento e status.

**Retorna:**
- `segmento`: Segmento de mercado do cliente
- `status`: Status do cliente (Ativo/Inativo)
- `qtd_clientes`: Quantidade de clientes no segmento
- `qtd_cnpjs_ativos`: CNPJs com status ativo

**Uso:** Entender a distribuição da carteira de clientes e identificar segmentos prioritários.

```sql
SELECT 
    segmento,
    status,
    COUNT(*) as qtd_clientes,
    COUNT(*) FILTER (WHERE status = 'Ativo') as qtd_cnpjs_ativos,
    STRING_AGG(DISTINCT grupo, ', ') as grupos
FROM bronze.base_oficial
WHERE empresa = 'Credits'
GROUP BY segmento, status
ORDER BY qtd_clientes DESC;
```

---

## Query 2: Análise de Faturamento

**Objetivo:** Evolução mensal do faturamento com métricas financeiras.

**Retorna:**
- `mes`: Mês de referência
- `faturamento_total`: Soma de `valor_da_conta`
- `recebido_total`: Soma de `valor_recebido`
- `a_receber_total`: Soma de `valor_a_receber`
- `qtd_clientes`: Clientes únicos que faturaram
- `qtd_notas`: Total de notas fiscais
- `ticket_medio`: Valor médio por nota

**Uso:** Acompanhar performance financeira mensal e identificar tendências.

```sql
SELECT 
    DATE_TRUNC('month', data_fat)::date as mes,
    SUM(valor_da_conta) as faturamento_total,
    SUM(valor_recebido) as recebido_total,
    SUM(valor_a_receber) as a_receber_total,
    COUNT(DISTINCT cnpj) as qtd_clientes,
    COUNT(*) as qtd_notas,
    AVG(valor_da_conta) as ticket_medio,
    ROUND(
        (SUM(valor_recebido) * 100.0) / NULLIF(SUM(valor_da_conta), 0), 
        2
    ) as taxa_recebimento_pct
FROM bronze.faturamento
WHERE data_fat IS NOT NULL
  AND empresa = 'Credits'
GROUP BY DATE_TRUNC('month', data_fat)
ORDER BY mes DESC;
```

---

## Query 3: Análise de Usuários (Equipe de Vendas)

**Objetivo:** Visão da estrutura da equipe de vendas por time e cargo.

**Retorna:**
- `time`: Time de vendas
- `cargo`: Cargo do colaborador
- `status_vendedor`: Status (Ativo/Inativo)
- `qtd_vendedores`: Quantidade de vendedores
- `vendedores`: Lista de nomes

**Uso:** Entender a estrutura da equipe comercial e distribuição de recursos.

```sql
SELECT 
    time,
    cargo,
    status_vendedor,
    COUNT(*) as qtd_vendedores,
    STRING_AGG(consultor, ', ' ORDER BY consultor) as vendedores
FROM bronze.usuarios
GROUP BY time, cargo, status_vendedor
ORDER BY time, cargo, qtd_vendedores DESC;
```

---

## Query 4: Análise Financeira Completa (JOIN)

**Objetivo:** Análise 360° cruzando faturamento, clientes e vendedores para responder questões financeiras e comerciais.

**Retorna:**
- `mes`: Mês de referência
- `cliente_nome_fantasia`: Nome do cliente
- `cnpj`: CNPJ do cliente
- `segmento`: Segmento do cliente
- `vendedor`: Vendedor responsável
- `time_vendedor`: Time do vendedor
- `cargo`: Cargo do vendedor
- `faturamento`: Valor faturado
- `recebido`: Valor recebido
- `a_receber`: Valor a receber
- `taxa_recebimento_pct`: % de recebimento

**Uso:** Responder perguntas como:
- Qual o faturamento por vendedor/time?
- Quais segmentos têm melhor taxa de recebimento?
- Qual a performance financeira por cliente?
- Quais vendedores têm maior inadimplência na carteira?

```sql
SELECT 
    DATE_TRUNC('month', f.data_fat)::date as mes,
    f.cliente_nome_fantasia,
    f.cnpj,
    bo.segmento,
    bo.grupo,
    f.vendedor,
    u.time as time_vendedor,
    u.cargo,
    u.nivel,
    SUM(f.valor_da_conta) as faturamento,
    SUM(f.valor_recebido) as recebido,
    SUM(f.valor_a_receber) as a_receber,
    COUNT(*) as qtd_notas,
    ROUND(
        (SUM(f.valor_recebido) * 100.0) / NULLIF(SUM(f.valor_da_conta), 0),
        2
    ) as taxa_recebimento_pct
FROM bronze.faturamento f
LEFT JOIN bronze.base_oficial bo 
    ON f.cnpj = bo.cnpj
LEFT JOIN bronze.usuarios u 
    ON UPPER(TRIM(SPLIT_PART(f.vendedor, '-', 1))) = UPPER(u.consultor)
WHERE f.empresa = 'Credits'
  AND f.data_fat >= '2024-01-01'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY mes DESC, faturamento DESC;
```

**Exemplos de análises derivadas:**

```sql
-- Top 10 vendedores por faturamento
SELECT vendedor, time_vendedor, SUM(faturamento) as total
FROM (...query acima...)
GROUP BY vendedor, time_vendedor
ORDER BY total DESC LIMIT 10;

-- Segmentos com melhor taxa de recebimento
SELECT segmento, AVG(taxa_recebimento_pct) as taxa_media
FROM (...query acima...)
GROUP BY segmento
ORDER BY taxa_media DESC;

-- Performance por time
SELECT time_vendedor, SUM(faturamento) as total, AVG(taxa_recebimento_pct) as taxa_media
FROM (...query acima...)
GROUP BY time_vendedor
ORDER BY total DESC;
```

---

## 💡 Dicas de Performance

- Use índices em `cnpj`, `vendedor`, `data_fat` (ver [INDEXES.md](INDEXES.md))
- Sempre filtre por `empresa = 'Credits'` quando necessário
- Use `EXPLAIN ANALYZE` para otimizar queries lentas
- Para análises de períodos longos, considere criar views materializadas
