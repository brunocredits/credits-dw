# Consultas Analíticas - Credits DW (Bronze Layer)

Este documento contém exemplos de queries SQL para extrair insights das tabelas da camada Bronze.
**Métrica principal:** `valor_conta` na tabela `faturamento` é usada para cálculos de preço e receita.

---

## 📊 Consultas Básicas

### 1. Faturamento Total por Mês
Visualiza a evolução do faturamento (valor_conta) ao longo do tempo.

```sql
SELECT 
    DATE_TRUNC('month', data_fat) as mes,
    SUM(valor_conta) as faturamento_total,
    COUNT(DISTINCT cnpj) as qtd_clientes
FROM bronze.faturamento
WHERE data_fat IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC;
```

### 2. Top 10 Clientes por Valor Faturado
Identifica os maiores clientes com base no valor_conta.

```sql
SELECT 
    cnpj,
    cliente_nome_fantasia,
    SUM(valor_conta) as total_faturado,
    COUNT(*) as qtd_notas
FROM bronze.faturamento
WHERE valor_conta IS NOT NULL
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10;
```

---

## 🔍 Consultas Complexas com JOINs

### 3. Performance de Vendedores por Time
Relaciona faturamento com dados de vendedores para análise de performance por equipe.

```sql
SELECT 
    u.time,
    u.consultor as vendedor,
    u.cargo,
    COUNT(DISTINCT f.cnpj) as clientes_atendidos,
    SUM(f.valor_conta) as faturamento_total,
    AVG(f.valor_conta) as ticket_medio,
    SUM(CASE WHEN f.valor_recebido > 0 THEN f.valor_recebido ELSE 0 END) as valor_recebido,
    ROUND(
        (SUM(CASE WHEN f.valor_recebido > 0 THEN f.valor_recebido ELSE 0 END) * 100.0) / 
        NULLIF(SUM(f.valor_conta), 0), 
        2
    ) as taxa_recebimento_pct
FROM bronze.faturamento f
INNER JOIN bronze.usuarios u 
    ON f.vendedor = u.consultor
WHERE 
    f.data_fat >= CURRENT_DATE - INTERVAL '12 months'
    AND u.status_vendedor = 'Ativo'
GROUP BY 1, 2, 3
HAVING SUM(f.valor_conta) > 0
ORDER BY 1, 5 DESC;
```

### 4. Análise Completa: Cliente + Vendedor + Segmento
Visão 360° cruzando faturamento, cliente (base oficial) e vendedor.

```sql
WITH faturamento_agregado AS (
    SELECT 
        f.cnpj,
        f.vendedor,
        DATE_TRUNC('month', f.data_fat) as mes,
        SUM(f.valor_conta) as faturamento_mes,
        SUM(f.valor_recebido) as recebido_mes,
        COUNT(*) as qtd_notas
    FROM bronze.faturamento f
    WHERE f.data_fat >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY 1, 2, 3
)
SELECT 
    fa.mes,
    bo.segmento,
    bo.grupo,
    bo.cliente_nome_fantasia,
    u.time as time_vendedor,
    u.consultor as vendedor,
    SUM(fa.faturamento_mes) as faturamento_total,
    SUM(fa.recebido_mes) as recebido_total,
    SUM(fa.qtd_notas) as total_notas,
    ROUND(
        (SUM(fa.recebido_mes) * 100.0) / NULLIF(SUM(fa.faturamento_mes), 0),
        2
    ) as percentual_recebido,
    CASE 
        WHEN SUM(fa.faturamento_mes) > 100000 THEN 'Alto Valor'
        WHEN SUM(fa.faturamento_mes) > 50000 THEN 'Médio Valor'
        ELSE 'Baixo Valor'
    END as classificacao_cliente
FROM faturamento_agregado fa
LEFT JOIN bronze.base_oficial bo 
    ON fa.cnpj = bo.cnpj
LEFT JOIN bronze.usuarios u 
    ON fa.vendedor = u.consultor
WHERE 
    bo.status = 'Ativo'
    AND u.status_vendedor = 'Ativo'
GROUP BY 1, 2, 3, 4, 5, 6
HAVING SUM(fa.faturamento_mes) > 0
ORDER BY 1 DESC, 7 DESC;
```

---

## 📈 Queries Adicionais Úteis

### 5. Análise de Inadimplência
Identifica clientes com valores a receber elevados.

```sql
SELECT 
    f.cnpj,
    f.cliente_nome_fantasia,
    SUM(f.valor_conta) as total_faturado,
    SUM(f.valor_recebido) as total_recebido,
    SUM(f.valor_a_receber) as total_pendente,
    ROUND(
        (SUM(f.valor_a_receber) * 100.0) / NULLIF(SUM(f.valor_conta), 0), 
        2
    ) as percentual_pendente
FROM bronze.faturamento f
WHERE f.valor_a_receber > 0
GROUP BY 1, 2
HAVING SUM(f.valor_a_receber) > 10000
ORDER BY 5 DESC
LIMIT 20;
```

### 6. Evolução de Carteira por Vendedor
Acompanha crescimento da carteira de cada vendedor mês a mês.

```sql
SELECT 
    DATE_TRUNC('month', f.data_fat) as mes,
    f.vendedor,
    COUNT(DISTINCT f.cnpj) as clientes_unicos,
    SUM(f.valor_conta) as faturamento,
    AVG(f.valor_conta) as ticket_medio
FROM bronze.faturamento f
WHERE f.data_fat >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY 1, 2
ORDER BY 1 DESC, 4 DESC;
```

---

## 💡 Dicas de Performance

- Use índices nas colunas de JOIN (`cnpj`, `vendedor`, `consultor`)
- Filtre sempre por `data_fat` em queries com grandes volumes
- Utilize `EXPLAIN ANALYZE` para verificar planos de execução
- CTE (WITH) melhora legibilidade sem impactar performance significativamente
