# Consultas SQL Essenciais - Credits DW

Queries simples e diretas para análise dos dados da camada Bronze.

---

## Query 1: Base Oficial (Clientes)

**Objetivo:** Listar clientes ativos com dados completos.

```sql
SELECT 
    cnpj,
    nome_fantasia,
    segmento,
    grupo,
    responsavel
FROM bronze.base_oficial
WHERE empresa = 'Credits'
  AND status = 'Ativo'
  AND cnpj IS NOT NULL
  AND nome_fantasia IS NOT NULL
  AND segmento IS NOT NULL
  AND grupo IS NOT NULL
  AND responsavel IS NOT NULL
ORDER BY nome_fantasia
LIMIT 50;
```

---

## Query 2: Faturamento

**Objetivo:** Faturamento mensal com dados completos.

```sql
SELECT 
    DATE_TRUNC('month', data_fat)::date as mes,
    COUNT(*) as qtd_notas,
    SUM(valor_da_conta) as faturamento_total,
    COUNT(DISTINCT cnpj) as qtd_clientes
FROM bronze.faturamento
WHERE empresa = 'Credits'
  AND data_fat IS NOT NULL
  AND valor_da_conta IS NOT NULL
  AND cnpj IS NOT NULL
GROUP BY DATE_TRUNC('month', data_fat)
ORDER BY mes DESC
LIMIT 12;
```

---

## Query 3: Usuários (Equipe)

**Objetivo:** Listar vendedores ativos com dados completos.

```sql
SELECT 
    consultor,
    cargo,
    time,
    nivel
FROM bronze.usuarios
WHERE status_vendedor = 'Ativo'
  AND consultor IS NOT NULL
  AND cargo IS NOT NULL
  AND time IS NOT NULL
  AND nivel IS NOT NULL
ORDER BY time, consultor;
```

---

## Query 4: Análise Financeira (JOIN Completo)

**Objetivo:** Cruzar faturamento + clientes + vendedores com TODOS os dados preenchidos.

```sql
SELECT 
    f.cliente_nome_fantasia,
    f.cnpj,
    bo.segmento,
    f.vendedor,
    u.time,
    SUM(f.valor_da_conta) as faturamento,
    COUNT(*) as qtd_notas
FROM bronze.faturamento f
INNER JOIN bronze.base_oficial bo 
    ON f.cnpj = bo.cnpj
INNER JOIN bronze.usuarios u 
    ON UPPER(TRIM(SPLIT_PART(f.vendedor, '-', 1))) = UPPER(u.consultor)
WHERE f.empresa = 'Credits'
  AND f.data_fat >= '2024-01-01'
  -- Filtros para garantir dados completos no faturamento
  AND f.cliente_nome_fantasia IS NOT NULL
  AND f.cnpj IS NOT NULL
  AND f.vendedor IS NOT NULL
  AND f.valor_da_conta IS NOT NULL
  AND f.data_fat IS NOT NULL
  -- Filtros para garantir dados completos na base_oficial
  AND bo.segmento IS NOT NULL
  AND bo.nome_fantasia IS NOT NULL
  AND bo.status = 'Ativo'
  -- Filtros para garantir dados completos nos usuarios
  AND u.time IS NOT NULL
  AND u.cargo IS NOT NULL
  AND u.status_vendedor = 'Ativo'
GROUP BY 
    f.cliente_nome_fantasia,
    f.cnpj,
    bo.segmento,
    f.vendedor,
    u.time
HAVING SUM(f.valor_da_conta) > 0
ORDER BY faturamento DESC
LIMIT 100;
```

---

## 💡 Dicas

- Todas as queries filtram dados nulos para garantir integridade
- Use `LIMIT` para testes rápidos
- A Query 4 usa `INNER JOIN` (só retorna registros que existem em todas as tabelas)
- Para ver mais resultados, aumente o `LIMIT`
