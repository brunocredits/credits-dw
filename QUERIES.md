# Consultas Analíticas - Credits DW (Bronze Layer)

Este documento contém exemplos de queries SQL para extrair insights iniciais das tabelas da camada Bronze.

## 1. Tabela: `bronze.faturamento`

### 1.1. Receita Líquida Total por Mês de Emissão
Analisa a evolução do faturamento liquido ao longo do tempo.
```sql
SELECT 
    TO_CHAR(data_emissao, 'YYYY-MM') as mes_referencia, 
    SUM(valor_liquido) as total_faturado
FROM bronze.faturamento 
WHERE data_emissao IS NOT NULL
GROUP BY 1 
ORDER BY 1 DESC;
```

### 1.2. Top 10 Clientes por Valor Faturado
Identifica os clientes mais valiosos com base no valor líquido total.
```sql
SELECT 
    cliente_nome_fantasia, 
    SUM(valor_liquido) as total_valor_liquido
FROM bronze.faturamento 
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 10;
```

### 1.3. Fluxo de Caixa: Recebido vs. A Receber
Visão geral do montante já recebido versus o que ainda está pendente.
```sql
SELECT 
    SUM(valor_recebido) as total_ja_recebido,
    SUM(valor_a_receber) as total_pendente
FROM bronze.faturamento;
```

---

## 2. Tabela: `bronze.base_oficial`

### 2.1. Distribuição de Clientes por Status
Contagem de clientes ativos, inativos, etc.
```sql
SELECT 
    status, 
    COUNT(*) as qtd_clientes
FROM bronze.base_oficial 
GROUP BY 1 
ORDER BY 2 DESC;
```

### 2.2. Segmentação de Mercado
Análise da carteira de clientes por segmento de atuação.
```sql
SELECT 
    segmento, 
    COUNT(*) as qtd_clientes
FROM bronze.base_oficial 
GROUP BY 1 
ORDER BY 2 DESC;
```

### 2.3. Maiores Grupos Empresariais
Identifica os grupos com maior número de empresas/CNPJs na base.
```sql
SELECT 
    grupo, 
    COUNT(*) as qtd_cnpjs
FROM bronze.base_oficial 
WHERE grupo IS NOT NULL AND grupo <> ''
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 5;
```

---

## 3. Tabela: `bronze.usuarios`

### 3.1. Headcount por Cargo
Quantidade de colaboradores por função.
```sql
SELECT 
    cargo, 
    COUNT(*) as qtd_usuarios
FROM bronze.usuarios 
GROUP BY 1 
ORDER BY 2 DESC;
```

### 3.2. Status dos Vendedores
Verifica quantos vendedores estão ativos ou inativos.
```sql
SELECT 
    status_vendedor, 
    COUNT(*) as total
FROM bronze.usuarios 
GROUP BY 1;
```

### 3.3. Distribuição por Nível de Senioridade
Análise da equipe por níveis (Júnior, Pleno, Sênior, etc).
```sql
SELECT 
    nivel, 
    COUNT(*) as total
FROM bronze.usuarios 
GROUP BY 1 
ORDER BY 2 DESC;
```

---

## 4. Visão Integrada (JOIN)

### 4.1. Performance de Vendas por Time e Consultor
Relaciona o faturamento com os dados do usuário (consultor) para ver a performance por time.

**Lógica do Join:** 
- `bronze.faturamento.vendedor` = `bronze.usuarios.consultor`

```sql
SELECT 
    u.time,
    u.consultor,
    u.cargo,
    COUNT(DISTINCT f.cnpj) as qtd_clientes_atendidos,
    SUM(f.valor_liquido) as total_faturado,
    SUM(f.valor_a_receber) as total_pendente
FROM bronze.faturamento f
LEFT JOIN bronze.usuarios u 
    ON f.vendedor = u.consultor
WHERE u.consultor IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 5 DESC;
```
