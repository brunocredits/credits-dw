# Índices Otimizados para Credits DW

Este documento lista os índices sugeridos para melhorar a performance de queries analíticas.

## Bronze Layer - Faturamento

```sql
-- Índice composto para queries por empresa e vendedor
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_faturamento_empresa_vendedor 
    ON bronze.faturamento(empresa, vendedor)
    WHERE empresa IS NOT NULL AND vendedor IS NOT NULL;

-- Índice composto para queries por cliente e data
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_faturamento_cnpj_data 
    ON bronze.faturamento(cnpj, data_fat)
    WHERE cnpj IS NOT NULL AND data_fat IS NOT NULL;

-- Índice para queries por tipo de documento e vencimento
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_faturamento_tipo_vencimento
    ON bronze.faturamento(tipo_documento, vencimento)
    WHERE tipo_documento IS NOT NULL AND vencimento IS NOT NULL;
```

## Bronze Layer - Base Oficial

```sql
-- Índice composto para hierarquia organizacional
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_base_oficial_empresa_grupo
    ON bronze.base_oficial(empresa, grupo)
    WHERE empresa IS NOT NULL AND grupo IS NOT NULL;

-- Índice para queries por canal
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_base_oficial_canal
    ON bronze.base_oficial(canal_1, canal_2)
    WHERE canal_1 IS NOT NULL;
```

## Bronze Layer - Usuários

```sql
-- Índice composto para hierarquia de vendedores
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usuarios_time_nivel
    ON bronze.usuarios(time, nivel)
    WHERE time IS NOT NULL AND nivel IS NOT NULL;
```

## Auditoria Layer - Log de Rejeição

```sql
-- Índice para lookup rápido por execução
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_log_rejeicao_execucao
    ON auditoria.log_rejeicao(execucao_fk)
    WHERE execucao_fk IS NOT NULL;

-- Índice para filtrar por severidade
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_log_rejeicao_severidade
    ON auditoria.log_rejeicao(severidade, data_rejeicao DESC)
    WHERE severidade IS NOT NULL;

-- Índice para análise por tabela destino
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_log_rejeicao_tabela
    ON auditoria.log_rejeicao(tabela_destino, data_rejeicao DESC)
    WHERE tabela_destino IS NOT NULL;

-- Índice composto para análise de campos problemáticos
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_log_rejeicao_campo_severidade
    ON auditoria.log_rejeicao(campo_falha, severidade)
    WHERE campo_falha IS NOT NULL;
```

## Auditoria Layer - Histórico de Execução

```sql
-- Índice composto para monitoramento de pipeline
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_historico_script_status
    ON auditoria.historico_execucao(script_nome, status, data_inicio DESC);

-- Índice para análise temporal de execuções
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_historico_data_inicio
    ON auditoria.historico_execucao(data_inicio DESC);
```

## Como Aplicar

Execute os comandos SQL diretamente no banco de dados:

```bash
psql -h creditsbrasil.postgres.database.azure.com -U creditsdw -d creditsdw
```

Ou crie um script e execute (copie os comandos acima para um arquivo .sql e execute).

**Nota**: O uso de `CONCURRENTLY` permite criar índices sem bloquear escritas, mas demora mais tempo. Remova essa palavra se preferir velocidade ao invés de disponibilidade.

## Verificar Índices Criados

```sql
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid::regclass)) as index_size
FROM pg_indexes
WHERE schemaname IN ('bronze', 'auditoria')
    AND indexname LIKE 'idx_%'
ORDER BY schemaname, tablename, indexname;
```

## Monitorar Uso de Índices

```sql
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes
WHERE schemaname IN ('bronze', 'auditoria')
ORDER BY idx_scan DESC;
```
