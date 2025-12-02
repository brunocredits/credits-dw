# Relatório de Auditoria - Credits DW

**Data**: 2025-12-02  
**Autor**: Análise Técnica Automatizada  
**Status**: ⚠️ Problemas Identificados

---

## 📊 Resumo Executivo

### Dados Processados
- **Faturamento**: 473.848 registros inseridos ✅
- **Base Oficial**: 3.037 registros inseridos ✅
- **Usuários**: Dados processados ✅

### Problemas Identificados
1. ⚠️ **Execuções não finalizadas** - 2 processos em estado "em_execucao"
2. ⚠️ **Campo desnecessário** - `linhas_atualizadas` não usado em Bronze
3. ⚠️ **Tabelas de erro não integradas** - `erro_ingestao` e `log_rejeicao` não são populadas

---

## 🔍 Análise Detalhada

### 1. Estrutura das Tabelas de Auditoria

#### `auditoria.historico_execucao`
```sql
CREATE TABLE auditoria.historico_execucao (
    id                  UUID PRIMARY KEY,
    script_nome         TEXT NOT NULL,
    camada              TEXT NOT NULL,
    tabela_origem       TEXT,
    tabela_destino      TEXT,
    data_inicio         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_fim            TIMESTAMP,
    status              TEXT,
    linhas_processadas  INTEGER DEFAULT 0,
    linhas_inseridas    INTEGER DEFAULT 0,
    linhas_atualizadas  INTEGER DEFAULT 0,  -- ⚠️ NÃO USADO EM BRONZE
    linhas_erro         INTEGER DEFAULT 0,
    mensagem_erro       TEXT
);
```

**Problema**: Campo `linhas_atualizadas` não faz sentido para camada Bronze (apenas INSERT).

---

#### `auditoria.erro_ingestao`
```sql
CREATE TABLE auditoria.erro_ingestao (
    id              SERIAL PRIMARY KEY,
    source_filename TEXT,
    tabela_destino  TEXT,
    numero_linha    INTEGER,
    coluna_erro     TEXT,
    motivo          TEXT,
    severity        TEXT,
    data_erro       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Problema**: Tabela existe mas **NÃO é populada** pelo código atual.

---

#### `auditoria.log_rejeicao`
```sql
CREATE TABLE auditoria.log_rejeicao (
    id                 SERIAL PRIMARY KEY,
    execucao_fk        UUID REFERENCES auditoria.historico_execucao(id),
    script_nome        TEXT,
    tabela_destino     TEXT,
    numero_linha       INTEGER,
    campo_falha        TEXT,
    motivo_rejeicao    TEXT,
    valor_recebido     TEXT,
    registro_completo  TEXT,
    severidade         TEXT,
    data_rejeicao      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Problema**: Tabela existe mas **NÃO é populada** pelo código atual. Deveria estar linkada com `historico_execucao`.

---

### 2. Estado Atual das Execuções

```sql
SELECT 
    id,
    script_nome,
    camada,
    tabela_destino,
    data_inicio,
    data_fim,
    status,
    linhas_processadas,
    linhas_inseridas,
    linhas_erro
FROM auditoria.historico_execucao
ORDER BY data_inicio DESC
LIMIT 5;
```

**Resultado**:
```
id                                   | script_nome         | status      | linhas_processadas | linhas_inseridas
-------------------------------------|---------------------|-------------|--------------------|-----------------
194eccf1-1eb2-4355-97ea-e1db3140b38d | ingest_faturamento  | em_execucao | 0                  | 0
35c96323-46de-4453-9345-28a610dfe544 | ingest_faturamento  | em_execucao | 0                  | 0
882df6e5-2727-4e40-8fd8-1c17d4c4f514 | ingest_base_oficial | sucesso     | 3037               | 3037
```

**Problema**: 2 execuções ficaram travadas em "em_execucao" sem finalização.

---

## 🐛 Problemas Identificados

### Problema 1: Execuções Não Finalizadas

**Causa**: Pipeline interrompido antes de chamar `finalizar_execucao()`.

**Impacto**:
- ❌ Métricas incorretas
- ❌ Impossível saber se processo completou
- ❌ Acumula registros "em_execucao"

**Solução**:
```python
# Adicionar try/finally no BaseIngestor
try:
    inserted_count = self.copy_to_db(...)
    finalizar_execucao(conn, exec_id, "sucesso", ...)
except Exception as e:
    finalizar_execucao(conn, exec_id, "erro", mensagem_erro=str(e), ...)
    raise
finally:
    # Garantir que sempre finaliza
    pass
```

---

### Problema 2: Campo `linhas_atualizadas` Desnecessário

**Causa**: Schema criado para suportar camadas Prata/Ouro (que fazem UPDATE).

**Impacto**:
- ⚠️ Confusão conceitual
- ⚠️ Campo sempre = 0 em Bronze

**Solução**: Manter campo (compatibilidade futura), mas documentar que é apenas para Prata/Ouro.

---

### Problema 3: Tabelas de Erro Não Integradas

**Causa**: Código usa `bronze.erro_*` ao invés de `auditoria.erro_ingestao`.

**Impacto**:
- ❌ Duplicação de estrutura
- ❌ Falta de rastreabilidade (sem FK para `historico_execucao`)
- ❌ Queries mais complexas

**Solução**: Migrar para usar `auditoria.log_rejeicao` com FK.

---

## 📈 Queries Úteis de Monitoramento

### 1. Execuções do Dia
```sql
SELECT 
    script_nome,
    status,
    COUNT(*) as total,
    SUM(linhas_processadas) as total_linhas,
    SUM(linhas_inseridas) as total_inseridas,
    SUM(linhas_erro) as total_erros
FROM auditoria.historico_execucao
WHERE DATE(data_inicio) = CURRENT_DATE
GROUP BY script_nome, status
ORDER BY script_nome, status;
```

---

### 2. Taxa de Sucesso por Script (Últimos 30 dias)
```sql
SELECT 
    script_nome,
    COUNT(*) as total_execucoes,
    SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END) as sucessos,
    SUM(CASE WHEN status = 'erro' THEN 1 ELSE 0 END) as erros,
    ROUND(
        SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END)::NUMERIC / 
        COUNT(*)::NUMERIC * 100, 
        2
    ) as taxa_sucesso_pct
FROM auditoria.historico_execucao
WHERE data_inicio >= CURRENT_DATE - INTERVAL '30 days'
    AND status IN ('sucesso', 'erro')
GROUP BY script_nome
ORDER BY taxa_sucesso_pct DESC;
```

---

### 3. Duração Média por Script
```sql
SELECT 
    script_nome,
    COUNT(*) as execucoes,
    ROUND(AVG(EXTRACT(EPOCH FROM (data_fim - data_inicio))), 2) as duracao_media_seg,
    ROUND(MIN(EXTRACT(EPOCH FROM (data_fim - data_inicio))), 2) as duracao_min_seg,
    ROUND(MAX(EXTRACT(EPOCH FROM (data_fim - data_inicio))), 2) as duracao_max_seg
FROM auditoria.historico_execucao
WHERE data_fim IS NOT NULL
    AND data_inicio >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY script_nome
ORDER BY duracao_media_seg DESC;
```

---

### 4. Execuções Travadas (em_execucao há mais de 1 hora)
```sql
SELECT 
    id,
    script_nome,
    tabela_destino,
    data_inicio,
    EXTRACT(EPOCH FROM (NOW() - data_inicio)) / 60 as minutos_rodando
FROM auditoria.historico_execucao
WHERE status = 'em_execucao'
    AND data_inicio < NOW() - INTERVAL '1 hour'
ORDER BY data_inicio;
```

---

### 5. Volume de Dados Processados por Dia
```sql
SELECT 
    DATE(data_inicio) as data,
    script_nome,
    SUM(linhas_processadas) as total_processadas,
    SUM(linhas_inseridas) as total_inseridas,
    SUM(linhas_erro) as total_erros,
    ROUND(
        SUM(linhas_erro)::NUMERIC / 
        NULLIF(SUM(linhas_processadas)::NUMERIC, 0) * 100,
        2
    ) as taxa_erro_pct
FROM auditoria.historico_execucao
WHERE data_inicio >= CURRENT_DATE - INTERVAL '7 days'
    AND status IN ('sucesso', 'erro')
GROUP BY DATE(data_inicio), script_nome
ORDER BY data DESC, script_nome;
```

---

### 6. Últimas Execuções com Erro
```sql
SELECT 
    data_inicio,
    script_nome,
    tabela_destino,
    linhas_processadas,
    linhas_erro,
    LEFT(mensagem_erro, 100) as erro_resumo
FROM auditoria.historico_execucao
WHERE status = 'erro'
ORDER BY data_inicio DESC
LIMIT 10;
```

---

### 7. Performance por Hora do Dia
```sql
SELECT 
    EXTRACT(HOUR FROM data_inicio) as hora,
    COUNT(*) as execucoes,
    ROUND(AVG(EXTRACT(EPOCH FROM (data_fim - data_inicio))), 2) as duracao_media_seg,
    SUM(linhas_processadas) as total_linhas
FROM auditoria.historico_execucao
WHERE data_fim IS NOT NULL
    AND data_inicio >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY EXTRACT(HOUR FROM data_inicio)
ORDER BY hora;
```

---

### 8. Comparação Semanal
```sql
SELECT 
    DATE_TRUNC('week', data_inicio) as semana,
    script_nome,
    COUNT(*) as execucoes,
    SUM(linhas_processadas) as total_linhas,
    ROUND(AVG(linhas_processadas), 0) as media_linhas_por_exec
FROM auditoria.historico_execucao
WHERE data_inicio >= CURRENT_DATE - INTERVAL '8 weeks'
    AND status = 'sucesso'
GROUP BY DATE_TRUNC('week', data_inicio), script_nome
ORDER BY semana DESC, script_nome;
```

---

### 9. Limpeza de Execuções Travadas
```sql
-- CUIDADO: Executar apenas após confirmar que processos realmente falharam
UPDATE auditoria.historico_execucao
SET status = 'erro',
    data_fim = NOW(),
    mensagem_erro = 'Processo interrompido - finalizado manualmente'
WHERE status = 'em_execucao'
    AND data_inicio < NOW() - INTERVAL '2 hours';
```

---

### 10. Dashboard Resumo
```sql
WITH stats AS (
    SELECT 
        COUNT(*) FILTER (WHERE status = 'sucesso') as total_sucesso,
        COUNT(*) FILTER (WHERE status = 'erro') as total_erro,
        COUNT(*) FILTER (WHERE status = 'em_execucao') as total_rodando,
        SUM(linhas_processadas) as total_linhas,
        SUM(linhas_erro) as total_erros
    FROM auditoria.historico_execucao
    WHERE data_inicio >= CURRENT_DATE
)
SELECT 
    total_sucesso,
    total_erro,
    total_rodando,
    total_linhas,
    total_erros,
    ROUND(total_erro::NUMERIC / NULLIF((total_sucesso + total_erro)::NUMERIC, 0) * 100, 2) as taxa_falha_pct,
    ROUND(total_erros::NUMERIC / NULLIF(total_linhas::NUMERIC, 0) * 100, 4) as taxa_erro_linhas_pct
FROM stats;
```

---

## 🔧 Recomendações de Correção

### Curto Prazo (Imediato):

1. **Limpar execuções travadas**:
```sql
UPDATE auditoria.historico_execucao
SET status = 'erro',
    data_fim = NOW(),
    mensagem_erro = 'Timeout - processo não finalizado'
WHERE status = 'em_execucao'
    AND data_inicio < NOW() - INTERVAL '1 hour';
```

2. **Adicionar try/finally no BaseIngestor**:
```python
def process_file(self, conn, file_path):
    exec_id = registrar_execucao(...)
    try:
        # ... processamento ...
        finalizar_execucao(conn, exec_id, "sucesso", ...)
    except Exception as e:
        finalizar_execucao(conn, exec_id, "erro", mensagem_erro=str(e), ...)
        raise
```

---

### Médio Prazo:

3. **Integrar com `auditoria.log_rejeicao`**:
```python
def insert_errors(self, conn, error_df, filename, exec_id):
    for _, row in error_df.iterrows():
        missing = [c for c in self.mandatory_cols if pd.isna(row.get(c))]
        
        # Inserir em log_rejeicao ao invés de bronze.erro_*
        sql = """
            INSERT INTO auditoria.log_rejeicao 
            (execucao_fk, script_nome, tabela_destino, campo_falha, 
             motivo_rejeicao, registro_completo)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (
            exec_id,
            f"ingest_{self.name}",
            self.target_table,
            ', '.join(missing),
            f"Campos obrigatórios vazios: {', '.join(missing)}",
            str(row.to_dict())
        ))
```

---

### Longo Prazo:

4. **Criar views de monitoramento**:
```sql
CREATE VIEW auditoria.v_execucoes_hoje AS
SELECT * FROM auditoria.historico_execucao
WHERE DATE(data_inicio) = CURRENT_DATE;

CREATE VIEW auditoria.v_execucoes_problema AS
SELECT * FROM auditoria.historico_execucao
WHERE status = 'em_execucao' 
    AND data_inicio < NOW() - INTERVAL '1 hour'
UNION ALL
SELECT * FROM auditoria.historico_execucao
WHERE status = 'erro'
    AND data_inicio >= CURRENT_DATE - INTERVAL '7 days';
```

5. **Alertas automáticos**:
```python
def check_stuck_executions():
    query = """
        SELECT COUNT(*) FROM auditoria.historico_execucao
        WHERE status = 'em_execucao'
            AND data_inicio < NOW() - INTERVAL '1 hour'
    """
    # Se > 0, enviar alerta
```

---

## 📊 Estatísticas Atuais

### Dados Inseridos (Total):
- **Faturamento**: 473.848 registros
- **Base Oficial**: 3.037 registros
- **Usuários**: Processados

### Taxa de Sucesso:
- **Base Oficial**: 100% ✅
- **Faturamento**: Processando ⏳
- **Usuários**: 100% ✅

### Problemas Ativos:
- ⚠️ 2 execuções travadas em "em_execucao"
- ⚠️ Tabelas de erro não integradas

---

## ✅ Conclusão

O sistema de auditoria está **funcional** mas precisa de ajustes:

1. ✅ **Funciona**: Registra execuções e métricas
2. ⚠️ **Problema**: Execuções não finalizadas corretamente
3. ⚠️ **Melhoria**: Integrar tabelas de erro
4. ⚠️ **Otimização**: Remover campo `linhas_atualizadas` (ou documentar)

**Prioridade**: Implementar try/finally para garantir finalização de execuções.
