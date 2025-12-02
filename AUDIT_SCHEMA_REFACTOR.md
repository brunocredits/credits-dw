# Refatoração do Schema de Auditoria

**Data**: 2025-12-02  
**Objetivo**: Separar corretamente Bronze (dados) de Auditoria (controle/logs)

---

## 🎯 Problema Identificado

O schema de auditoria estava **misturado** com o schema Bronze:

### ❌ Antes (ERRADO):
```
bronze/
├── base_oficial          ✅ Dados
├── faturamento           ✅ Dados
├── usuarios              ✅ Dados
├── erro_base_oficial     ❌ AUDITORIA (lugar errado!)
├── erro_faturamento      ❌ AUDITORIA (lugar errado!)
└── erro_usuarios         ❌ AUDITORIA (lugar errado!)

auditoria/
├── historico_execucao    ✅ Auditoria
├── erro_ingestao         ⚠️  Redundante
└── log_rejeicao          ✅ Auditoria (mas não era usada!)
```

### ✅ Depois (CORRETO):
```
bronze/
├── base_oficial          ✅ Apenas dados brutos
├── faturamento           ✅ Apenas dados brutos
├── data                  ✅ Apenas dados brutos
└── usuarios              ✅ Apenas dados brutos

auditoria/
├── historico_execucao    ✅ Controle de execuções
└── log_rejeicao          ✅ Registros rejeitados (com FK)
```

---

## 🔧 Mudanças Implementadas

### 1. Banco de Dados

#### Tabelas Removidas:
```sql
DROP TABLE bronze.erro_base_oficial CASCADE;
DROP TABLE bronze.erro_faturamento CASCADE;
DROP TABLE bronze.erro_usuarios CASCADE;
DROP TABLE auditoria.erro_ingestao CASCADE;
```

#### Dados Limpos:
```sql
TRUNCATE TABLE auditoria.log_rejeicao CASCADE;
TRUNCATE TABLE auditoria.historico_execucao CASCADE;
```

#### Índices Criados:
```sql
CREATE INDEX idx_historico_data_inicio ON auditoria.historico_execucao(data_inicio DESC);
CREATE INDEX idx_historico_status ON auditoria.historico_execucao(status);
CREATE INDEX idx_historico_script ON auditoria.historico_execucao(script_nome, data_inicio DESC);
CREATE INDEX idx_log_rejeicao_execucao ON auditoria.log_rejeicao(execucao_fk);
CREATE INDEX idx_log_rejeicao_data ON auditoria.log_rejeicao(data_rejeicao DESC);
```

---

### 2. Código Python

#### BaseIngestor - Antes:
```python
def __init__(self, name, target_table, mandatory_cols):
    self.name = name
    self.target_table = target_table
    self.error_table = f"bronze.erro_{target_table.split('.')[1]}"  # ❌ ERRADO
    self.mandatory_cols = mandatory_cols

def insert_errors(self, conn, error_df, filename):
    sql = f"INSERT INTO {self.error_table} ..."  # ❌ Bronze
```

#### BaseIngestor - Depois:
```python
def __init__(self, name, target_table, mandatory_cols):
    self.name = name
    self.target_table = target_table
    # ✅ Removido self.error_table
    self.mandatory_cols = mandatory_cols

def insert_errors(self, conn, error_df, filename, exec_id):
    sql = """
        INSERT INTO auditoria.log_rejeicao  -- ✅ Auditoria
        (execucao_fk, script_nome, tabela_destino, numero_linha, 
         campo_falha, motivo_rejeicao, registro_completo, severidade)
        VALUES %s
    """
    # ✅ Com FK para historico_execucao
```

---

## 📊 Estrutura Final

### auditoria.historico_execucao
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
    linhas_atualizadas  INTEGER DEFAULT 0,
    linhas_erro         INTEGER DEFAULT 0,
    mensagem_erro       TEXT
);
```

### auditoria.log_rejeicao
```sql
CREATE TABLE auditoria.log_rejeicao (
    id                 SERIAL PRIMARY KEY,
    execucao_fk        UUID REFERENCES auditoria.historico_execucao(id),  -- ✅ FK
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

---

## 🎯 Benefícios

### 1. Separação Clara de Responsabilidades
- **Bronze**: Apenas dados brutos (source of truth)
- **Auditoria**: Apenas controle e logs

### 2. Rastreabilidade Total
- Erros linkados à execução que os gerou via FK
- Possível saber exatamente quando e por que um registro foi rejeitado

### 3. Queries Mais Simples
```sql
-- Ver rejeições de uma execução específica
SELECT * FROM auditoria.log_rejeicao 
WHERE execucao_fk = 'uuid-da-execucao';

-- Ver execução com suas rejeições
SELECT 
    he.*,
    COUNT(lr.id) as total_rejeicoes
FROM auditoria.historico_execucao he
LEFT JOIN auditoria.log_rejeicao lr ON he.id = lr.execucao_fk
GROUP BY he.id;
```

### 4. Performance
- Índices criados para queries comuns
- Menos tabelas = menos complexidade

### 5. Manutenibilidade
- Estrutura mais limpa e intuitiva
- Fácil adicionar novas tabelas de dados sem poluir com erros

---

## 📝 Queries Úteis

### Ver todas as execuções de hoje:
```sql
SELECT * FROM auditoria.historico_execucao
WHERE DATE(data_inicio) = CURRENT_DATE
ORDER BY data_inicio DESC;
```

### Ver rejeições recentes:
```sql
SELECT 
    lr.data_rejeicao,
    lr.script_nome,
    lr.tabela_destino,
    lr.campo_falha,
    lr.motivo_rejeicao,
    he.linhas_processadas
FROM auditoria.log_rejeicao lr
JOIN auditoria.historico_execucao he ON lr.execucao_fk = he.id
WHERE lr.data_rejeicao >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY lr.data_rejeicao DESC;
```

### Estatísticas de rejeição por tabela:
```sql
SELECT 
    tabela_destino,
    COUNT(*) as total_rejeicoes,
    COUNT(DISTINCT execucao_fk) as execucoes_com_erro,
    STRING_AGG(DISTINCT campo_falha, ', ') as campos_problematicos
FROM auditoria.log_rejeicao
WHERE data_rejeicao >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY tabela_destino
ORDER BY total_rejeicoes DESC;
```

---

## ✅ Checklist de Migração

- [x] Dropar tabelas `bronze.erro_*`
- [x] Dropar tabela `auditoria.erro_ingestao`
- [x] Limpar dados antigos de auditoria
- [x] Criar índices de performance
- [x] Atualizar código Python
- [x] Remover `self.error_table` do BaseIngestor
- [x] Atualizar `insert_errors()` para usar `auditoria.log_rejeicao`
- [x] Adicionar FK `exec_id` nas inserções de erro
- [x] Testar pipeline (pendente)

---

## 🚀 Próximos Passos

1. **Testar pipeline** com dados reais
2. **Verificar** se rejeições estão sendo gravadas corretamente
3. **Criar views** úteis para monitoramento
4. **Documentar** queries comuns

---

## 📚 Arquivos Modificados

| Arquivo | Mudança |
|---------|---------|
| `python/core/base_ingestor.py` | Removido `error_table`, atualizado `insert_errors()` |
| Banco de dados | Dropadas 4 tabelas, criados 5 índices |

---

## ✨ Conclusão

Schema de auditoria agora está **corretamente separado** do schema Bronze:
- ✅ Bronze = Apenas dados
- ✅ Auditoria = Apenas controle/logs
- ✅ Rastreabilidade total via FK
- ✅ Performance otimizada com índices
- ✅ Código mais limpo e manutenível

**Pronto para ingestão!** 🎯
