# RELATÓRIO TÉCNICO INTERNO - Data Warehouse Credits Brasil
**Data:** 26/11/2025
**Versão:** 2.1
**Status:** Produção

---

## 1. VISÃO GERAL DO PROJETO

### 1.1 Arquitetura

O projeto implementa um **Data Warehouse com arquitetura Medallion** (Bronze → Silver → Gold) para processamento de dados financeiros da Credits Brasil.

**Camadas:**
- **Bronze**: Dados brutos validados (raw data com validação rigorosa)
- **Silver**: Dados curados e modelados (Star Schema com SCD Type 2)
- **Gold**: *(Não implementado ainda)* - Agregações e métricas de negócio

**Tecnologias:**
- PostgreSQL 15 (Azure Database)
- Python 3.10+ (Pandas, psycopg2)
- Docker Compose (orquestração ETL)
- Git/GitHub (controle de versão)

---

## 2. FLUXO DE DADOS

### 2.1 Pipeline Bronze (Ingestão)

```
CSV Files → Validação Rigorosa → Bronze Tables → Auditoria
                ↓ (rejeitados)
           Logs de Rejeição
```

**Processo:**
1. **Leitura CSV** (`/app/data/input/onedrive/`)
2. **Validação linha a linha** (todos os campos conforme regras)
3. **Rejeição imediata** de registros inválidos
4. **Inserção no Bronze** (TRUNCATE/RELOAD)
5. **Arquivamento** do arquivo processado (`/app/data/processed/`)
6. **Registro de auditoria** (`auditoria.historico_execucao`)

**Comando:**
```bash
docker compose exec etl-processor python python/run_bronze_ingestors.py
```

### 2.2 Pipeline Silver (Transformação)

```
Bronze → Extração → Transformação → Validação → Carga Silver
                                                      ↓
                                               SCD Type 2 (dimensões)
```

**Processo:**
1. **Extração** da Bronze
2. **Transformações de negócio** (padronização, cálculos, enriquecimento)
3. **Validação de qualidade**
4. **Processamento SCD Type 2** (dimensões)
5. **Carga no Silver**

**Comando:**
```bash
docker compose exec etl-processor python python/run_silver_transformers.py
```

---

## 3. REGRAS DE NEGÓCIO

### 3.1 Validação na Bronze (CRÍTICO)

**Princípio:** Bronze **NUNCA aceita dados inválidos**. Todos os registros são validados antes da inserção.

**Tipos de validação:**
- **Obrigatoriedade**: Campos required não podem ser nulos/vazios
- **Tipo de dado**: String, int, float, decimal, data, email, CNPJ/CPF
- **Domínio**: Valores permitidos (ex: moeda em ['BRL', 'USD', 'EUR'])
- **Formato**: Datas (YYYY-MM-DD), emails (regex)
- **Range**: Números positivos, não-negativos, min/max
- **Integridade**: CNPJ/CPF com dígitos verificadores corretos

### 3.2 Transformações na Silver

**CNPJ/CPF:**
- `cnpj_cpf_nk`: Somente números (11 ou 14 dígitos) - chave natural
- `cnpj_cpf_formatado`: Formatado (00.000.000/0000-00 ou 000.000.000-00)

**Dimensão Cliente (dim_cliente):**
- `porte_empresa`: Calculado (NAO_CALCULADO por enquanto)
- `categoria_risco`: Avaliado (NAO_AVALIADO por enquanto)
- `tempo_cliente_dias`: Dias desde `data_criacao` até hoje

**Dimensão Usuário (dim_usuario):**
- `gestor_sk`: FK auto-referenciada (hierarquia de gestores)
- `nivel_hierarquia`: 1 (é gestor), 2 (tem gestor), 3 (sem gestor)
- `status_usuario`: Sempre 'ATIVO' na ingestão inicial

**Fato Faturamento (fato_faturamento):**
- `valor_liquido` = `valor_bruto` - `valor_desconto`
- `valor_imposto` = `valor_bruto` * 0.15 (15%)
- `valor_comissao` = `valor_bruto` * 0.05 (5%)
- `hash_transacao`: MD5 para idempotência

---

## 4. CAMPOS OBRIGATÓRIOS

### 4.1 Bronze

**bronze.contas:**
- `cnpj_cpf` (CNPJ/CPF válido com dígitos verificadores)
- `tipo` (PJ ou PF)
- `status` (domínio: ATIVO, INATIVO, SUSPENSO)
- `data_criacao` (formato YYYY-MM-DD)

**bronze.usuarios:**
- `nome`
- `nome_empresa`
- `email` (formato válido)
- `canal_1` (canal principal)

**bronze.faturamentos:**
- `data` (formato YYYY-MM-DD)
- `receita` (decimal positivo)
- `moeda` (domínio: BRL, USD, EUR)
- `cnpj_cliente` (CNPJ/CPF válido)
- `email_usuario` (formato válido)

### 4.2 Silver

**Chaves Naturais (não-nulas):**
- `dim_cliente.cnpj_cpf_nk`
- `dim_usuario.usuario_nk`
- `dim_data.data_completa`
- `fato_faturamento.data_sk`

**Campos NOT NULL:**
- Todas as surrogate keys (SKs)
- Valores monetários principais
- Campos de SCD Type 2 (data_inicio, flag_ativo, versao)

---

## 5. TRATAMENTO DE CNPJ/CPF

### 5.1 Validação (Bronze)

**Função:** `validar_cnpj_cpf()` em `utils/validators.py`

**Regras:**
1. Remover caracteres não-numéricos
2. Verificar tamanho (11 para CPF, 14 para CNPJ)
3. Rejeitar dígitos repetidos (ex: 11111111111111)
4. Calcular e validar dígitos verificadores
5. Rejeitar se inválido

**Exemplo de rejeição:**
```
CNPJ: 12345678000195 → REJEITADO (dígito verificador incorreto)
Motivo: "CNPJ inválido (dígito verificador incorreto): 12345678000195"
```

### 5.2 Formatação (Silver)

**Função:** `_formatar_cnpj_cpf()` em `transform_dim_cliente.py`

**Saída:**
- CNPJ: `11.222.333/0001-81`
- CPF: `123.456.789-01`

**Campos gerados:**
- `cnpj_cpf_nk`: `11222333000181` (só números, chave natural)
- `cnpj_cpf_formatado`: `11.222.333/0001-81` (formatado)

---

## 6. GERAÇÃO DE SKs/PKs/FKs

### 6.1 Surrogate Keys (SKs)

**Geração automática** via sequências PostgreSQL:

```sql
-- Dimensões
cliente_sk    SERIAL PRIMARY KEY
usuario_sk    SERIAL PRIMARY KEY
data_sk       INT PRIMARY KEY (formato: YYYYMMDD, ex: 20240115)

-- Fatos
faturamento_sk BIGSERIAL PRIMARY KEY
```

**Nomenclatura padrão:** `<entidade>_sk`

### 6.2 Natural Keys (NKs)

**Chaves de negócio** (unique business identifiers):
- `dim_cliente.cnpj_cpf_nk` (CNPJ/CPF limpo)
- `dim_usuario.usuario_nk` (email ou nome)
- `dim_data.data_completa` (data)

### 6.3 Foreign Keys (FKs)

**Fato Faturamento:**
```sql
cliente_sk → dim_cliente(cliente_sk)
usuario_sk → dim_usuario(usuario_sk)
data_sk    → dim_data(data_sk)
canal_sk   → dim_canal(sk_canal) -- nullable (dimensão não implementada)
```

**Hierarquia (self-join):**
```sql
dim_usuario.gestor_sk → dim_usuario.usuario_sk
```

**Validação:** Todas as FKs são resolvidas via `LEFT JOIN` durante transformação. Valores órfãos resultam em NULL.

---

## 7. LOGS DE REJEIÇÃO

### 7.1 Estrutura

**Tabela:** `auditoria.log_rejeicao`

**Campos:**
- `id`: BIGSERIAL (PK)
- `execucao_fk`: UUID (FK → auditoria.historico_execucao)
- `script_nome`: Nome do ingestor (ex: ingest_contas.py)
- `tabela_destino`: Tabela Bronze (ex: bronze.contas)
- `numero_linha`: Linha no CSV
- `campo_falha`: Campo que falhou validação
- `motivo_rejeicao`: Mensagem detalhada do erro
- `valor_recebido`: Valor que causou a falha
- `registro_completo`: JSON completo da linha rejeitada
- `data_rejeicao`: Timestamp (auto)
- `severidade`: WARNING, ERROR ou CRITICAL

### 7.2 Exemplo de Rejeição

```json
{
  "id": 123,
  "execucao_fk": "550e8400-e29b-41d4-a716-446655440000",
  "script_nome": "ingest_contas.py",
  "tabela_destino": "bronze.contas",
  "numero_linha": 15,
  "campo_falha": "cnpj_cpf",
  "motivo_rejeicao": "CNPJ inválido (dígito verificador incorreto): 12345678000195",
  "valor_recebido": "12345678000195",
  "registro_completo": {"cnpj_cpf": "12345678000195", "tipo": "PJ", ...},
  "data_rejeicao": "2025-11-26T23:00:00Z",
  "severidade": "ERROR"
}
```

### 7.3 Consultas Úteis

**Rejeições por campo:**
```sql
SELECT campo_falha, COUNT(*) as total
FROM auditoria.log_rejeicao
WHERE script_nome = 'ingest_contas.py'
GROUP BY campo_falha
ORDER BY total DESC;
```

**Taxa de rejeição:**
```sql
SELECT
    h.script_nome,
    h.linhas_processadas,
    COUNT(l.id) as rejeitadas,
    ROUND(COUNT(l.id)::numeric / NULLIF(h.linhas_processadas, 0) * 100, 2) as taxa_pct
FROM auditoria.historico_execucao h
LEFT JOIN auditoria.log_rejeicao l ON l.execucao_fk = h.id
WHERE h.data_inicio >= NOW() - INTERVAL '7 days'
GROUP BY h.id, h.script_nome, h.linhas_processadas;
```

---

## 8. SCD TYPE 2 (Slowly Changing Dimensions)

### 8.1 Implementação

**Dimensões com SCD2:**
- `dim_cliente`
- `dim_usuario`

**Campos de controle:**
- `data_inicio`: Data de início da versão
- `data_fim`: Data de fim da versão (NULL se ativo)
- `flag_ativo`: TRUE para versão atual, FALSE para históricas
- `versao`: Número da versão (1, 2, 3, ...)
- `hash_registro`: MD5 para detectar mudanças
- `motivo_mudanca`: Opcional (não implementado)

### 8.2 Processo

1. **Calcular hash** dos campos de negócio
2. **Comparar** hash novo vs hash atual
3. **Detectar mudanças**:
   - Sem mudança → Nenhuma ação
   - Com mudança → Fechar versão antiga + Criar nova versão
4. **Fechar versão antiga**:
   ```sql
   UPDATE dim_cliente
   SET data_fim = CURRENT_DATE - 1, flag_ativo = FALSE
   WHERE cnpj_cpf_nk = '11222333000181' AND flag_ativo = TRUE;
   ```
5. **Inserir nova versão**:
   ```sql
   INSERT INTO dim_cliente (cnpj_cpf_nk, ..., versao, flag_ativo)
   VALUES ('11222333000181', ..., 2, TRUE);
   ```

### 8.3 Exemplo

**Situação:** Cliente Alpha muda de status

| cliente_sk | cnpj_cpf_nk | razao_social | status | versao | flag_ativo | data_inicio | data_fim |
|------------|-------------|--------------|--------|--------|------------|-------------|----------|
| 1 | 11222333000181 | Alpha Tech | Ativo | 1 | FALSE | 2025-01-15 | 2025-11-25 |
| 12 | 11222333000181 | Alpha Tech | Inativo | 2 | TRUE | 2025-11-26 | NULL |

---

## 9. ESQUEMA DE PERMISSÕES

### 9.1 Roles Existentes

**Service Account:**
- `creditsdw`: Conta de serviço ETL (owner, createdb)

**Usuários de negócio:**
- `bruno.pires@creditsbrasil.com.br` / `bruno_pires_pg`
- `joao.viveiros@creditsbrasil.com.br` / `joao_viveiros_pg`
- `maria.rodrigues@creditsbrasil.com.br` / `maria_rodrigues_pg`

**Grupos de permissão:**
- `dw_admin`: Administradores (NOLOGIN)
- `dw_developer`: Desenvolvedores (NOLOGIN)
- `dw_reader`: Leitores (NOLOGIN)

### 9.2 Permissões Aplicadas

**Schema bronze:**
- Bruno, João, Maria: SELECT, INSERT, UPDATE, DELETE

**Schema silver:**
- Bruno, João, Maria: SELECT, INSERT, UPDATE, DELETE
- Sequences: USAGE

**Schema auditoria:**
- Bruno, João, Maria: SELECT (somente leitura)

### 9.3 Comandos de Verificação

```sql
-- Ver permissões de um usuário
SELECT * FROM information_schema.role_table_grants
WHERE grantee = 'bruno.pires@creditsbrasil.com.br';

-- Ver roles de um usuário
SELECT * FROM pg_roles WHERE rolname LIKE '%bruno%';
```

---

## 10. MELHORIAS PENDENTES

### 10.1 Funcionalidades

- [ ] **Camada Gold**: Agregações e métricas de negócio
- [ ] **dim_canal**: Implementar dimensão de canais
- [ ] **Cálculo de porte_empresa**: Lógica baseada em faturamento
- [ ] **Categoria_risco**: Sistema de scoring
- [ ] **Testes unitários**: Cobertura > 80%
- [ ] **CI/CD**: GitHub Actions para deploy automático
- [ ] **Monitoramento**: Alertas de falhas e SLA

### 10.2 Performance

- [ ] **Índices adicionais**: dim_cliente (status), dim_usuario (area)
- [ ] **Particionamento**: fato_faturamento por data
- [ ] **Materialização**: Views agregadas no Gold
- [ ] **Vacuum/Analyze**: Manutenção automática

### 10.3 Qualidade

- [ ] **Data quality checks**: Anomalias em valores (outliers)
- [ ] **Reconciliação**: Bronze vs Silver (contadores)
- [ ] **Documentação de campos**: Data dictionary
- [ ] **Lineage tracking**: Rastreamento de dados

### 10.4 Segurança

- [ ] **Criptografia**: Dados sensíveis (PII)
- [ ] **Auditoria de acesso**: Log de queries
- [ ] **Row-level security**: Políticas por usuário
- [ ] **Backup automatizado**: Snapshots diários

---

## 11. ESTRUTURA DE DIRETÓRIOS

```
credits-dw/
├── docker/
│   ├── docker-compose.yml       # Orquestração
│   ├── Dockerfile               # Imagem ETL
│   └── data/
│       ├── input/onedrive/      # CSVs de entrada
│       ├── processed/           # CSVs processados (arquivados)
│       └── templates/           # Templates de exemplo
├── python/
│   ├── ingestors/
│   │   └── csv/
│   │       ├── base_csv_ingestor.py      # Classe base (Template Method)
│   │       ├── ingest_contas.py          # Ingestor de contas
│   │       ├── ingest_usuarios.py        # Ingestor de usuários
│   │       ├── ingest_faturamentos.py    # Ingestor de faturamentos
│   │       └── ingest_calendario.py      # Ingestor de datas
│   ├── transformers/
│   │   ├── base_transformer.py           # Classe base Silver
│   │   └── silver/
│   │       ├── transform_dim_data.py
│   │       ├── transform_dim_cliente.py
│   │       ├── transform_dim_usuario.py
│   │       └── transform_fato_faturamento.py
│   ├── utils/
│   │   ├── db_connection.py              # Conexões PostgreSQL
│   │   ├── logger.py                     # Sistema de logs
│   │   ├── audit.py                      # Auditoria de execuções
│   │   ├── validators.py                 # Funções de validação
│   │   └── rejection_logger.py           # Logs de rejeição
│   ├── run_bronze_ingestors.py           # Runner Bronze
│   └── run_silver_transformers.py        # Runner Silver
├── tests/                                # Testes unitários
├── README.md                             # Documentação principal
├── CLAUDE.md                             # Instruções para Claude Code
├── GEMINI.md                             # Histórico Gemini
└── RELATORIO_TECNICO_INTERNO.md          # Este documento
```

---

## 12. COMANDOS RÁPIDOS

### 12.1 Docker

```bash
# Subir ambiente
cd docker && docker compose up -d --build

# Parar ambiente
docker compose down

# Ver logs
docker compose logs -f etl-processor

# Entrar no container
docker compose exec etl-processor bash
```

### 12.2 ETL

```bash
# Bronze (ingestão CSV)
docker compose exec etl-processor python python/run_bronze_ingestors.py

# Silver (transformações)
docker compose exec etl-processor python python/run_silver_transformers.py

# Ingestor individual
docker compose exec etl-processor python python/ingestors/csv/ingest_contas.py
```

### 12.3 Banco de Dados

```bash
# Conectar via psql
PGPASSWORD='58230925AD@' psql -h creditsdw.postgres.database.azure.com -U creditsdw -d creditsdw

# Contagens rápidas
psql -c "SELECT COUNT(*) FROM bronze.contas"
psql -c "SELECT COUNT(*) FROM silver.dim_cliente WHERE flag_ativo = TRUE"

# Verificar rejeições
psql -c "SELECT COUNT(*) FROM auditoria.log_rejeicao WHERE data_rejeicao::date = CURRENT_DATE"
```

---

## 13. TROUBLESHOOTING

### 13.1 Problemas Comuns

**1. Rejeições em massa (>80%)**
- **Causa**: CSV com formato incorreto ou dados inválidos
- **Solução**: Verificar template em `data/templates/`, validar domínios

**2. FK constraint violation na Silver**
- **Causa**: Dimensão não carregada antes da fact
- **Solução**: Executar `run_silver_transformers.py` (ordem correta: dim_data → dim_cliente → dim_usuario → fato)

**3. Duplicate key error (UNIQUE constraint)**
- **Causa**: SCD Type 2 gerando versão duplicada
- **Solução**: Verificar hash_registro, limpar registros antigos

**4. Slow performance**
- **Causa**: Índices faltando ou estatísticas desatualizadas
- **Solução**: `VACUUM ANALYZE`, criar índices adicionais

### 13.2 Logs

**Localização:** `/app/logs/` no container

**Arquivos principais:**
- `ingest_contas.py.log`
- `transform_dim_cliente.py.log`
- `run_bronze_ingestors.py.log`
- `run_silver_transformers.py.log`

**Ver logs em tempo real:**
```bash
docker compose exec etl-processor tail -f /app/logs/<script>.log
```

---

## 14. CONTATOS E MANUTENÇÃO

**Time de Dados:**
- Bruno Pires: bruno.pires@creditsbrasil.com.br
- João Viveiros: joao.viveiros@creditsbrasil.com.br
- Maria Rodrigues: maria.rodrigues@creditsbrasil.com.br

**Repositório:** https://github.com/brunocredits/credits-dw

**Última atualização:** 26/11/2025
**Responsável pela documentação:** Claude Code (Anthropic)
