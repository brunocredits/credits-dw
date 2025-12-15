# Credits Brasil Data Warehouse (Credits DW)

Este projeto implementa um pipeline de dados (ETL) containerizado para ingerir dados brutos de arquivos (CSV, Excel, ODS) em um Data Warehouse PostgreSQL na camada Bronze (Raw).

## 📋 Visão Geral

O pipeline é desenvolvido em Python e orquestrado via Docker Compose. Ele suporta:
- **Ingestão Dinâmica:** Detecta automaticamente arquivos de `faturamento`, `base_oficial` e `usuarios` no diretório de input.
- **Validação de Schema:** Verifica se os arquivos de entrada correspondem aos templates esperados.
- **Limpeza de Dados:** Tratamento básico de tipos numéricos e datas.
- **Auditoria Robusta:** Histórico de execuções e logs de rejeição (`auditoria.historico_execucao` e `auditoria.log_rejeicao`) no banco de dados.
- **Rastreamento de Usuário:** Identifica automaticamente quem executou cada processo via `DB_USER` do .env.
- **Estratégia "Warn-on-Fail":** Registros com campos obrigatórios vazios são ingeridos com um aviso (WARN), enquanto erros críticos de dados rejeitam o registro (ERROR).

## 🏗️ Estrutura do Projeto

```
credits-dw/
├── docker/
│   ├── data/
│   │   ├── input/       # Coloque seus arquivos CSV/XLSX aqui
│   │   ├── processed/   # Arquivos processados são movidos para cá
│   │   └── templates/   # Templates para validação de cabeçalho
│   ├── Dockerfile
│   └── docker-compose.yml
├── python/
│   ├── core/            # Lógica base (Ingestor, Validador, Cleaner)
│   ├── ingestors/       # Classes específicas para cada tipo de arquivo
│   ├── scripts/         # Scripts executáveis (run_pipeline.py, etc)
│   └── utils/           # Utilitários (DB, Audit, Logger)
├── QUERIES.md           # Exemplos de consultas SQL
├── run_pipeline.sh      # Script facilitador para rodar o ETL
├── reset_env.sh         # Script para limpar dados e resetar tabelas
└── requirements.txt
```

## 🚀 Como Executar

### Pré-requisitos
- Docker e Docker Compose instalados.
- Arquivo `.env` configurado (copie de `.env.example`).

### 1. Configurar Variáveis de Ambiente
Crie um arquivo `.env` na raiz do projeto com as credenciais do banco:
```bash
cp .env.example .env
# Edite o .env com suas configurações
```

### 2. Colocar Arquivos de Input
Mova os arquivos que deseja processar para `docker/data/input/`.
Exemplo:
```bash
cp meus_dados/*.csv docker/data/input/
```

### 3. Rodar o Pipeline
Execute o script wrapper:
```bash
./run_pipeline.sh
```
Isso irá:
1. Buildar a imagem Docker.
2. Executar o processamento.
3. Mover os arquivos processados para `docker/data/processed/YYYY/MM/DD/`.
4. Registrar o resultado no banco de dados.

### 4. Verificar Resultados
Consulte o arquivo [QUERIES.md](QUERIES.md) para exemplos de como explorar os dados ingeridos.

Para verificar erros ou avisos de ingestão:
```sql
SELECT * FROM auditoria.log_rejeicao ORDER BY data_hora DESC LIMIT 100;
```

## 🛠️ Comandos Úteis

- **Resetar Ambiente:** Limpa tabelas bronze e arquivos processados (CUIDADO!).
  ```bash
  ./reset_env.sh
  ```

## 📝 Decisões de Arquitetura

- **Camada Bronze (Raw):** O foco é ingerir os dados com mínima transformação destrutiva.
- **Validação Flexível:** Campos obrigatórios ausentes geram alertas (`WARN`) mas não bloqueiam a ingestão, permitindo correção posterior na camada Silver.
- **Alta Performance:** Uso de `COPY FROM STDIN` do PostgreSQL para carga em massa.

## 🔍 Campos Obrigatórios por Ingestor

### Faturamento
Apenas 5 campos essenciais:
- `numero_documento`: Identificador único do documento
- `cnpj`: Cliente (essencial para joins)
- `data_fat`: Data de faturamento 
- `valor_da_conta`: Valor principal
- `empresa`: Empresa emissora (multi-tenant)

> **Nota**: Reduzido de 33 para 5 campos obrigatórios seguindo princípios da camada Bronze. Validações de negócio mais rigorosas devem ser feitas na Silver.

### Base Oficial
14 campos refletindo estrutura organizacional:
- `cnpj`, `status`, `manter_no_baseline`
- `razao_social`, `nome_fantasia`
- `canal_1`, `canal_2`
- `lider`, `responsavel`
- `empresa`, `grupo`, `corte`, `segmento`, `obs`

### Usuários
Todos os campos do template são obrigatórios para manter integridade da hierarquia de vendas, exceto `acesso_indireto` e `acesso_temporario` que são opcionais.

## 🚀 Otimizações de Performance

### Índices do Banco de Dados

O projeto inclui índices otimizados para queries analíticas comuns. Veja o arquivo [INDEXES.md](INDEXES.md) para:
- Lista completa de índices criados
- Scripts SQL para aplicar
- Instruções de monitoramento

**Principais índices:**
- `bronze.faturamento`: Índices compostos por empresa+vendedor, cnpj+data
- `bronze.base_oficial`: Índices por empresa+grupo, canais
- `auditoria.log_rejeicao`: Índices para debugging (execução, severidade, tabela)

### Estratégia de Validação (WARN vs ERROR)

- **WARN**: Campos obrigatórios vazios → Dados são inseridos, mas registrado warning
- **ERROR**: Tipos inválidos (ex: texto em campo numérico) → Linha rejeitada completamente

Isso permite máxima ingestão de dados na Bronze, com correção posterior na Silver.

## 🔧 Troubleshooting

### Problema: Muitos warnings de campos obrigatórios
**Solução**: Revise a qualidade dos dados de origem. Warnings não bloqueiam ingestão.

```sql
-- Ver campos mais problemáticos
SELECT campo_falha, COUNT(*) as total
FROM auditoria.log_rejeicao
WHERE severidade = 'WARN'
GROUP BY campo_falha
ORDER BY total DESC
LIMIT 10;
```

### Problema: Arquivo não está sendo processado
**Causas comuns:**
1. Nome do arquivo não corresponde ao padrão esperado
2. Arquivo duplicado (mesmo hash MD5)
3. Headers não correspondem ao template

```sql
-- Ver últimas execuções com erro
SELECT script_nome, tabela_destino, mensagem_erro, data_inicio
FROM auditoria.historico_execucao
WHERE status = 'erro'
ORDER BY data_inicio DESC
LIMIT 5;
```

### Problema: Performance lenta
**Soluções:**
1. Verifique se índices foram criados (veja INDEXES.md)
2. Execute `VACUUM ANALYZE` após grandes cargas
3. Considere aumentar `work_mem` do PostgreSQL para sorts grandes

```sql
-- Verificar tamanho das tabelas
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname IN ('bronze', 'auditoria')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Problema: Erro de conexão com o banco
**Checklist:**
1. Verifique arquivo `.env` está configurado
2. Confirme conectividade de rede
3. Valide credenciais do banco
4. PostgreSQL Azure requer `sslmode=require`

## 📊 Auditoria e Rastreamento

### Histórico de Execuções

Todas as execuções do pipeline são registradas automaticamente em `auditoria.historico_execucao`:

```sql
SELECT 
    script_nome,
    usuario_executante,  -- Identifica quem executou (DB_USER)
    data_inicio,
    status,
    linhas_inseridas,
    linhas_erro
FROM auditoria.historico_execucao
ORDER BY data_inicio DESC
LIMIT 20;
```

**Rastreamento automático:**
- ✅ O sistema usa `DB_USER` do .env para identificar quem executou cada processo
- ✅ Cada pessoa tem seu próprio usuário no banco, permitindo auditoria completa
- ✅ Todas as métricas (linhas processadas, inseridas, erros) são registradas

## 📊 Consultas Analíticas

Veja [QUERIES.md](QUERIES.md) para exemplos de queries usando `valor_conta` como métrica principal:
- Consultas básicas (faturamento por mês, top clientes)
- Consultas avançadas com JOINs (performance de vendedores, análise 360°)
- Queries de inadimplência e evolução de carteira

## 📚 Documentação Adicional

- [QUERIES.md](QUERIES.md) - Exemplos de consultas SQL úteis (usando valor_conta)
- [INDEXES.md](INDEXES.md) - Documentação de índices do banco
- [ACCESS.md](ACCESS.md) - Configuração de acesso ao banco

## 🎯 Próximos Passos

1. **Aplicar índices de performance** (ver [INDEXES.md](INDEXES.md))
2. **Implementar camada Silver** para transformações
3. **Criar views analíticas** para relatórios

---

**Versão**: 2.1  
**Última Atualização**: 2025-12-11  
**Mantido por**: Equipe Credits Brasil
