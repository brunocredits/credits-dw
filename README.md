# 🏦 Data Warehouse Credits Brasil

> **Versão 3.0** | **Arquitetura Bronze Layer** | **PostgreSQL 15** | **Python 3.10+**

---

## 📋 Índice

1. [Visão Geral](#-visão-geral)
2. [Arquitetura](#-arquitetura)
3. [Pré-requisitos](#-pré-requisitos)
4. [Instalação](#-instalação)
5. [Configuração](#-configuração)
6. [Uso](#-uso)
7. [Estrutura do Projeto](#-estrutura-do-projeto)
8. [Desenvolvimento](#-desenvolvimento)
9. [Melhorias da Versão 3.0](#-melhorias-da-versão-30)
10. [Troubleshooting](#-troubleshooting)
11. [Contribuição](#-contribuição)

---

## 🎯 Visão Geral

Data Warehouse moderno para consolidação de dados financeiros e operacionais da Credits Brasil. O projeto implementa um **pipeline ETL robusto** que processa arquivos CSV e os carrega em uma camada Bronze no PostgreSQL, seguindo as melhores práticas de engenharia de dados.

### ✨ Características Principais

- **🏗️ Camada Bronze**: Armazena dados brutos com mínima transformação
- **🔄 ETL Automatizado**: Scripts Python modulares e reutilizáveis
- **🐳 Containerizado**: Ambiente Docker para consistência entre dev/prod
- **📊 Auditoria Completa**: Rastreamento de todas as execuções no schema `credits`
- **⚡ Performance**: Inserções em batch com retry automático
- **🔍 Observabilidade**: Logs estruturados com Loguru e métricas detalhadas
- **🚀 Paralelização**: Suporte a execução paralela de múltiplos ingestores
- **🛡️ Segurança**: Credenciais em variáveis de ambiente, sem hardcoding

---

## 🏗️ Arquitetura

### Fluxo de Dados

```
┌─────────────────┐
│  Arquivos CSV   │  (OneDrive, SFTP, etc)
│  - Faturamento  │
│  - Usuários     │
│  - Contas       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  ETL Processor  │  (Python + Pandas)
│  - Validação    │
│  - Limpeza      │
│  - Formatação   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ PostgreSQL      │
│ ├── bronze.*    │  (Dados Brutos)
│ └── credits.*   │  (Metadados & Auditoria)
└─────────────────┘
```

### Schemas do Banco de Dados

#### `bronze` - Dados Brutos
Contém as tabelas com dados originais das fontes CSV:

| Tabela                    | Descrição                          | Atualização |
|---------------------------|------------------------------------|-------------|
| `bronze.faturamento`      | Receitas diárias por data          | Diária      |
| `bronze.usuarios`         | Cadastro de usuários e hierarquia  | Diária      |
| `bronze.contas_base_oficial` | Contas de clientes (B2B)      | Diária      |
| `bronze.data`             | Dimensão de data pré-calculada     | Uma vez     |

#### `credits` - Metadados e Auditoria
Sistema de controle e observabilidade:

| Tabela                         | Descrição                                    |
|--------------------------------|----------------------------------------------|
| `credits.historico_atualizacoes` | Log de execuções ETL com métricas e status |

### A Dimensão de Data

A tabela `bronze.data` é uma **Date Dimension Table**, técnica essencial em Data Warehousing:

**Benefícios:**
- ⚡ **Performance**: Agregações 10x mais rápidas (ex: `GROUP BY trimestre`)
- 🧩 **Simplicidade**: SQL mais limpo, sem funções complexas de data
- 📏 **Consistência**: Todos usam as mesmas definições de períodos
- 🔄 **Reusabilidade**: Uma vez criada, serve para todas as análises

**Exemplo de uso:**
```sql
-- Receita por trimestre (rápido e simples)
SELECT d.trimestre, SUM(f.receita::numeric)
FROM bronze.faturamento f
JOIN bronze.data d ON f.data = d.data_completa
GROUP BY d.trimestre
ORDER BY d.trimestre;
```

---

## 💻 Pré-requisitos

### Requisitos Mínimos

- **Docker** 20.10+ e **Docker Compose** V2
- **Python** 3.10+ (para desenvolvimento local)
- **PostgreSQL** 15+ (gerenciado externamente)
- **Cliente PostgreSQL** (DBeaver, pgAdmin, psql, etc.)
- **Git** para controle de versão

### Requisitos Recomendados

- 2 CPU cores / 2GB RAM para container ETL
- 10GB de espaço em disco para logs e arquivos processados
- Conexão de rede estável com o banco PostgreSQL

---

## 🚀 Instalação

### 1. Clonar Repositório

```bash
git clone https://github.com/brunocredits/credits-dw.git
cd credits-dw
```

### 2. Configurar Variáveis de Ambiente

Copie o template e preencha com suas credenciais:

```bash
cp .env.example .env
```

Edite o arquivo `.env`:

```properties
# Conexão PostgreSQL
DB_HOST=seu_host.postgres.database.azure.com
DB_PORT=5432
DB_NAME=creditsdw
DB_USER=seu_usuario
DB_PASSWORD=sua_senha_forte

# Configurações Opcionais
LOG_LEVEL=INFO                    # DEBUG, INFO, WARNING, ERROR
ETL_BATCH_SIZE=1000              # Tamanho do batch para inserções
ETL_PARALLEL_INGESTORS=3         # Número de ingestores em paralelo
CSV_SEPARATOR=;                  # Separador dos CSVs
CSV_ENCODING=utf-8-sig           # Encoding dos CSVs
```

**⚠️ IMPORTANTE**: Nunca commite o arquivo `.env` no Git!

### 3. Preparar Dados de Entrada

Coloque seus arquivos CSV no diretório de input:

```bash
# Estrutura esperada
docker/data/input/onedrive/
├── faturamento.csv
├── usuarios.csv
└── contas_base_oficial.csv
```

**Templates de exemplo:**
```bash
# Para testar, copie os templates
cp docker/data/templates/*.csv docker/data/input/onedrive/
```

### 4. Iniciar Container ETL

```bash
cd docker
docker compose up -d --build
```

Verificar se está rodando:
```bash
docker compose ps
# Deve mostrar: credits-dw-etl com status "Up"
```

---

## ⚙️ Configuração

### Configuração Centralizada

A versão 3.0 introduz configuração centralizada em `python/utils/config.py`:

```python
from utils.config import get_config

config = get_config()
print(f"Database: {config.database.host}")
print(f"Batch size: {config.etl.batch_insert_size}")
```

### Variáveis de Ambiente Disponíveis

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `DB_HOST` | - | **Obrigatório**. Host do PostgreSQL |
| `DB_PORT` | 5432 | Porta do PostgreSQL |
| `DB_NAME` | - | **Obrigatório**. Nome do banco de dados |
| `DB_USER` | - | **Obrigatório**. Usuário do banco |
| `DB_PASSWORD` | - | **Obrigatório**. Senha do banco |
| `LOG_LEVEL` | INFO | Nível de log (DEBUG/INFO/WARNING/ERROR) |
| `ETL_MAX_RETRIES` | 3 | Tentativas em caso de falha |
| `ETL_BATCH_SIZE` | 1000 | Registros por batch |
| `ETL_PARALLEL_INGESTORS` | 1 | Ingestores em paralelo |
| `CSV_SEPARATOR` | ; | Separador de colunas CSV |
| `CSV_ENCODING` | utf-8-sig | Encoding dos arquivos CSV |

---

## 🎮 Uso

### Comandos Essenciais

#### Executar TODOS os Ingestores

```bash
# Modo sequencial (padrão)
docker compose exec etl-processor python python/run_all_ingestors.py

# Modo paralelo (mais rápido)
docker compose exec etl-processor python python/run_all_ingestors.py --parallel

# Especificar número de workers
docker compose exec etl-processor python python/run_all_ingestors.py --parallel --workers 5
```

#### Executar Ingestores Específicos

```bash
# Apenas faturamento e usuários
docker compose exec etl-processor python python/run_all_ingestors.py \
  --scripts faturamento usuarios

# Listar ingestores disponíveis
docker compose exec etl-processor python python/run_all_ingestors.py --list
```

#### Executar Ingestor Individual

```bash
# Faturamento
docker compose exec etl-processor python python/ingestors/csv/ingest_faturamento.py

# Usuários
docker compose exec etl-processor python python/ingestors/csv/ingest_usuarios.py

# Contas
docker compose exec etl-processor python python/ingestors/csv/ingest_contas_base_oficial.py
```

### Acessar Shell do Container

```bash
docker compose exec etl-processor bash
```

### Visualizar Logs

```bash
# Logs do container
docker compose logs -f etl-processor

# Logs dos scripts (dentro do container)
docker compose exec etl-processor tail -f /app/logs/*.log
```

### Parar e Remover Container

```bash
docker compose down
```

### Reconstruir Container (após mudanças)

```bash
docker compose up -d --build
```

---

## 📂 Estrutura do Projeto

```
credits-dw/
├── 📁 docker/                          # Configuração Docker
│   ├── Dockerfile                      # Imagem Python ETL
│   ├── docker-compose.yml              # Orquestração de serviços
│   └── 📁 data/
│       ├── 📁 input/onedrive/          # CSVs a processar
│       ├── 📁 processed/               # CSVs já processados (backup)
│       └── 📁 templates/               # Exemplos de CSV
│
├── 📁 python/                          # Código-fonte Python
│   ├── 📁 ingestors/                   # Scripts de ingestão
│   │   └── 📁 csv/
│   │       ├── base_csv_ingestor.py    # ⭐ Classe base (Template Method)
│   │       ├── ingest_faturamento.py   # Ingestor de faturamento
│   │       ├── ingest_usuarios.py      # Ingestor de usuários
│   │       └── ingest_contas_base_oficial.py  # Ingestor de contas
│   │
│   ├── 📁 utils/                       # Utilitários compartilhados
│   │   ├── config.py                   # ⭐ Configuração centralizada
│   │   ├── db_connection.py            # ⭐ Gerenciamento de conexões
│   │   ├── audit.py                    # ⭐ Sistema de auditoria
│   │   └── logger.py                   # ⭐ Logging com Loguru
│   │
│   └── run_all_ingestors.py            # ⭐ Orquestrador principal
│
├── 📁 docs/                            # Documentação adicional
│   ├── 01-Configuracao-Ambiente.md
│   ├── 02-Acesso-Banco-de-Dados.md
│   ├── 03-Executando-ETL.md
│   └── 04-Estrutura-Projeto.md
│
├── .env.example                        # Template de variáveis de ambiente
├── .gitignore                          # Arquivos ignorados pelo Git
├── requirements.txt                    # Dependências Python
├── README.md                           # 📖 Esta documentação
└── CLAUDE.md                           # Guia para Claude Code

⭐ = Arquivos principais/refatorados na v3.0
```

### Arquivos Principais

| Arquivo | Descrição | Versão |
|---------|-----------|--------|
| `base_csv_ingestor.py` | Classe base abstrata com Template Method pattern | v3.0 |
| `config.py` | Configuração centralizada com dataclasses | v3.0 |
| `db_connection.py` | Context managers e retry logic para PostgreSQL | v3.0 |
| `audit.py` | Sistema de auditoria com context managers | v3.0 |
| `logger.py` | Logging estruturado com Loguru e rotação | v3.0 |
| `run_all_ingestors.py` | Orquestrador com paralelização e CLI | v3.0 |

---

## 🛠️ Desenvolvimento

### Setup Local (Sem Docker)

```bash
# Criar ambiente virtual
python3.10 -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# Instalar dependências
pip install -r requirements.txt

# Executar ingestor
python python/ingestors/csv/ingest_faturamento.py
```

### Criar Novo Ingestor

1. **Herdar da classe base:**

```python
# python/ingestors/csv/ingest_meu_dados.py
from ingestors.csv.base_csv_ingestor import BaseCSVIngestor
from typing import Dict, List

class IngestMeusDados(BaseCSVIngestor):
    def __init__(self):
        super().__init__(
            script_name='ingest_meus_dados.py',
            tabela_destino='bronze.meus_dados',
            arquivo_nome='meus_dados.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """Mapeia colunas CSV -> Bronze"""
        return {
            'Coluna CSV': 'coluna_bronze',
            'Outra Coluna': 'outra_coluna'
        }

    def get_bronze_columns(self) -> List[str]:
        """Lista colunas Bronze na ordem"""
        return ['coluna_bronze', 'outra_coluna']

if __name__ == '__main__':
    import sys
    sys.exit(IngestMeusDados().executar())
```

2. **Registrar no orquestrador:**

```python
# python/run_all_ingestors.py
from ingestors.csv.ingest_meus_dados import IngestMeusDados

INGESTORS_REGISTRY = {
    'contas': IngestContasBaseOficial,
    'faturamento': IngestFaturamento,
    'usuarios': IngestUsuarios,
    'meus_dados': IngestMeusDados,  # ← Adicionar aqui
}
```

3. **Criar tabela no PostgreSQL:**

```sql
CREATE TABLE bronze.meus_dados (
    coluna_bronze TEXT,
    outra_coluna TEXT,
    data_carga_bronze TIMESTAMP DEFAULT NOW()
);
```

### Ferramentas de Qualidade de Código

```bash
# Formatação automática
black python/

# Linting
ruff check python/

# Type checking
mypy python/

# Executar todos
black python/ && ruff check python/ && mypy python/
```

### Testes (Futuro)

```bash
# Executar testes unitários
pytest python/tests/ -v

# Com cobertura
pytest python/tests/ --cov=python --cov-report=html
```

---

## 🎉 Melhorias da Versão 3.0

### 🆕 Novos Recursos

#### 1. **Configuração Centralizada** (`config.py`)
- ✅ Dataclasses para configuração tipada
- ✅ Validação automática de variáveis obrigatórias
- ✅ Singleton pattern para configuração global
- ✅ Suporte a múltiplos ambientes

#### 2. **Logging Aprimorado** (`logger.py`)
- ✅ Migração para Loguru (logs mais bonitos e informativos)
- ✅ Rotação automática de logs (100MB, 30 dias)
- ✅ Compressão de logs antigos em ZIP
- ✅ Formatação colorida no console
- ✅ Métricas de throughput (linhas/segundo)

#### 3. **Conexões Robustas** (`db_connection.py`)
- ✅ Context managers (`with get_connection()`)
- ✅ Retry automático em falhas de conexão (tenacity)
- ✅ Pool de conexões configurável
- ✅ Cursores especializados (dict cursor)
- ✅ Health check de conexão

#### 4. **Auditoria Avançada** (`audit.py`)
- ✅ Context manager para auditoria automática
- ✅ Estatísticas de execução (taxa de sucesso, duração média)
- ✅ Consulta de execuções em andamento
- ✅ Histórico detalhado por script

#### 5. **ETL Otimizado** (`base_csv_ingestor.py`)
- ✅ Validação de arquivo e schema
- ✅ Inserções em batch configuráveis
- ✅ Métricas de performance (tempo, throughput)
- ✅ Logs estruturados e informativos
- ✅ Tratamento robusto de erros

#### 6. **Orquestração Avançada** (`run_all_ingestors.py`)
- ✅ **Execução paralela** de ingestores
- ✅ CLI completo com argparse
- ✅ Relatório consolidado de execução
- ✅ Execução seletiva de ingestores
- ✅ Listagem de ingestores disponíveis

### 🔧 Melhorias Técnicas

| Área | v2.0 | v3.0 |
|------|------|------|
| **Logging** | logging padrão | Loguru com rotação |
| **Config** | Hardcoded/env vars | Centralizado com validação |
| **Conexões** | Manual | Context managers + retry |
| **Auditoria** | Básica | Avançada com métricas |
| **Paralelização** | ❌ | ✅ ThreadPoolExecutor |
| **CLI** | ❌ | ✅ argparse completo |
| **Validação** | Mínima | Schema + arquivo + dados |
| **Métricas** | Básicas | Throughput + duração detalhada |
| **Segurança** | Senhas hardcoded | 100% env vars + validação |

---

## 🐛 Troubleshooting

### Problema: Container não inicia

```bash
# Ver logs detalhados
docker compose logs etl-processor

# Reconstruir imagem
docker compose down
docker compose build --no-cache
docker compose up -d
```

### Problema: Erro de conexão com banco de dados

```bash
# Testar conexão manualmente
docker compose exec etl-processor psql -h $DB_HOST -U $DB_USER -d $DB_NAME

# Verificar variáveis de ambiente
docker compose exec etl-processor env | grep DB_
```

**Soluções comuns:**
- ✅ Verifique se `.env` está configurado corretamente
- ✅ Confirme que o IP do container tem acesso ao PostgreSQL
- ✅ Verifique firewall/security groups no Azure
- ✅ Teste credenciais com cliente PostgreSQL local

### Problema: Arquivo CSV não encontrado

```bash
# Listar arquivos no container
docker compose exec etl-processor ls -la /app/data/input/onedrive/

# Copiar arquivo para container
docker cp meu_arquivo.csv credits-dw-etl:/app/data/input/onedrive/
```

### Problema: Erro "Missing required environment variables"

**Causa:** Variáveis obrigatórias não definidas no `.env`

**Solução:**
```bash
# Verificar .env existe
ls -la .env

# Comparar com template
diff .env .env.example

# Garantir que Docker Compose carrega .env
docker compose config | grep DB_HOST
```

### Problema: Logs não aparecem

```bash
# Verificar permissões
docker compose exec etl-processor ls -la /app/logs/

# Criar diretório manualmente
docker compose exec etl-processor mkdir -p /app/logs
docker compose exec etl-processor chmod 777 /app/logs
```

### Problema: Performance lenta

**Otimizações:**

1. **Aumentar batch size:**
```bash
# No .env
ETL_BATCH_SIZE=5000  # Padrão: 1000
```

2. **Usar execução paralela:**
```bash
docker compose exec etl-processor python python/run_all_ingestors.py \
  --parallel --workers 5
```

3. **Ajustar resources no Docker:**
```yaml
# docker-compose.yml
deploy:
  resources:
    limits:
      cpus: "4"      # Aumentar CPUs
      memory: 4G     # Aumentar RAM
```

---

## 📊 Monitoramento e Observabilidade

### Consultar Histórico de Execuções

```sql
-- Últimas 10 execuções
SELECT
    script_nome,
    status,
    data_inicio,
    data_fim,
    EXTRACT(EPOCH FROM (data_fim - data_inicio)) as duracao_segundos,
    linhas_processadas,
    linhas_inseridas
FROM credits.historico_atualizacoes
ORDER BY data_inicio DESC
LIMIT 10;

-- Taxa de sucesso por script (últimos 30 dias)
SELECT
    script_nome,
    COUNT(*) as total_execucoes,
    SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END) as sucessos,
    ROUND(SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) as taxa_sucesso
FROM credits.historico_atualizacoes
WHERE data_inicio >= NOW() - INTERVAL '30 days'
GROUP BY script_nome
ORDER BY taxa_sucesso DESC;

-- Execuções em andamento (possível travamento)
SELECT
    script_nome,
    data_inicio,
    NOW() - data_inicio as tempo_decorrido
FROM credits.historico_atualizacoes
WHERE status = 'em_execucao'
  AND data_inicio < NOW() - INTERVAL '1 hour';  -- Mais de 1h rodando
```

### Métricas de Performance

Os logs agora incluem:
- ⏱️ Duração total da execução
- 📈 Throughput (linhas/segundo)
- 💾 Uso de memória do DataFrame
- 📊 Estatísticas de valores nulos
- 🔢 Contadores de linhas processadas vs inseridas

---

## 🤝 Contribuição

### Fluxo de Trabalho

1. **Fork** o repositório
2. **Clone** seu fork localmente
3. **Crie branch** para sua feature (`git checkout -b feat/nova-feature`)
4. **Commit** suas mudanças (`git commit -m 'feat: adiciona nova feature'`)
5. **Push** para o branch (`git push origin feat/nova-feature`)
6. **Abra Pull Request** no GitHub

### Convenções de Commit

Seguimos [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` Nova funcionalidade
- `fix:` Correção de bug
- `refactor:` Refatoração sem mudança de funcionalidade
- `docs:` Mudanças na documentação
- `test:` Adição ou correção de testes
- `chore:` Tarefas de manutenção

---

## 📝 Licença

Este projeto é de propriedade da **Credits Brasil** e é de uso interno.

---

## 👥 Equipe

**Mantido por:** Equipe de Engenharia de Dados - Credits Brasil

**Contato:** [Seu email/Slack aqui]

---

## 📚 Documentação Adicional

Para mais detalhes, consulte:

- 📄 [CLAUDE.md](./CLAUDE.md) - Guia para Claude Code
- 📄 [docs/01-Configuracao-Ambiente.md](./docs/01-Configuracao-Ambiente.md)
- 📄 [docs/02-Acesso-Banco-de-Dados.md](./docs/02-Acesso-Banco-de-Dados.md)
- 📄 [docs/03-Executando-ETL.md](./docs/03-Executando-ETL.md)
- 📄 [docs/04-Estrutura-Projeto.md](./docs/04-Estrutura-Projeto.md)

---

<div align="center">

**🚀 Feito com ❤️ pela equipe de Engenharia de Dados da Credits Brasil**

⭐ Se este projeto foi útil, considere dar uma estrela!

</div>
