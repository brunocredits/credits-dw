# Credits Brasil Data Warehouse (Credits DW)

Este projeto implementa um pipeline de dados (ETL) containerizado para ingerir dados brutos de arquivos (CSV, Excel, ODS) em um Data Warehouse PostgreSQL na camada Bronze (Raw).

## 📋 Visão Geral

O pipeline é desenvolvido em Python e orquestrado via Docker Compose. Ele suporta:
- **Ingestão Dinâmica:** Detecta automaticamente arquivos de `faturamento`, `base_oficial` e `usuarios` no diretório de input.
- **Validação de Schema:** Verifica se os arquivos de entrada correspondem aos templates esperados.
- **Limpeza de Dados:** Tratamento básico de tipos numéricos e datas.
- **Auditoria Robusta:** Logs de execução e tabela de rejeição (`auditoria.log_rejeicao`) detalhada no banco de dados.
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
│   ├── scripts/         # Scripts executáveis (run_pipeline.py)
│   └── utils/           # Utilitários (DB, Logger)
├── logs/                # Logs de execução em arquivo
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

- **Logs:** Verifique a pasta `logs/` para detalhes técnicos da execução.

## 📝 Decisões de Arquitetura

- **Camada Bronze (Raw):** O foco é ingerir os dados com mínima transformação destrutiva.
- **Validação Flexível:** Campos obrigatórios ausentes geram alertas (`WARN`) mas não bloqueiam a ingestão, permitindo correção posterior na camada Silver.
- **Alta Performance:** Uso de `COPY FROM STDIN` do PostgreSQL para carga em massa.
