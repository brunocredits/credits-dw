# Credits DW - Data Warehouse Project

Este projeto implementa um Data Warehouse para processamento de dados financeiros e de clientes da Credits Brasil. O pipeline é construído em **Python**, rodando sobre **Docker**, utilizando **PostgreSQL** como banco de dados.

## 📂 Estrutura do Projeto

```
credits-dw/
├── docker/
│   ├── data/
│   │   ├── input/          # Coloque seus CSVs/XLSX aqui para ingestão
│   │   ├── processed/      # Arquivos movidos para cá após sucesso
│   │   └── templates/      # Modelos .xlsx vazios para referência de preenchimento
│   ├── docker-compose.yml  # Orquestração dos containers (ETL + DB)
│   └── Dockerfile          # Imagem do processador ETL
├── python/
│   ├── core/               # Motor de ingestão otimizado (Bulk Copy)
│   ├── ingestors/          # Regras de negócio de cada arquivo (Faturamento, Base, Usuários)
│   ├── scripts/            # Scripts de execução (run_pipeline.py, generate_templates.py)
│   └── utils/              # Conexão DB, Logging, Auditoria
└── README.md
```

## 🚀 Como Rodar

### 1. Pré-requisitos
- Docker e Docker Compose instalados.

### 2. Execução
Suba o ambiente:
```bash
docker compose -f docker/docker-compose.yml up -d
```

Execute o pipeline de ingestão (processa arquivos na pasta `input`):
```bash
docker compose -f docker/docker-compose.yml exec etl-processor python3 python/scripts/run_pipeline.py
```

Gere templates atualizados (baseados no schema atual):
```bash
docker compose -f docker/docker-compose.yml exec etl-processor python3 python/scripts/generate_templates.py
```

---

## 🏗️ Arquitetura de Dados (Medallion Architecture)

O projeto segue a arquitetura Bronze/Silver/Gold.

### 🥉 Camada Bronze (Atual - Implementada)
Responsável pela ingestão bruta (Raw Data) com tipagem forte e validação básica.
*   **Objetivo:** Trazer o dado do arquivo para o banco com segurança, sem perder histórico.
*   **Validações:**
    *   Tipos de dados (Datas, Decimais, Inteiros).
    *   Campos obrigatórios (Rejeita a linha se faltar CNPJ, Documento, etc.).
    *   Metadados (Data de carga, Nome do arquivo de origem).
*   **Tabelas:**
    *   `bronze.faturamento`
    *   `bronze.base_oficial`
    *   `bronze.usuarios`
    *   `bronze.data` (Calendário)
    *   `bronze.erro_*` (Linhas rejeitadas para auditoria).

### 🥈 Camada Silver (Próximos Passos)
Responsável pela limpeza, padronização e enriquecimento (Business Logic).
*   **O que será feito:**
    *   **Cálculo de Fórmulas:**
        *   `FAIXAS`: Classificação baseada em regras de negócio.
        *   `MEDIANA`: Cálculos estatísticos agregados.
        *   `CNPJ (PONTUAÇÃO)`: Tratamento e validação de pontuação.
    *   **Padronização:** Unificação de nomes de status (ex: "Pago", "PAGO", "pago" -> "Pago").
    *   **Joins:** Cruzamento entre Faturamento e Base Oficial para enriquecer dados do cliente.

### 🥇 Camada Gold (Futuro)
Responsável por Agregações e KPIs para BI (Power BI/Metabase).
*   **Objetivo:** Tabelas fato e dimensão otimizadas para leitura (Star Schema).
*   **Exemplos:**
    *   `fato_receita_mensal`
    *   `dim_cliente_analise`
    *   KPIs de Inadimplência e Previsão de Recebimento.

---

## 📝 Campos e Regras (Bronze)

### Faturamento
Campos estritos definidos: `Status`, `Número do Documento`, `Valor Líquido`, `Vencimento`, etc.
*   Linhas sem `Cliente` ou `Número do Documento` são enviadas para `bronze.erro_faturamento`.

### Base Oficial
Campos estritos: `CNPJ`, `Status`, `Manter no Baseline`, etc.
*   Campos calculados como `Faixas` e `Mediana` **não** entram aqui; serão gerados na Silver.

---

**Desenvolvido por Credits Brasil - Data Team**