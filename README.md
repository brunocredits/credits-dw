# 🏭 Credits Data Warehouse - Camada Bronze (ETL)

Bem-vindo ao repositório de ETL da **Credits Brasil**. Este projeto gerencia a ingestão, padronização e carga de dados brutos (CSV/Excel) para o Data Warehouse na camada Bronze.

---

## 🚀 Visão Geral

O sistema roda inteiramente em **Docker**, garantindo isolamento e reprodutibilidade.
Ele é responsável por:
1.  **Ler** arquivos da pasta `docker/data/input/`.
2.  **Padronizar** automaticamente os nomes de colunas e formatos.
3.  **Filtrar** estritamente os campos permitidos pelo schema.
4.  **Ingerir** os dados no banco PostgreSQL (`bronze`).
5.  **Auditar** todo o processo (`auditoria`).

---

## 📁 Estrutura de Pastas

-   `docker/data/input/`: **Coloque seus arquivos aqui** (CSV ou Excel).
-   `docker/data/processed/`: Arquivos processados são movidos para cá automaticamente.
-   `docker/data/templates/`: Modelos CSV de exemplo para preenchimento correto.
-   `docker/logs/`: Logs detalhados de execução.
-   `python/`: Código fonte dos ingestores e utilitários.

---

## 📋 Como Usar

### 1. Preparar Arquivos
Consulte a pasta `docker/data/templates/` para ver o formato esperado.
Os arquivos esperados são:
*   `faturamentos.csv` (ou Excel contendo "Faturamento")
*   `base_oficial.csv` (ou Excel contendo "Base")
*   `usuarios.csv` (ou Excel contendo "Usuarios")

**Nota:** O sistema é inteligente. Ele detecta a aba correta no Excel e renomeia colunas comuns (ex: "Valor da Conta" -> "valor_conta").

### 2. Colocar na Pasta de Entrada
Mova seus arquivos para:
```bash
~/credits-dw/docker/data/input/
```

### 3. Executar a Carga
Execute o comando abaixo para rodar todo o pipeline (Padronização + Ingestão):

```bash
# Entra na pasta do projeto
cd ~/credits-dw

# 1. Padronizar arquivos (Gera CSVs limpos em data/ready e move para input)
docker-compose -f docker/docker-compose.yml exec etl-processor python python/standardize_files.py
docker-compose -f docker/docker-compose.yml exec etl-processor bash -c "mv /app/data/ready/*.csv /app/data/input/"

# 2. Ingerir no Banco
docker-compose -f docker/docker-compose.yml exec etl-processor python python/run_bronze_ingestors.py
```

---

## 📊 Schemas e Campos

O sistema utiliza um **Schema Estrito**. Campos fora desta lista serão **descartados**. Campos obrigatórios vazios gerarão **avisos** nos logs, mas o registro será carregado (como NULL).

### 🟦 Faturamento (`bronze.faturamento`)
*   **Dados:** status, numero_documento, parcela, nota_fiscal, cliente_nome_fantasia
*   **Financeiro:** valor_conta, valor_liquido, impostos_retidos, desconto, juros_multa, valor_recebido, valor_a_receber
*   **Datas:** previsao_recebimento, ultimo_recebimento, vencimento, data_emissao, data_fat
*   **Outros:** categoria, operacao, vendedor, projeto, conta_corrente, numero_boleto, tipo_documento, cliente_razao_social, tags_cliente, observacao, empresa, ms

### 🟦 Base Oficial (`bronze.base_oficial`)
*   **Principal:** cnpj, status, nome_fantasia
*   **Gestão:** lider, responsavel, empresa, grupo, canal1, canal2
*   **Controle:** manter_no_baseline, obs
*   **Fórmulas:** faixas, mediana

### 🟦 Usuários (`bronze.usuarios`)
*   **Perfil:** nome_usuario, cargo, status, nivel, time, email_usuario
*   **Metas:** meta_mensal, meta_fidelidade, meta_anual
*   **Acessos (Boolean):** acesso_vendedor, acesso_gerente, acesso_indireto, acesso_diretoria, acesso_temporario
*   **Hierarquia (Emails):** email_superior, email_gerencia, email_diretoria

---

## 🛠️ Comandos Úteis

**Verificar Logs em Tempo Real:**
```bash
docker-compose -f docker/docker-compose.yml logs -f
```

**Verificar Status da Auditoria:**
```bash
docker-compose -f docker/docker-compose.yml exec etl-processor python python/check_audit.py
```

**Reiniciar Serviços:**
```bash
docker-compose -f docker/docker-compose.yml restart
```
