# 01 - Configuração do Ambiente de Desenvolvimento

Este guia descreve os passos necessários para configurar o ambiente de desenvolvimento do Data Warehouse da Credits Brasil em uma nova máquina.

---

## Pré-requisitos

Antes de começar, certifique-se de que você tem as seguintes ferramentas instaladas na sua máquina:

*   **Git:** Para controle de versão.
*   **Docker e Docker Compose:** Para orquestração dos contêineres.
*   **Python 3.10+:** Para desenvolvimento e execução de scripts locais.
*   Um **cliente de banco de dados SQL** (DBeaver, pgAdmin, etc.) para se conectar ao PostgreSQL.

---

## Passo 1: Clonar o Repositório

O primeiro passo é clonar o repositório do projeto para a sua máquina local.

Abra um terminal e execute o seguinte comando:

```bash
git clone <URL_DO_REPOSITORIO_GIT>
cd credits.dw
```

Substitua `<URL_DO_REPOSITORIO_GIT>` pela URL correta do repositório no GitHub/Azure DevOps.

---

## Passo 2: Configurar Variáveis de Ambiente

O projeto utiliza um arquivo `.env` para armazenar as credenciais do banco de dados de forma segura, sem que elas sejam enviadas para o repositório Git.

1.  **Crie o arquivo `.env`:** Na raiz do projeto (`credits.dw/`), crie um arquivo chamado `.env`.

2.  **Copie e cole o conteúdo abaixo** no arquivo `.env` e preencha com as credenciais corretas do banco de dados PostgreSQL no Azure.

    ```properties
    # Credenciais do Banco de Dados PostgreSQL
    DB_HOST=creditsdw.postgres.database.azure.com
    DB_PORT=5432
    DB_NAME=creditsdw
    DB_USER=<seu_usuario_do_banco>
    DB_PASSWORD=<sua_senha_do_banco>

    # Nível de log (opcional, padrão: INFO)
    LOG_LEVEL=INFO
    ```

    **Importante:** O arquivo `.env` já está incluído no `.gitignore`, então suas credenciais nunca serão versionadas.

---

## Passo 3: Preparar Dados de Exemplo (Opcional)

Os scripts de ingestão procuram por arquivos CSV no diretório `docker/data/input/onedrive/`. Para testar o pipeline com dados de exemplo, você pode copiar os templates existentes.

Execute o seguinte comando a partir da raiz do projeto:

```bash
cp docker/data/templates/*.csv docker/data/input/onedrive/
```

Isso garante que, ao rodar os scripts de ETL, haverá dados para serem processados.

---

Com o ambiente configurado, o próximo passo é aprender a se conectar ao banco de dados, o que está descrito no arquivo `02-Acesso-Banco-de-Dados.md`.
