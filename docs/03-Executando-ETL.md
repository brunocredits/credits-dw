# 03 - Executando os Scripts de ETL

Este guia explica como executar os scripts de ingestão de dados (ETL) usando o ambiente Docker configurado no projeto.

---

## Visão Geral do Processo

Os scripts de ETL são escritos em Python e executados dentro de um contêiner Docker. Isso garante que o ambiente de execução seja consistente e não dependa da configuração da máquina local.

O `docker-compose.yml` orquestra um serviço chamado `etl-processor`, que é o contêiner onde os scripts são executados.

---

## Passo 1: Iniciar o Ambiente Docker

Antes de executar qualquer script, você precisa iniciar o contêiner de ETL.

1.  Abra um terminal na raiz do projeto (`credits.dw/`).
2.  Navegue até a pasta `docker`:

    ```bash
    cd docker
    ```

3.  Inicie os serviços do Docker em segundo plano (`-d`):

    ```bash
    docker compose up -d --build
    ```

    O comando `--build` garante que a imagem Docker seja reconstruída se houver alguma alteração no `Dockerfile`.

    Após a execução, o contêiner `etl-processor` estará ativo e pronto para receber comandos.

---

## Passo 2: Executar os Scripts de Ingestão

Com o contêiner rodando, você pode executar os scripts de ingestão usando o comando `docker compose exec`.

**Localização dos Comandos:** Lembre-se de executar estes comandos de dentro da pasta `docker/`.

### Opção A: Executar Todos os Ingestores de CSV de Uma Vez

Para rodar todos os scripts de ingestão de arquivos CSV em sequência, use o `run_all_ingestors.py`.

```bash
docker compose exec etl-processor python python/run_all_ingestors.py
```

### Opção B: Executar um Ingestor Específico

Se você precisar processar apenas um arquivo específico (ex: faturamento), pode executar o script individualmente.

```bash
# Exemplo para o ingestor de faturamento
docker compose exec etl-processor python python/ingestors/csv/ingest_faturamento.py

# Exemplo para o ingestor de usuários
docker compose exec etl-processor python python/ingestors/csv/ingest_usuarios.py
```

---

## Passo 3: Verificar os Logs

Cada script de ingestão gera um arquivo de log na pasta `logs/` (na raiz do projeto). Se ocorrer algum erro ou se você quiser verificar o que aconteceu durante a execução, consulte o arquivo de log correspondente.

Os logs também são exibidos em tempo real no terminal se você rodar o Docker em modo interativo (sem o `-d`). Para ver os logs de um contêiner que está rodando em segundo plano, use:

```bash
# Execute de dentro da pasta docker/
docker compose logs -f etl-processor
```

---

## Passo 4: Parar o Ambiente Docker

Quando terminar de usar o ambiente de ETL, é uma boa prática parar os contêineres para liberar recursos da sua máquina.

```bash
# Execute de dentro da pasta docker/
docker compose down
```

Este comando para e remove os contêineres definidos no `docker-compose.yml`.
