# Estrutura de Dados

## 📂 Organização dos Diretórios

### `/input` - Arquivos de Entrada
Aqui ficam os arquivos CSV que serão processados pelos scripts ETL.

#### Estrutura:
```
input/
├── contas.csv
├── usuarios.csv
├── faturamentos.csv
├── data.csv
└── ... (outros arquivos CSV)
```

**Importante:**
- Coloque os arquivos CSV diretamente nesta pasta (sem subdiretórios)
- Os scripts lerão os arquivos daqui e carregarão na camada Bronze
- Após processamento, os arquivos são movidos para `/processed` com timestamp

### `/processed` - Arquivos Processados
Arquivos que já foram carregados com sucesso no Data Warehouse.

#### Estrutura:
```
processed/
├── 2025-11-01_14-30-00_contas.csv
├── 2025-11-01_14-32-15_usuarios.csv
└── ... (histórico de cargas)
```

**Importante:**
- Arquivos são movidos automaticamente após processamento bem-sucedido
- Nome do arquivo inclui timestamp do processamento (YYYY-MM-DD_HH-MM-SS_nome.csv)
- Mantém histórico de cargas para auditoria

### `/templates` - Exemplos de Arquivos CSV
Arquivos de exemplo com headers e dados de teste para referência.

## 🔄 Fluxo de Processamento

1. **Coloque arquivo em** → `/input/arquivo.csv`
2. **Execute script ETL** → Script lê de `/input`
3. **Validação rigorosa** → Apenas dados válidos são aceitos (v2.0)
4. **Carrega no banco** → Dados válidos vão para camada Bronze
5. **Move arquivo** → De `/input` para `/processed` com timestamp

## 📋 Formatos Suportados

- **CSV** (separador: `,` por padrão, configurável por ingestor)
- **Encoding**: UTF-8

## 🔍 Exemplo de Uso

```bash
# 1. Copiar arquivo para pasta de input
cp /caminho/origem/contas.csv docker/data/input/

# 2. Executar ingestão via Docker
cd docker
docker-compose exec etl-processor python python/ingestors/csv/ingest_contas.py

# 3. Verificar arquivo processado
ls -lh docker/data/processed/
```
