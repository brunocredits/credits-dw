# Estrutura de Dados

## 📂 Organização dos Diretórios

### `/input` - Arquivos de Entrada
Aqui ficam os arquivos compartilhados que serão processados pelos scripts ETL.

#### Estrutura Sugerida:
```
input/
├── onedrive/           # Arquivos do OneDrive
│   ├── Clientes.csv
│   ├── Contratos.csv
│   └── ...
├── faturamento/        # Arquivos de faturamento (mensal)
│   ├── 2025-01/
│   │   └── faturamento_janeiro_2025.csv
│   ├── 2025-02/
│   │   └── faturamento_fevereiro_2025.csv
│   └── ...
├── consumo/            # 5 fontes diferentes de consumo
│   ├── fonte1/
│   ├── fonte2/
│   ├── fonte3/
│   ├── fonte4/
│   └── fonte5/
└── outros/             # Outras fontes CSV/JSON/TXT
```

**Importante:**
- Coloque os arquivos nesta pasta antes de executar os scripts de ingestão
- Os scripts lerão os arquivos daqui e carregarão na camada Bronze
- Após processamento, os arquivos são movidos para `/processed`

### `/processed` - Arquivos Processados
Arquivos que já foram carregados com sucesso no Data Warehouse.

#### Estrutura:
```
processed/
├── 2025-11-01_14-30-00_Clientes.csv
├── 2025-11-01_14-32-15_Contratos.csv
└── ...
```

**Importante:**
- Arquivos são movidos automaticamente após processamento bem-sucedido
- Nome do arquivo inclui timestamp do processamento
- Mantém histórico de cargas para auditoria

## 🔄 Fluxo de Processamento

1. **Coloque arquivo em** → `/input/[categoria]/arquivo.csv`
2. **Execute script ETL** → Script lê de `/input`
3. **Carrega no banco** → Dados vão para camada Bronze
4. **Move arquivo** → De `/input` para `/processed` com timestamp

## 📋 Formatos Suportados

- **CSV** (separador: `;` ou `,`)
- **JSON** (array de objetos ou line-delimited)
- **TXT** (delimitado por tabulação)
- **Excel** (XLSX, XLS) - via pandas

## 🔍 Exemplo de Uso

```bash
# 1. Copiar arquivo para pasta compartilhada
cp /caminho/origem/Clientes.csv docker/data/input/onedrive/

# 2. Executar ingestão via Docker
cd docker
docker-compose exec etl-processor python python/ingestors/csv/ingest_onedrive_clientes.py

# 3. Verificar arquivo processado
ls docker/data/processed/
```
