# Credits DW - Data Warehouse Pipeline

Pipeline de ingestão de dados para a camada Bronze do Data Warehouse, implementado com arquitetura modular e otimizado para alta performance.

## 🏗️ Arquitetura

### Camadas do Data Warehouse
- **Bronze (RAW)**: Dados brutos validados e limpos
- **Silver**: Dados transformados e enriquecidos *(próxima etapa)*
- **Gold**: Dados agregados para consumo *(próxima etapa)*

### Componentes Principais

```
python/
├── core/
│   ├── base_ingestor.py    # Orquestrador principal
│   ├── data_cleaner.py     # Limpeza de dados
│   ├── file_handler.py     # Gerenciamento de arquivos
│   └── validator.py        # Validação de estrutura
├── ingestors/
│   ├── ingest_faturamento.py
│   ├── ingest_usuarios.py
│   └── ingest_base_oficial.py
└── utils/
    ├── db_connection.py    # Conexão com PostgreSQL
    └── audit.py            # Sistema de auditoria
```

## 🚀 Quick Start

### Pré-requisitos
- Docker e Docker Compose
- PostgreSQL (Azure Database for PostgreSQL)
- Python 3.9+

### Configuração

1. **Clone o repositório**
```bash
git clone https://github.com/brunocredits/credits-dw.git
cd credits-dw
```

2. **Configure variáveis de ambiente**
```bash
cp .env.example .env
# Edite .env com suas credenciais
```

3. **Execute o pipeline**
```bash
./run_pipeline.sh
```

### Reset do Ambiente
Para limpar dados e preparar nova carga:
```bash
./reset_env.sh
```

## 📊 Funcionalidades

### ✅ Implementado

- **Detecção de Duplicatas**: Hash MD5 para evitar reprocessamento
- **Validação de Headers**: Comparação com templates oficiais
- **Limpeza de Dados**:
  - Conversão de formato brasileiro (1.000,00 → 1000.00)
  - Tratamento de hífen como zero
  - Validação de datas (DD/MM/YYYY)
- **Idempotência**: Delete-before-load por arquivo
- **Auditoria Completa**: Rastreamento de execuções e erros
- **Alta Performance**: PostgreSQL COPY para carga em massa

### 🎯 Características Técnicas

- **Arquitetura Modular**: Princípios SOLID (SRP, DIP)
- **Clean Code**: Código documentado em português
- **Segurança**: Validação rigorosa, parâmetros bind SQL
- **Performance**: Operações vetorizadas (Pandas/Numpy)

## 📁 Estrutura de Dados

### Tabelas Bronze
- `bronze.faturamento` - Dados de faturamento (32 colunas)
- `bronze.usuarios` - Cadastro de usuários
- `bronze.base_oficial` - Base oficial de clientes
- `bronze.data` - Tabela de datas (dimensão)

### Auditoria
- `auditoria.historico_execucao` - Log de execuções
- `auditoria.log_rejeicao` - Linhas rejeitadas com motivo

## 🔍 Queries de Monitoramento

### Verificar última execução
```sql
SELECT script_nome, data_inicio, status, 
       linhas_processadas, linhas_inseridas, linhas_erro
FROM auditoria.historico_execucao
ORDER BY data_inicio DESC
LIMIT 10;
```

### Analisar rejeições
```sql
SELECT tabela_destino, motivo_rejeicao, COUNT(*) as qtd
FROM auditoria.log_rejeicao
GROUP BY tabela_destino, motivo_rejeicao
ORDER BY qtd DESC;
```

### Verificar duplicatas detectadas
```sql
SELECT COUNT(*) as arquivos_duplicados
FROM auditoria.historico_execucao
WHERE status = 'sucesso' 
  AND file_hash IN (
    SELECT file_hash 
    FROM auditoria.historico_execucao 
    GROUP BY file_hash 
    HAVING COUNT(*) > 1
  );
```

## 📋 Próximos Passos

### 🔄 Camada Silver (Transformação)
- [ ] Criar módulo de transformação de dados
- [ ] Implementar deduplicação de registros
- [ ] Adicionar enriquecimento de dados
- [ ] Criar tabelas de dimensão (SCD Type 2)
- [ ] Implementar validações de negócio avançadas

### 📊 Camada Gold (Agregação)
- [ ] Criar views materializadas para dashboards
- [ ] Implementar métricas de negócio
- [ ] Adicionar tabelas de fatos agregadas
- [ ] Otimizar para queries analíticas

### 🔧 Melhorias Técnicas
- [ ] Implementar testes unitários (pytest)
- [ ] Adicionar testes de integração
- [ ] Configurar CI/CD (GitHub Actions)
- [ ] Implementar monitoramento com Prometheus/Grafana
- [ ] Adicionar alertas automáticos (Slack/Email)
- [ ] Criar documentação técnica completa (Sphinx)

### 🚀 Performance
- [ ] Implementar particionamento de tabelas
- [ ] Adicionar índices otimizados
- [ ] Configurar vacuum automático
- [ ] Implementar cache de queries frequentes

### 🔐 Segurança
- [ ] Implementar criptografia de dados sensíveis
- [ ] Adicionar auditoria de acessos
- [ ] Configurar backup automático
- [ ] Implementar disaster recovery

### 📱 Observabilidade
- [ ] Dashboard de métricas em tempo real
- [ ] Logs centralizados (ELK Stack)
- [ ] Rastreamento distribuído (OpenTelemetry)
- [ ] SLA monitoring

## 🛠️ Desenvolvimento

### Estrutura de Branches
- `main` - Produção
- `develop` - Desenvolvimento
- `feature/*` - Novas funcionalidades
- `hotfix/*` - Correções urgentes

### Padrões de Commit
```
feat: Nova funcionalidade
fix: Correção de bug
docs: Documentação
refactor: Refatoração
perf: Melhoria de performance
test: Testes
chore: Tarefas gerais
```

## 📚 Documentação Adicional

- [Guia de Demonstração](/.gemini/antigravity/brain/.../DEMO_GUIDE.md)
- [Regras de Validação](/.gemini/antigravity/brain/.../regras_validacao_faturamento.md)
- [Estrutura do Banco](/.gemini/antigravity/brain/.../estrutura_banco_dados.md)

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'feat: Add AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📝 Licença

Projeto proprietário - Credits Brasil

## 👥 Time

- **Desenvolvedor**: Bruno Pires
- **Organização**: Credits Brasil

---

**Status**: ✅ Bronze Layer - Produção  
**Próxima Milestone**: 🔄 Silver Layer - Em Planejamento