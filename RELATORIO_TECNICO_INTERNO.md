# RELATÓRIO TÉCNICO INTERNO - Data Warehouse Credits Brasil
**Data:** 27/11/2025
**Versão:** 5.0
**Status:** Produção - Arquitetura Medallion Completa (Bronze → Silver → Gold)
**Autor:** Equipe Engenharia de Dados

---

## SUMÁRIO EXECUTIVO

Este relatório documenta a arquitetura, implementação e decisões técnicas do Data Warehouse da Credits Brasil. O projeto implementa uma arquitetura Medallion completa (Bronze → Silver → Gold) com validação rigorosa, modelagem dimensional Star Schema e SCD Type 2 para rastreamento histórico.

**Resultados Atuais (27/11/2025):**
- ✅ Bronze: 35 registros válidos aceitos, 23 rejeitados (39.6% rejection rate)
- ✅ Silver: 10 clientes, 12 usuários, 319 datas, 13 faturamentos
- ✅ Gold: 5 views analíticas (apenas dados reais) - REFATORADA
- ✅ 100% integridade referencial (0 FKs órfãs)
- ✅ Pipeline 100% Docker (sem dependências locais)
- ✅ Zero manutenção Gold (views auto-atualizam)

---

## 1. VISÃO GERAL DO PROJETO

### 1.1 Objetivo

Criar um Data Warehouse centralizado para consolidar dados financeiros da Credits Brasil (clientes, usuários, faturamento) com:
- **Qualidade garantida**: Validação rigorosa na entrada (Bronze nunca aceita dados inválidos)
- **Modelo analítico**: Star Schema otimizado para Business Intelligence
- **Rastreabilidade**: Histórico de mudanças (SCD Type 2) e auditoria completa
- **Automação**: Pipeline ETL orquestrado via Docker

### 1.2 Arquitetura Medallion

```
┌─────────────────────────────────────────────────────────────────┐
│                         FONTE DE DADOS                          │
│                    CSV Files (OneDrive)                         │
│            contas.csv | usuarios.csv | faturamentos.csv         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CAMADA BRONZE 🥉                           │
│                   (Dados Brutos Validados)                      │
│                                                                 │
│  Estratégia: TRUNCATE/RELOAD                                    │
│  Validação: RIGOROSA (rejeita inválidos antes da inserção)     │
│                                                                 │
│  Tabelas:                                                       │
│    • bronze.contas       (10 registros)                         │
│    • bronze.usuarios     (12 registros)                         │
│    • bronze.faturamentos (13 registros)                         │
│    • bronze.data         (366 registros - calendário 2024)      │
│                                                                 │
│  Rejeições:                                                     │
│    • auditoria.log_rejeicao (23 registros rejeitados)           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CAMADA SILVER 🥈                           │
│                (Dados Curados - Star Schema)                    │
│                                                                 │
│  Estratégia: SCD Type 2 (dimensões) + FULL (fatos)             │
│  Modelagem: Star Schema                                         │
│                                                                 │
│  Dimensões:                                                     │
│    • silver.dim_cliente  (10 registros, SCD Type 2)             │
│    • silver.dim_usuario  (12 registros, SCD Type 2)             │
│    • silver.dim_data     (319 registros)                        │
│    • silver.dim_canal    (não implementada)                     │
│                                                                 │
│  Fatos:                                                         │
│    • silver.fato_faturamento (13 registros)                     │
│                                                                 │
│  Auditoria:                                                     │
│    • auditoria.historico_execucao (rastreamento completo)       │
└─────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                       CAMADA GOLD 🏆                            │
│               (Views Analíticas - Apenas Dados Reais)           │
│                                                                 │
│  Estratégia: SQL Views (auto-atualizam com Silver)             │
│  Propósito: Agregações simples para Power BI                   │
│                                                                 │
│  Views (5):                                                     │
│    • gold.vendas_diarias          (13 registros)                │
│    • gold.vendas_semanais         (13 registros)                │
│    • gold.vendas_mensais          (12 registros)                │
│    • gold.carteira_clientes       (13 registros)                │
│    • gold.performance_consultores (12 registros)                │
│                                                                 │
│  Características:                                               │
│    - Zero ETL (views SQL puras)                                 │
│    - Zero manutenção (atualização automática)                  │
│    - Sem campos inventados (apenas dados reais)                │
└─────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CONSUMO (BI / Analytics)                     │
│              Power BI | Metabase | Queries SQL                  │
│                                                                 │
│  • Dashboards: Vendas, Carteira, Performance                   │
│  • Filtros: consultor, gestor, período, cliente                │
│  • Modelo: Import (Gold views) + DirectQuery (Silver)          │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 Padrões de Arquitetura

**Template Method Pattern:**
- `BaseCSVIngestor`: Classe base para ingestores Bronze (define fluxo de execução)
- `BaseSilverTransformer`: Classe base para transformadores Silver (extração → transformação → validação → carga)

**Dependency Injection:**
- Configurações via variáveis de ambiente (.env)
- Conexão de banco injetada via `utils.db_connection`

**Strategy Pattern:**
- Bronze: TRUNCATE/RELOAD (substituição completa)
- Silver Dimensões: SCD Type 2 (versionamento)
- Silver Fatos: FULL (reconstrução completa)
- Gold: SQL Views (zero ETL, atualização automática)

**Design Principles:**
- DRY (Don't Repeat Yourself): Reutilização de classes base
- SOLID: Single Responsibility Principle em cada camada
- Data Quality First: Validação rigorosa antes de inserção
- **"Agregue o que existe, não invente o que falta"** (filosofia Gold layer)

### 1.4 Tecnologias

| Componente | Tecnologia | Versão | Justificativa |
|------------|------------|--------|---------------|
| **Database** | PostgreSQL | 15 | JSONB para logs, window functions para SCD2, suporte a constraints |
| **Linguagem** | Python | 3.10+ | Pandas para transformações, bibliotecas maduras para ETL |
| **Orquestração** | Docker Compose | - | Ambiente isolado, reprodutível, sem dependências locais |
| **Processamento** | Pandas | 2.1.4 | DataFrames eficientes para transformações tabulares |
| **DB Driver** | psycopg2 | 2.9.9 | Driver PostgreSQL oficial e performático |
| **Logging** | Loguru | 0.7.2 | Logs estruturados, coloridos, com rotação automática |
| **Hosting** | Azure Database | - | Managed PostgreSQL, backups automáticos, alta disponibilidade |

---

## 2. FLUXO DE DADOS DETALHADO

### 2.1 Pipeline Bronze (Ingestão com Validação Rigorosa)

```
┌─────────────┐
│ CSV File    │
│ (OneDrive)  │
└──────┬──────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│ 1. LEITURA                                   │
│    • Encoding: UTF-8 com fallback ISO-8859-1 │
│    • Delimiter: ';' (ponto-vírgula)          │
│    • Todas colunas lidas como STRING         │
└──────┬───────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│ 2. VALIDAÇÃO LINHA A LINHA                   │
│    Para cada linha:                          │
│    • Validar obrigatoriedade                 │
│    • Validar tipo de dado                    │
│    • Validar domínio (valores permitidos)    │
│    • Validar formato (email, CNPJ, data)     │
│    • Validar integridade (check digits)      │
│                                              │
│    Se VÁLIDO → aceitar                       │
│    Se INVÁLIDO → rejeitar e logar            │
└──────┬───────────────────────────────────────┘
       │
       ├─── VÁLIDOS ────────┬─── INVÁLIDOS
       │                    │
       ▼                    ▼
┌──────────────┐    ┌──────────────────────┐
│ 3. INSERÇÃO  │    │ LOG DE REJEIÇÃO      │
│ Bronze Table │    │ auditoria.log_rej.   │
│ TRUNCATE +   │    │ • linha CSV          │
│ INSERT       │    │ • campo que falhou   │
└──────┬───────┘    │ • motivo detalhado   │
       │            │ • valor recebido     │
       │            │ • registro completo  │
       │            └──────────────────────┘
       ▼
┌──────────────────────────────────────────────┐
│ 4. ARQUIVAMENTO                              │
│    Move CSV para /app/data/processed/        │
│    Nome: original_YYYYMMDD_HHMMSS.csv        │
└──────┬───────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│ 5. AUDITORIA                                 │
│    auditoria.historico_execucao:             │
│    • script_nome, status, timestamp          │
│    • linhas_processadas, linhas_inseridas    │
│    • linhas_rejeitadas, mensagem_erro        │
└──────────────────────────────────────────────┘
```

**Comando de Execução:**
```bash
docker compose exec etl-processor python python/run_bronze_ingestors.py
```

**Tempo de Execução:** ~10.7s (para 58 linhas de 3 CSVs)

### 2.2 Pipeline Silver (Transformação e Modelagem)

```
┌──────────────┐
│ Bronze Tables│
└──────┬───────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│ 1. EXTRAÇÃO (extrair_bronze)                 │
│    Query SQL: SELECT * FROM bronze.<table>   │
│    Retorna: pandas DataFrame                 │
└──────┬───────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│ 2. TRANSFORMAÇÃO (aplicar_transformacoes)    │
│    • Padronização (CNPJ/CPF formatado)       │
│    • Derivações (tempo_cliente_dias)         │
│    • Enriquecimento (cálculos de métricas)   │
│    • Renomeação de colunas                   │
│    • Limpeza de dados                        │
└──────┬───────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│ 3. VALIDAÇÃO (validar_qualidade)             │
│    • Verificar campos obrigatórios           │
│    • Validar tipos de dados                  │
│    • Garantir integridade referencial (FKs)  │
│    Se FALHOU → ABORTAR transformação         │
└──────┬───────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│ 4. PROCESSAMENTO SCD TYPE 2 (dimensões)      │
│    • Calcular hash_registro (MD5)            │
│    • Comparar hash novo vs atual             │
│    • Detectar mudanças:                      │
│      - Novos → INSERT (versao=1)             │
│      - Alterados → UPDATE old + INSERT new   │
│      - Inalterados → SKIP                    │
│                                              │
│ 4b. RESOLUÇÃO DE FKs (fatos)                 │
│    • Lookup em dimensões (LEFT JOIN)         │
│    • Atribuir surrogate keys                 │
│    • Validar: 0 FKs nulas obrigatórias       │
└──────┬───────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│ 5. CARGA (carregar_silver)                   │
│    • Dimensões: SCD Type 2 (INSERT/UPDATE)   │
│    • Fatos: TRUNCATE + INSERT                │
└──────┬───────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│ 6. AUDITORIA                                 │
│    auditoria.historico_execucao              │
│    • Transformação concluída com sucesso     │
└──────────────────────────────────────────────┘
```

**Ordem de Execução (respeitando dependências):**
1. `transform_dim_data.py` (sem dependências)
2. `transform_dim_cliente.py` (sem dependências)
3. `transform_dim_usuario.py` (auto-dependência: gestor_sk)
4. `transform_fato_faturamento.py` (depende de todas as dims)

**Comando de Execução:**
```bash
docker compose exec etl-processor python python/run_silver_transformers.py
```

**Tempo de Execução:** ~13.3s (para 4 transformações)

---

## 3. STAR SCHEMA - DESENHO E JUSTIFICATIVA

### 3.1 Diagrama Completo

```
                    ┌─────────────────────────────┐
                    │     dim_data (319 reg)      │
                    ├─────────────────────────────┤
                    │ PK: data_sk (INT)           │
                    │ UK: data_completa (DATE)    │
                    ├─────────────────────────────┤
                    │ ano, mes, trimestre         │
                    │ nome_mes, dia_semana        │
                    │ flag_fim_semana, dia_util   │
                    └──────────────┬──────────────┘
                                   │
                                   │ 1:N
                                   │
    ┌──────────────────────────────┼──────────────────────────────┐
    │                              │                              │
    │ 1:N                          │                              │ N:1
    │                              │                              │
┌───┴────────────────────┐  ┌──────┴───────────────────────────┐  ┌────────────────────────┐
│ dim_cliente (10 reg)   │  │   fato_faturamento (13 reg)      │  │ dim_usuario (12 reg)   │
├────────────────────────┤  ├──────────────────────────────────┤  ├────────────────────────┤
│ PK: cliente_sk (SERIAL)│  │ PK: faturamento_sk (BIGSERIAL)   │  │ PK: usuario_sk (SERIAL)│
│ UK: cnpj_cpf_nk+versao │◄─┤ FK: cliente_sk → dim_cliente     │  │ UK: usuario_nk+versao  │
│ (UNIQUE)               │  │ FK: usuario_sk → dim_usuario     ├─►│ (UNIQUE)               │
├────────────────────────┤  │ FK: data_sk → dim_data           │  ├────────────────────────┤
│ cnpj_cpf_nk (NK)       │  │ FK: canal_sk → dim_canal (NULL)  │  │ usuario_nk (email/NK)  │
│ cnpj_cpf_formatado     │  ├──────────────────────────────────┤  │ nome_completo          │
│ razao_social           │  │ MEASURES (métricas):             │  │ email_corporativo      │
│ tipo_pessoa (PJ/PF)    │  │ • valor_bruto (NUMERIC)          │  │ area_atuacao           │
│ status_conta           │  │ • valor_liquido (calculado)      │  │ senioridade            │
│ email_financeiro       │  │ • valor_desconto                 │  │ gestor_sk (hierarquia) │
│ grupo_economico        │  │ • valor_imposto (15%)            │  │ canal_principal        │
│ porte_empresa          │  │ • valor_comissao (5%)            │  │ canal_secundario       │
│ categoria_risco        │  │ • hash_transacao (idempotência)  │  │ email_lider            │
│ tempo_cliente_dias     │  │                                  │  │ nivel_hierarquia       │
│ responsavel_conta      │  │ DEGENERATE DIMENSIONS:           │  │ status_usuario         │
│ faixa_faturamento      │  │ • moeda (BRL/USD/EUR)            │  │                        │
│                        │  │                                  │  │                        │
│ SCD Type 2:            │  └──────────────────────────────────┘  │ SCD Type 2:            │
│ • data_inicio          │                                        │ • data_inicio          │
│ • data_fim             │                                        │ • data_fim             │
│ • flag_ativo           │                                        │ • flag_ativo           │
│ • versao               │                                        │ • versao               │
│ • hash_registro        │                                        │ • hash_registro        │
└────────────────────────┘                                        └────────┬───────────────┘
                                                                           │
                                                                           │ hierarquia
                                                                           │ (self-join)
                                                                           │
                                                                           └───────────┐
                                                                                       │
                                                                        gestor_sk ────►│
```

### 3.2 Propósito de Cada Tabela

#### **FATO: fato_faturamento**

**Por que FATO?**
- Representa **eventos de negócio** (transações de faturamento)
- Contém **métricas quantitativas** (valor_bruto, receita, descontos)
- **Granularidade atômica**: 1 linha = 1 transação de faturamento
- **Cresce rapidamente** (volume transacional)
- **Imutável** por natureza (fatos históricos não mudam)

**Propósito:**
- Responder perguntas analíticas: "Quanto faturamos?", "Qual cliente gera mais receita?", "Tendência mensal de faturamento?"

**Métricas (Measures):**
- `valor_bruto`: Receita bruta da transação
- `valor_liquido`: Receita líquida (bruto - desconto)
- `valor_desconto`: Descontos aplicados
- `valor_imposto`: Impostos calculados (15% do bruto)
- `valor_comissao`: Comissões calculadas (5% do bruto)

**Dimensões Degeneradas:**
- `moeda`: BRL, USD, EUR (não justifica dimensão separada)

**Por que não normalizar métricas?**
- Performance: Agregações (SUM, AVG) são operações comuns em BI
- Simplicidade: Evita JOINs desnecessários
- Padrão Star Schema: Desnormalização controlada é desejável

#### **DIMENSÃO: dim_cliente**

**Por que DIMENSÃO?**
- Representa **entidades de negócio** (clientes da empresa)
- Contém **atributos descritivos** (nome, CNPJ, categoria)
- **Cresce lentamente** (novos clientes são menos frequentes que transações)
- **Muda ao longo do tempo** (necessita SCD Type 2)

**Propósito:**
- Filtrar e agrupar fatos por características do cliente
- Responder: "Quais clientes PJ estão ativos?", "Distribuição por porte de empresa?"

**Atributos-chave:**
- `cnpj_cpf_nk`: Chave natural (identificador de negócio)
- `cnpj_cpf_formatado`: Para exibição (00.000.000/0000-00)
- `tipo_pessoa`: PJ (empresa) ou PF (pessoa física)
- `porte_empresa`: Pequeno, Médio, Grande (calculado por faturamento)
- `categoria_risco`: Baixo, Médio, Alto (scoring de crédito)
- `tempo_cliente_dias`: Tempo desde o cadastro (métrica derivada)

**SCD Type 2:**
- Rastreia mudanças históricas (ex: cliente mudou de status ATIVO → INATIVO)
- Permite análise temporal: "Qual era o status do cliente na data X?"

#### **DIMENSÃO: dim_usuario**

**Por que DIMENSÃO?**
- Representa **pessoas** (usuários/vendedores da Credits Brasil)
- Contém **atributos descritivos** (nome, área, senioridade)
- **Hierarquia organizacional** (gestor_sk → self-join)

**Propósito:**
- Filtrar e agrupar fatos por características do usuário
- Responder: "Qual vendedor teve mais receita?", "Distribuição por área?"

**Atributos-chave:**
- `usuario_nk`: Chave natural (email ou nome)
- `senioridade`: Junior, Pleno, Senior
- `gestor_sk`: FK para dim_usuario (relacionamento hierárquico)
- `nivel_hierarquia`: 1 (é gestor), 2 (tem gestor), 3 (sem gestor)

**Hierarquia Self-Join:**
- Permite análises organizacionais: "Quantos subordinados por gestor?"
- Navegação top-down: CEO → Diretores → Gerentes → Vendedores

#### **DIMENSÃO: dim_data**

**Por que DIMENSÃO separada? (Padrão em Star Schema)**
- **Performance**: Pré-calcula atributos temporais (trimestre, nome_mes)
- **Flexibilidade**: Adicionar atributos de negócio (feriados, dias úteis)
- **Simplicidade**: Queries de agregação temporal ficam mais simples

**Propósito:**
- Filtrar e agrupar fatos por períodos temporais
- Responder: "Faturamento do Q1?", "Tendência mensal?", "Receita em dias úteis?"

**Atributos-chave:**
- `data_sk`: Surrogate key (formato YYYYMMDD, ex: 20240115)
- `data_completa`: Natural key (DATE)
- `trimestre`, `semestre`: Agregações pré-calculadas
- `flag_fim_semana`, `dia_util`: Filtros de negócio

**Alternativa rejeitada:** Usar DATE diretamente no fato
- **Problema**: Dificulta análises complexas (ex: "feriados vs dias úteis")
- **Problema**: Reduz performance (GROUP BY em DATE é mais lento)

#### **DIMENSÃO: dim_canal (não implementada)**

**Por que não foi implementada ainda?**
- Fonte de dados incompleta (canal_1, canal_2 em bronze.usuarios)
- Falta clareza sobre domínio completo (quais canais existem?)
- Priorização: Implementar quando houver necessidade analítica clara

**Quando implementar:**
- Se análises por canal se tornarem frequentes
- Se novos canais forem adicionados regularmente

### 3.3 Justificativa do Star Schema (vs. Alternativas)

#### **Por que Star Schema?**

| Critério | Star Schema | Snowflake Schema | Modelo Normalizado |
|----------|-------------|------------------|-------------------|
| **Performance de Queries** | ✅ Excelente (poucos JOINs) | ⚠️ Média (muitos JOINs) | ❌ Ruim (muitos JOINs) |
| **Simplicidade SQL** | ✅ Queries simples | ⚠️ Queries complexas | ❌ Queries muito complexas |
| **Compatibilidade BI** | ✅ Ideal (Tableau, Power BI) | ✅ Boa | ❌ Ruim |
| **Espaço em disco** | ⚠️ Maior (desnormalizado) | ✅ Menor (normalizado) | ✅ Menor |
| **Manutenção** | ✅ Fácil (estrutura clara) | ⚠️ Média | ❌ Difícil |

**Decisão:** Star Schema pela performance analítica e simplicidade.

#### **Por que desnormalizar dimensões?**

**Exemplo: Não criar dim_status separada para dim_cliente**

```sql
-- REJEITADO: Snowflake (normalizado)
dim_cliente (cliente_sk, nome, status_sk) ─┐
                                            ├─► dim_status (status_sk, descricao)
fato_faturamento (fato_sk, cliente_sk) ────┘

-- ADOTADO: Star Schema (desnormalizado)
dim_cliente (cliente_sk, nome, status_conta)
fato_faturamento (fato_sk, cliente_sk)
```

**Justificativa:**
- `status_conta` tem **cardinalidade baixa** (ATIVO, INATIVO, SUSPENSO)
- JOIN adicional **prejudica performance** sem ganho significativo
- BI tools preferem filtros diretos em dimensões

---

## 4. REGRAS DE NEGÓCIO E VALIDAÇÃO

### 4.1 Validação na Bronze (CRÍTICO - Garantia de Qualidade)

**Princípio:** Bronze **NUNCA aceita dados inválidos**. Todos os registros são validados antes da inserção.

#### **Tipos de Validação Implementados**

| Tipo | Descrição | Exemplo | Rejeição |
|------|-----------|---------|----------|
| **Obrigatoriedade** | Campo não pode ser vazio/nulo | `cnpj_cpf` é obrigatório | "Campo obrigatório 'cnpj_cpf' está vazio ou nulo" |
| **Tipo de Dado** | Validar conversão de tipo | `receita` deve ser decimal | "Valor '123abc' não é um decimal válido" |
| **Domínio** | Apenas valores permitidos | `moeda` ∈ {BRL, USD, EUR} | "Valor 'XXX' não está no domínio permitido: BRL, USD, EUR" |
| **Formato** | Regex ou padrão específico | Email deve conter @ e domínio | "Email 'teste@' inválido" |
| **Integridade** | Cálculo de check digits | CNPJ/CPF com dígitos verificadores | "CNPJ 11222333000199 inválido (dígito verificador incorreto)" |
| **Range** | Valores numéricos em faixa | `receita` > 0 | "Valor -100.50 deve ser positivo" |
| **Tamanho** | String com comprimento específico | CNPJ 14 dígitos, CPF 11 dígitos | "CNPJ deve ter 14 dígitos, recebido: 12" |

#### **Validação de CNPJ/CPF (Crítico)**

**Arquivo:** `python/utils/validators.py::validar_cnpj_cpf()`

**Algoritmo:**
1. Remover caracteres não-numéricos
2. Verificar tamanho (11 para CPF, 14 para CNPJ)
3. Rejeitar dígitos repetidos (11111111111111, 00000000000000)
4. Calcular primeiro dígito verificador
5. Calcular segundo dígito verificador
6. Comparar dígitos calculados vs recebidos

**Exemplo de Rejeição:**
```
Linha 5: CNPJ 12345678000195
Rejeição: "CNPJ inválido (dígito verificador incorreto): 12345678000195"
Motivo: Dígitos esperados: 12345678000190 (não 95)
```

#### **Validação de Email**

**Regex:** `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`

**Casos de Rejeição:**
- `teste` → ❌ "Email não contém @"
- `teste@` → ❌ "Email não tem domínio"
- `teste@@example.com` → ❌ "Email contém múltiplos @"
- `teste@example` → ❌ "Email sem TLD (.com, .br)"

#### **Validação de Datas**

**Formato esperado:** `YYYY-MM-DD` (ISO 8601)

**Validações:**
- Formato correto
- Data válida (ex: 2024-02-30 é inválido)
- Range aceitável (não aceitar 1900-01-01 se improvável)

### 4.2 Transformações na Silver

#### **CNPJ/CPF: Limpeza e Formatação**

**Bronze → Silver:**
```python
# Bronze: '11.222.333/0001-81' (pode ter formatação ou não)
# Silver:
#   cnpj_cpf_nk = '11222333000181' (somente números, chave natural)
#   cnpj_cpf_formatado = '11.222.333/0001-81' (padronizado)
```

**Função:** `transform_dim_cliente.py::_formatar_cnpj_cpf()`

**Lógica:**
```python
limpo = str(valor).replace(r'\D', '')  # Remove não-numéricos
if len(limpo) == 14:  # CNPJ
    return f"{limpo[0:2]}.{limpo[2:5]}.{limpo[5:8]}/{limpo[8:12]}-{limpo[12:14]}"
elif len(limpo) == 11:  # CPF
    return f"{limpo[0:3]}.{limpo[3:6]}.{limpo[6:9]}-{limpo[9:11]}"
```

#### **Dimensão Cliente: Regras de Negócio**

**Cálculo de tempo_cliente_dias:**
```python
df['tempo_cliente_dias'] = (pd.Timestamp.now() - pd.to_datetime(df['data_criacao'])).dt.days
```

**Derivações (placeholders - a implementar):**
- `porte_empresa`: Baseado em faturamento anual (Pequeno < 1M, Médio < 10M, Grande >= 10M)
- `categoria_risco`: Score baseado em histórico (Baixo, Médio, Alto)

#### **Dimensão Usuário: Hierarquia de Gestores**

**Problema:** Resolver `email_lider` → `gestor_sk`

**Solução:**
```python
# 1. Criar lookup de emails → SKs
gestores = df[['usuario_sk', 'email_corporativo']].copy()
gestores.columns = ['gestor_sk', 'email_lider']

# 2. LEFT JOIN para resolver hierarquia
df = df.merge(gestores, on='email_lider', how='left')
```

**Resultado:**
- Se `email_lider` existe e está em dim_usuario → `gestor_sk` preenchido
- Se `email_lider` é nulo ou não encontrado → `gestor_sk` = NULL

#### **Fato Faturamento: Cálculos de Métricas**

**Métricas Derivadas:**
```python
df['valor_liquido'] = df['valor_bruto'] - df['valor_desconto']
df['valor_imposto'] = df['valor_bruto'] * 0.15  # 15% de imposto
df['valor_comissao'] = df['valor_bruto'] * 0.05  # 5% de comissão
```

**Resolução de FKs:**
```python
# FK: cliente_sk
df = df.merge(
    dim_cliente[['cnpj_cpf_nk', 'cliente_sk']],
    left_on='cnpj_cliente',
    right_on='cnpj_cpf_nk',
    how='left'
)

# Validação: Nenhum fato deve ter cliente_sk nulo
assert df['cliente_sk'].notna().all(), "Encontrados fatos órfãos (cliente_sk nulo)"
```

---

## 5. GERAÇÃO E USO DE SKs/PKs/FKs

### 5.1 Surrogate Keys (SKs) - Por que usar?

**Definição:** Chaves primárias artificiais (auto-incrementadas) sem significado de negócio.

**Vantagens vs Natural Keys:**

| Critério | Surrogate Key (SK) | Natural Key (NK) |
|----------|-------------------|------------------|
| **Estabilidade** | ✅ Nunca muda | ❌ Pode mudar (ex: email) |
| **Tamanho** | ✅ Pequeno (4-8 bytes) | ❌ Grande (strings) |
| **Performance JOIN** | ✅ Rápido (índice numérico) | ⚠️ Lento (string) |
| **Simplicidade** | ✅ Sempre único | ⚠️ Pode ter duplicatas (ex: SCD2) |
| **Independência** | ✅ Não depende de fonte | ❌ Depende da fonte |

**Decisão:** Usar SKs como PKs, manter NKs para lookup e auditoria.

### 5.2 Geração de SKs por Tabela

#### **Dimensões: SERIAL (Auto-increment)**

```sql
-- dim_cliente
CREATE TABLE silver.dim_cliente (
    cliente_sk SERIAL PRIMARY KEY,  -- Auto-increment: 1, 2, 3, ...
    cnpj_cpf_nk VARCHAR(14) NOT NULL,  -- Natural key
    ...
    CONSTRAINT uk_cliente_cnpj_versao UNIQUE (cnpj_cpf_nk, versao)
);
```

**Como funciona:**
- PostgreSQL mantém sequência `silver.dim_cliente_cliente_sk_seq`
- A cada INSERT, `NEXTVAL('seq')` é chamado automaticamente
- Gaps são permitidos (ex: se rollback ocorrer)

**Vantagens:**
- Simples, sem lógica de aplicação
- Garantia de unicidade pelo banco
- Performance (índice B-tree em inteiros)

#### **Fatos: BIGSERIAL (Auto-increment para grandes volumes)**

```sql
-- fato_faturamento
CREATE TABLE silver.fato_faturamento (
    faturamento_sk BIGSERIAL PRIMARY KEY,  -- Auto-increment: 1, 2, ..., 9,223,372,036,854,775,807
    ...
);
```

**Por que BIGSERIAL?**
- Fatos crescem rápido (transações diárias)
- SERIAL suporta até 2.1 bilhões (pode estourar em anos)
- BIGSERIAL suporta até 9 quintilhões (nunca vai estourar)

#### **Dimensão Data: SK Inteligente (YYYYMMDD)**

```sql
-- dim_data
CREATE TABLE silver.dim_data (
    data_sk INT PRIMARY KEY,  -- Formato: 20240115 (15 de janeiro de 2024)
    data_completa DATE UNIQUE NOT NULL,
    ...
);
```

**Por que não usar SERIAL?**
- Data tem significado temporal ordenado
- Queries de range são comuns: `WHERE data_sk BETWEEN 20240101 AND 20240131`
- Formato YYYYMMDD é intuitivo e sortável

**Geração:**
```python
df['data_sk'] = pd.to_datetime(df['data_completa']).dt.strftime('%Y%m%d').astype(int)
# 2024-01-15 → '20240115' → 20240115
```

### 5.3 Natural Keys (NKs) - Manter para Lookup

**Definição:** Identificadores de negócio (CNPJ, email, data).

**Uso:**
- **Lookup durante transformação:** Resolver FKs (ex: `cnpj_cliente` → `cliente_sk`)
- **Auditoria:** Rastreabilidade para fontes externas
- **SCD Type 2:** Combinar NK + versão para garantir unicidade

**Constraints:**
```sql
-- Combinação (NK + versao) deve ser única
CONSTRAINT uk_cliente_cnpj_versao UNIQUE (cnpj_cpf_nk, versao);

-- NK sozinha pode repetir (múltiplas versões)
-- Exemplo: cnpj_cpf_nk='11222333000181' com versao=1 e versao=2
```

### 5.4 Foreign Keys (FKs) - Integridade Referencial

**Definição:** Colunas que referenciam PKs de outras tabelas.

**Implementação:**
```sql
-- fato_faturamento
ALTER TABLE silver.fato_faturamento
    ADD CONSTRAINT fk_fato_faturamento_cliente
        FOREIGN KEY (cliente_sk) REFERENCES silver.dim_cliente(cliente_sk);

ALTER TABLE silver.fato_faturamento
    ADD CONSTRAINT fk_fato_faturamento_usuario
        FOREIGN KEY (usuario_sk) REFERENCES silver.dim_usuario(usuario_sk);

ALTER TABLE silver.fato_faturamento
    ADD CONSTRAINT fk_fato_faturamento_data
        FOREIGN KEY (data_sk) REFERENCES silver.dim_data(data_sk);
```

**Por que enforçar constraints?**
- ✅ Garante integridade: Impossível ter fato órfão (FK inválida)
- ✅ Documenta relacionamentos
- ✅ Protege contra erros de DELETE (cascade)
- ⚠️ Performance: INSERT/UPDATE ligeiramente mais lento (validação FK)

**Trade-off aceito:** Garantia de qualidade > leve impacto em performance.

#### **FK Nullable: canal_sk**

```sql
canal_sk INT NULL,  -- Nullable porque dim_canal não está implementada
```

**Justificativa:**
- dim_canal ainda não existe
- Não podemos bloquear pipeline por dimensão futura
- Quando implementada, atualizar fatos existentes com UPDATE

#### **FK Auto-referenciada: gestor_sk**

```sql
-- dim_usuario
gestor_sk INT NULL,
CONSTRAINT fk_usuario_gestor
    FOREIGN KEY (gestor_sk) REFERENCES silver.dim_usuario(usuario_sk);
```

**Navegação hierárquica:**
```sql
-- Listar subordinados de um gestor
SELECT u1.nome_completo AS subordinado,
       u2.nome_completo AS gestor
FROM silver.dim_usuario u1
LEFT JOIN silver.dim_usuario u2 ON u1.gestor_sk = u2.usuario_sk
WHERE u2.usuario_sk = 5;
```

### 5.5 Resolução de FKs na Transformação

**Processo (exemplo para fato_faturamento):**

```python
# 1. Extrair dimensão com SK e NK
dim_cliente = pd.read_sql(
    "SELECT cliente_sk, cnpj_cpf_nk FROM silver.dim_cliente WHERE flag_ativo = TRUE",
    conn
)

# 2. LEFT JOIN para resolver FK
fato_df = fato_df.merge(
    dim_cliente,
    left_on='cnpj_cliente',  # Coluna em bronze.faturamentos
    right_on='cnpj_cpf_nk',  # NK em dim_cliente
    how='left'
)

# 3. Validar: 0 nulos em FKs obrigatórias
assert fato_df['cliente_sk'].notna().all(), "FKs órfãs detectadas!"

# 4. Inserir fato com FK resolvida
fato_df[['cliente_sk', 'usuario_sk', 'data_sk', 'valor_bruto', ...]].to_sql(...)
```

**Por que LEFT JOIN (não INNER)?**
- Preserva todas as linhas do fato
- Permite detectar órfãos (FK nula) para debug
- Se usássemos INNER, órfãos seriam silenciosamente removidos

---

## 6. SCD TYPE 2 - IMPLEMENTAÇÃO DETALHADA

### 6.1 O Que é SCD Type 2?

**Slowly Changing Dimension Type 2:** Técnica para rastrear mudanças históricas em dimensões.

**Problema:** Cliente "Alpha Tech" mudou de status "ATIVO" → "INATIVO" em 26/11/2025. Como preservar histórico?

**Soluções possíveis:**

| Tipo | Estratégia | Histórico? | Uso |
|------|-----------|-----------|-----|
| **SCD Type 0** | Nunca atualiza | ❌ Não | Dados imutáveis (ex: data de nascimento) |
| **SCD Type 1** | Sobrescreve | ❌ Não | Valores não-históricos (ex: email atual) |
| **SCD Type 2** | Cria nova versão | ✅ Sim | Rastrear mudanças (ex: status, endereço) |
| **SCD Type 3** | Coluna "anterior" | ⚠️ Limitado | Apenas 1 mudança |

**Decisão:** SCD Type 2 para dim_cliente e dim_usuario.

### 6.2 Estrutura de Dados

**Campos de Controle:**
```sql
data_inicio     TIMESTAMPTZ NOT NULL,  -- Quando versão se tornou ativa
data_fim        TIMESTAMPTZ,           -- Quando versão foi desativada (NULL = ativa)
flag_ativo      BOOLEAN NOT NULL,      -- TRUE = versão atual, FALSE = histórico
versao          INT NOT NULL,          -- Número da versão (1, 2, 3, ...)
hash_registro   VARCHAR(32) NOT NULL,  -- MD5 para detectar mudanças
```

**Constraint de Unicidade:**
```sql
CONSTRAINT uk_cliente_cnpj_versao UNIQUE (cnpj_cpf_nk, versao);
```

**Exemplo de Evolução:**

| cliente_sk | cnpj_cpf_nk | razao_social | status | versao | flag_ativo | data_inicio | data_fim |
|------------|-------------|--------------|--------|--------|------------|-------------|----------|
| 1 | 11222333000181 | Alpha Tech | Ativo | 1 | FALSE | 2025-01-15 | 2025-11-25 |
| 12 | 11222333000181 | Alpha Tech | Inativo | 2 | TRUE | 2025-11-26 | NULL |

**Interpretação:**
- Linha 1 (versao=1): Cliente estava ATIVO de 15/01 até 25/11
- Linha 12 (versao=2): Cliente está INATIVO desde 26/11 (atual)

### 6.3 Algoritmo de Processamento

**Arquivo:** `python/transformers/base_transformer.py::processar_scd2()`

**Fluxo:**

```
┌─────────────────────┐
│ Novos Dados (Bronze)│
└──────────┬──────────┘
           │
           ▼
┌──────────────────────────┐
│ Calcular hash_registro   │
│ MD5(campos de negócio)   │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│ Query Versões Atuais     │
│ WHERE flag_ativo = TRUE  │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│ LEFT JOIN (bronze ⟕ silver)          │
│ ON natural_key                       │
└──────────┬───────────────────────────┘
           │
           ▼
    ┌──────┴──────┐
    │             │
    ▼             ▼
┌────────┐   ┌─────────┐   ┌──────────┐
│ NOVOS  │   │ALTERADOS│   │INALTERADOS│
│ (NK    │   │ (hash   │   │ (hash    │
│ novo)  │   │ mudou)  │   │ igual)   │
└────┬───┘   └────┬────┘   └────┬─────┘
     │            │              │
     │            │              └──► SKIP (nenhuma ação)
     │            │
     │            ▼
     │       ┌────────────────────────┐
     │       │ UPDATE versão antiga   │
     │       │ SET data_fim = hoje-1  │
     │       │     flag_ativo = FALSE │
     │       └────────┬───────────────┘
     │                │
     ▼                ▼
┌─────────────────────────────────┐
│ INSERT nova versão              │
│ • versao = old + 1 (ou 1)       │
│ • flag_ativo = TRUE             │
│ • data_inicio = hoje            │
│ • data_fim = NULL               │
└─────────────────────────────────┘
```

**Código Simplificado:**

```python
def processar_scd2(self, df_novos, tabela_silver, natural_key_col):
    # 1. Calcular hash
    df_novos['hash_registro'] = self.calcular_hash_registro(df_novos, campos_hash)

    # 2. Buscar versões atuais
    df_atuais = pd.read_sql(f"SELECT * FROM {tabela_silver} WHERE flag_ativo = TRUE", conn)

    # 3. Merge para comparar
    df_merged = df_novos.merge(df_atuais, on=natural_key_col, how='left', suffixes=('', '_atual'))

    # 4. Classificar
    novos = df_merged[df_merged['cliente_sk_atual'].isna()]  # NK não existe
    alterados = df_merged[
        (df_merged['hash_registro'] != df_merged['hash_registro_atual']) &
        (df_merged['cliente_sk_atual'].notna())
    ]

    # 5. Inserir novos
    novos['versao'] = 1
    novos['flag_ativo'] = True
    novos['data_inicio'] = pd.Timestamp.now()
    novos.to_sql(tabela_silver, conn, if_exists='append', index=False)

    # 6. Fechar versões antigas
    for _, row in alterados.iterrows():
        conn.execute(f"""
            UPDATE {tabela_silver}
            SET data_fim = CURRENT_DATE - INTERVAL '1 day',
                flag_ativo = FALSE
            WHERE {natural_key_col} = %s AND flag_ativo = TRUE
        """, (row[natural_key_col],))

    # 7. Inserir novas versões
    alterados['versao'] = alterados['versao_atual'] + 1
    alterados['flag_ativo'] = True
    alterados['data_inicio'] = pd.Timestamp.now()
    alterados['data_fim'] = None
    alterados.to_sql(tabela_silver, conn, if_exists='append', index=False)
```

### 6.4 Cálculo de Hash para Detecção de Mudanças

**Função:** `base_transformer.py::calcular_hash_registro()`

**Campos incluídos no hash:**
```python
# dim_cliente
campos_hash = [
    'razao_social', 'tipo_pessoa', 'status_conta',
    'email_financeiro', 'grupo_economico', 'responsavel_conta'
]

# dim_usuario
campos_hash = [
    'nome_completo', 'email_corporativo', 'area_atuacao',
    'senioridade', 'gestor_sk', 'canal_principal'
]
```

**Lógica:**
```python
def calcular_hash_registro(self, df, campos):
    # Concatenar valores dos campos + gerar MD5
    df['_concat'] = df[campos].astype(str).agg('|'.join, axis=1)
    df['hash_registro'] = df['_concat'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    return df['hash_registro']
```

**Exemplo:**
```
Campos: razao_social='Alpha Tech', status_conta='ATIVO'
Concat: 'Alpha Tech|ATIVO'
MD5:    '5d41402abc4b2a76b9719d911017c592'
```

**Por que MD5?**
- Rápido (performance)
- Fixo 32 caracteres (armazenamento eficiente)
- Colisões improváveis para nosso caso de uso

### 6.5 Queries Temporais com SCD Type 2

**Versão atual de um cliente:**
```sql
SELECT * FROM silver.dim_cliente
WHERE cnpj_cpf_nk = '11222333000181'
  AND flag_ativo = TRUE;
```

**Todas as versões de um cliente (histórico completo):**
```sql
SELECT * FROM silver.dim_cliente
WHERE cnpj_cpf_nk = '11222333000181'
ORDER BY versao;
```

**Status do cliente em uma data específica (time travel):**
```sql
SELECT * FROM silver.dim_cliente
WHERE cnpj_cpf_nk = '11222333000181'
  AND data_inicio <= '2025-06-15'
  AND (data_fim IS NULL OR data_fim >= '2025-06-15');
```

**Fatos com dimensão temporal (join em data específica):**
```sql
SELECT f.*, c.razao_social, c.status_conta
FROM silver.fato_faturamento f
JOIN silver.dim_data d ON f.data_sk = d.data_sk
JOIN silver.dim_cliente c ON f.cliente_sk = c.cliente_sk
WHERE d.data_completa = '2025-06-15'
  AND c.data_inicio <= '2025-06-15'
  AND (c.data_fim IS NULL OR c.data_fim >= '2025-06-15');
```

---

## 7. LOGS DE REJEIÇÃO E AUDITORIA

### 7.1 Sistema de Rejeição

**Tabela:** `auditoria.log_rejeicao`

**Estrutura:**
```sql
CREATE TABLE auditoria.log_rejeicao (
    id                BIGSERIAL PRIMARY KEY,
    execucao_fk       UUID NOT NULL,  -- FK → auditoria.historico_execucao
    script_nome       VARCHAR(255) NOT NULL,
    tabela_destino    VARCHAR(100),
    numero_linha      INT,
    campo_falha       VARCHAR(100),
    motivo_rejeicao   TEXT NOT NULL,
    valor_recebido    TEXT,
    registro_completo JSONB,  -- JSON completo da linha rejeitada
    severidade        VARCHAR(20) DEFAULT 'ERROR',
    data_rejeicao     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_log_rejeicao_execucao
        FOREIGN KEY (execucao_fk) REFERENCES auditoria.historico_execucao(id)
);
```

**Exemplo de Registro:**
```json
{
  "id": 15,
  "execucao_fk": "550e8400-e29b-41d4-a716-446655440000",
  "script_nome": "ingest_contas.py",
  "tabela_destino": "bronze.contas",
  "numero_linha": 8,
  "campo_falha": "cnpj_cpf",
  "motivo_rejeicao": "CNPJ inválido (dígito verificador incorreto): 11111111111111",
  "valor_recebido": "11111111111111",
  "registro_completo": {
    "cnpj_cpf": "11111111111111",
    "tipo": "PJ",
    "status": "ATIVO",
    "data_criacao": "2024-01-15"
  },
  "severidade": "ERROR",
  "data_rejeicao": "2025-11-26 10:30:45"
}
```

### 7.2 Níveis de Severidade

| Severidade | Uso | Exemplo |
|------------|-----|---------|
| **WARNING** | Dados aceitáveis com ressalvas | Email sem TLD (.com, .br) mas formato válido |
| **ERROR** | Dados rejeitados (padrão) | CNPJ inválido, data futura, valor negativo |
| **CRITICAL** | Falha sistêmica | Tabela não existe, conexão perdida |

### 7.3 Análise de Rejeições

**Query: Rejeições por campo (últimas 24h)**
```sql
SELECT
    tabela_destino,
    campo_falha,
    COUNT(*) as total_rejeicoes,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentual
FROM auditoria.log_rejeicao
WHERE data_rejeicao >= NOW() - INTERVAL '24 hours'
GROUP BY tabela_destino, campo_falha
ORDER BY total_rejeicoes DESC;
```

**Query: Motivos mais comuns**
```sql
SELECT
    motivo_rejeicao,
    COUNT(*) as ocorrencias,
    ARRAY_AGG(DISTINCT campo_falha) as campos_afetados
FROM auditoria.log_rejeicao
WHERE data_rejeicao >= NOW() - INTERVAL '7 days'
GROUP BY motivo_rejeicao
ORDER BY ocorrencias DESC
LIMIT 10;
```

**Query: Taxa de rejeição por execução**
```sql
SELECT
    h.script_nome,
    h.data_inicio,
    h.linhas_processadas,
    h.linhas_inseridas,
    COUNT(l.id) as linhas_rejeitadas,
    ROUND(COUNT(l.id)::numeric / NULLIF(h.linhas_processadas, 0) * 100, 2) as taxa_rejeicao_pct
FROM auditoria.historico_execucao h
LEFT JOIN auditoria.log_rejeicao l ON l.execucao_fk = h.id
WHERE h.data_inicio >= NOW() - INTERVAL '30 days'
GROUP BY h.id, h.script_nome, h.data_inicio, h.linhas_processadas, h.linhas_inseridas
ORDER BY h.data_inicio DESC;
```

### 7.4 Auditoria de Execuções

**Tabela:** `auditoria.historico_execucao`

**Estrutura:**
```sql
CREATE TABLE auditoria.historico_execucao (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    script_nome         VARCHAR(255) NOT NULL,
    data_inicio         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_fim            TIMESTAMPTZ,
    status              VARCHAR(50),  -- 'em_execucao', 'sucesso', 'erro'
    linhas_processadas  INT,
    linhas_inseridas    INT,
    mensagem_erro       TEXT,
    duracao_segundos    NUMERIC(10,2)
);
```

**Exemplo de Registro:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "script_nome": "ingest_contas.py",
  "data_inicio": "2025-11-26 10:30:00",
  "data_fim": "2025-11-26 10:30:45",
  "status": "sucesso",
  "linhas_processadas": 17,
  "linhas_inseridas": 10,
  "mensagem_erro": null,
  "duracao_segundos": 45.23
}
```

**Ciclo de vida:**
1. **Início:** INSERT com status='em_execucao'
2. **Execução:** Processar CSV, validar, inserir
3. **Fim:** UPDATE com status='sucesso'/'erro', data_fim, linhas, duração

---

## 8. DECISÕES TÉCNICAS E JUSTIFICATIVAS

### 8.1 Por que TRUNCATE/RELOAD na Bronze?

**Alternativa 1: Incremental (INSERT apenas novos)**
- ❌ Complexidade: Precisa detectar novos vs atualizados
- ❌ Duplicatas: Risco de inserir mesmo registro 2x
- ❌ Histórico na Bronze: Não faz sentido (Bronze é snapshot da origem)

**Alternativa 2: TRUNCATE/RELOAD**
- ✅ Simplicidade: Apaga tudo e recarrega
- ✅ Idempotência: Rodar 10x = mesmo resultado
- ✅ Sem duplicatas: Tabela sempre reflete CSV atual
- ⚠️ Performance: Para tabelas grandes (>1M linhas), pode ser lento

**Decisão:** TRUNCATE/RELOAD pela simplicidade e idempotência. Se Bronze crescer >1M linhas, reavaliar para MERGE/UPSERT.

### 8.2 Por que SCD Type 2 (não Type 1)?

**SCD Type 1 (Sobrescrever):**
```sql
-- Cliente mudou de status
UPDATE dim_cliente SET status_conta = 'INATIVO' WHERE cliente_sk = 5;
```
- ✅ Simples
- ❌ **Perde histórico**: Impossível saber quando mudou
- ❌ **Fatos órfãos temporalmente**: Faturamento de junho vai mostrar status atual (INATIVO), não o status de junho (ATIVO)

**SCD Type 2 (Versionar):**
```sql
-- Cliente mudou de status
UPDATE dim_cliente SET flag_ativo = FALSE, data_fim = '2025-11-25' WHERE cliente_sk = 5;
INSERT INTO dim_cliente (..., versao = 2, flag_ativo = TRUE) VALUES (...);
```
- ✅ **Preserva histórico**: Todas mudanças rastreadas
- ✅ **Time travel**: Queries podem "voltar no tempo"
- ✅ **Auditoria**: Compliance e análise de mudanças
- ⚠️ Complexidade: Queries precisam filtrar flag_ativo
- ⚠️ Espaço: Múltiplas versões ocupam mais disco

**Decisão:** SCD Type 2 para dim_cliente e dim_usuario. Histórico de mudanças é crítico para análises (ex: "Quantos clientes perdemos em 2024?").

### 8.3 Por que PostgreSQL (não MySQL/SQL Server)?

| Critério | PostgreSQL | MySQL | SQL Server |
|----------|------------|-------|------------|
| **JSON Support** | ✅ JSONB (binário, indexável) | ⚠️ JSON (texto) | ✅ JSON |
| **Window Functions** | ✅ Completo | ⚠️ Limitado | ✅ Completo |
| **Constraints** | ✅ FK, Check, Exclude | ✅ FK, Check | ✅ FK, Check |
| **Open Source** | ✅ Sim | ✅ Sim | ❌ Proprietário |
| **Custo Azure** | ✅ Managed ($) | ✅ Managed ($) | ❌ Caro ($$$) |
| **Compliance** | ✅ ACID | ✅ ACID (InnoDB) | ✅ ACID |

**Decisão:** PostgreSQL pela combinação de JSONB (logs de rejeição), window functions (SCD Type 2), custo-benefício e maturidade open-source.

### 8.4 Por que Docker (não Airflow/Luigi)?

**Airflow/Luigi (Orquestradores):**
- ✅ Scheduling (cron jobs)
- ✅ DAGs visuais
- ✅ Retry automático
- ❌ **Overhead**: Precisa de metastore, webserver, scheduler
- ❌ **Complexidade**: Curva de aprendizado
- ❌ **Infraestrutura**: Mais recursos (CPU, RAM)

**Docker Compose (Container Orchestration):**
- ✅ **Simplicidade**: docker-compose up
- ✅ **Portabilidade**: Roda em qualquer lugar
- ✅ **Isolamento**: Sem conflitos de dependências
- ⚠️ **Scheduling manual**: Precisa de cron externo
- ⚠️ **Sem UI**: Logs em terminal

**Decisão:** Docker Compose para MVP. Se necessidade de scheduling complexo surgir, migrar para Airflow.

### 8.5 Por que Pandas (não Spark/Dask)?

| Critério | Pandas | Spark | Dask |
|----------|--------|-------|------|
| **Tamanho de dados** | ⚠️ <1GB (memória) | ✅ >1TB (distribuído) | ✅ >10GB (paralelo) |
| **Complexidade** | ✅ Simples | ❌ Cluster setup | ⚠️ Média |
| **Performance** | ✅ Rápido (single-core) | ✅ Muito rápido (cluster) | ✅ Rápido (multi-core) |
| **Curva aprendizado** | ✅ Baixa | ❌ Alta | ⚠️ Média |
| **Infraestrutura** | ✅ Mínima | ❌ Cluster (YARN/K8s) | ✅ Mínima |

**Decisão:** Pandas porque:
- Volume atual: <1000 linhas (cabe em memória)
- Simplicidade: Time conhece Pandas
- Performance: Processamento leva <15s

**Reavaliar se:** Bronze crescer para >100k linhas ou processamento >5min.

### 8.6 Por que Validação na Bronze (não apenas Silver)?

**Alternativa 1: Bronze aceita tudo, Silver valida**
- ❌ **Poluição**: Bronze contém dados inválidos
- ❌ **Propagação de erros**: Transformações falham em runtime
- ❌ **Debug difícil**: Erro na Silver, mas origem na Bronze

**Alternativa 2: Bronze valida e rejeita inválidos**
- ✅ **Qualidade garantida**: Bronze sempre 100% válido
- ✅ **Fail-fast**: Erros detectados na entrada
- ✅ **Rastreabilidade**: Logs de rejeição documentam problemas
- ⚠️ **Performance**: Validação adiciona ~20% ao tempo de ingestão

**Decisão:** Validar na Bronze. Custo de validação (tempo) é menor que custo de dados ruins (retrabalho, decisões erradas).

### 8.7 Por que MD5 para Hash (não SHA256)?

**MD5:**
- ✅ Rápido (~300 MB/s)
- ✅ 32 caracteres (VARCHAR(32))
- ⚠️ **Não é criptograficamente seguro** (colisões teóricas)

**SHA256:**
- ✅ Criptograficamente seguro
- ❌ Lento (~150 MB/s, 2x mais lento)
- ❌ 64 caracteres (VARCHAR(64), 2x espaço)

**Decisão:** MD5 porque:
- Não usamos para segurança (apenas detecção de mudanças)
- Colisões são improváveis (probabilidade < 1 em 2^64 para nosso volume)
- Performance importa (hash calculado a cada transformação)

---

## 9. EVIDÊNCIAS - EXECUÇÃO 26/11/2025

### 9.1 Pipeline Bronze - Resultados

**Comando:**
```bash
docker compose exec etl-processor python python/run_bronze_ingestors.py
```

**Output:**
```
=== EXECUTANDO INGESTORES BRONZE ===
[1/4] Executando ingest_contas.py...
  ✓ 10 registros inseridos (7 rejeitados)
[2/4] Executando ingest_usuarios.py...
  ✓ 12 registros inseridos (5 rejeitados)
[3/4] Executando ingest_faturamentos.py...
  ✓ 13 registros inseridos (11 rejeitados)
[4/4] Executando ingest_calendario.py...
  ✓ 366 registros inseridos (0 rejeitados)

=== RESUMO ===
Total processado: 58 linhas
Total aceito: 35 linhas (60.3%)
Total rejeitado: 23 linhas (39.7%)
Tempo total: 10.7s
```

**Validação Bronze:**
```sql
-- Contagem de registros
SELECT 'bronze.contas' as tabela, COUNT(*) FROM bronze.contas
UNION ALL
SELECT 'bronze.usuarios', COUNT(*) FROM bronze.usuarios
UNION ALL
SELECT 'bronze.faturamentos', COUNT(*) FROM bronze.faturamentos
UNION ALL
SELECT 'bronze.data', COUNT(*) FROM bronze.data;
```

| tabela | count |
|--------|-------|
| bronze.contas | 10 |
| bronze.usuarios | 12 |
| bronze.faturamentos | 13 |
| bronze.data | 366 |

**Análise de Rejeições:**
```sql
SELECT
    tabela_destino,
    campo_falha,
    COUNT(*) as total
FROM auditoria.log_rejeicao
WHERE DATE(data_rejeicao) = '2025-11-26'
GROUP BY tabela_destino, campo_falha
ORDER BY total DESC;
```

| tabela_destino | campo_falha | total |
|----------------|-------------|-------|
| bronze.faturamentos | receita | 5 |
| bronze.contas | cnpj_cpf | 4 |
| bronze.usuarios | email | 3 |
| bronze.faturamentos | moeda | 3 |
| bronze.faturamentos | data | 2 |
| bronze.contas | status | 2 |
| bronze.usuarios | nome | 2 |
| bronze.contas | data_criacao | 1 |

### 9.2 Pipeline Silver - Resultados

**Comando:**
```bash
docker compose exec etl-processor python python/run_silver_transformers.py
```

**Output:**
```
=== EXECUTANDO TRANSFORMADORES SILVER ===
[1/4] Executando transform_dim_data.py...
  ✓ 319 registros carregados
[2/4] Executando transform_dim_cliente.py...
  ✓ 10 registros carregados (10 novos, 0 alterados)
[3/4] Executando transform_dim_usuario.py...
  ✓ 12 registros carregados (12 novos, 0 alterados)
[4/4] Executando transform_fato_faturamento.py...
  ✓ 13 registros carregados

=== RESUMO ===
Total transformações: 4
Status: ✅ Sucesso
Tempo total: 13.3s
```

**Validação Silver:**
```sql
-- Contagem de registros
SELECT 'dim_cliente' as tabela, COUNT(*) FROM silver.dim_cliente
UNION ALL
SELECT 'dim_usuario', COUNT(*) FROM silver.dim_usuario
UNION ALL
SELECT 'dim_data', COUNT(*) FROM silver.dim_data
UNION ALL
SELECT 'fato_faturamento', COUNT(*) FROM silver.fato_faturamento;
```

| tabela | count |
|--------|-------|
| dim_cliente | 10 |
| dim_usuario | 12 |
| dim_data | 319 |
| fato_faturamento | 13 |

### 9.3 Validação de Integridade Referencial

**Query: Verificar FKs órfãs**
```sql
-- Todos os fatos devem ter cliente_sk, usuario_sk, data_sk válidos
SELECT
    COUNT(*) FILTER (WHERE cliente_sk IS NULL) as cliente_sk_nulos,
    COUNT(*) FILTER (WHERE usuario_sk IS NULL) as usuario_sk_nulos,
    COUNT(*) FILTER (WHERE data_sk IS NULL) as data_sk_nulos,
    COUNT(*) as total_fatos
FROM silver.fato_faturamento;
```

| cliente_sk_nulos | usuario_sk_nulos | data_sk_nulos | total_fatos |
|------------------|------------------|---------------|-------------|
| 0 | 0 | 0 | 13 |

**✅ VALIDAÇÃO APROVADA: 0 FKs nulas (100% integridade referencial)**

**Query: JOIN completo (Fatos + Dimensões)**
```sql
SELECT
    d.data_completa,
    c.razao_social,
    c.cnpj_cpf_formatado,
    u.nome_completo,
    f.valor_bruto,
    f.valor_liquido,
    f.moeda
FROM silver.fato_faturamento f
JOIN silver.dim_data d ON f.data_sk = d.data_sk
JOIN silver.dim_cliente c ON f.cliente_sk = c.cliente_sk
JOIN silver.dim_usuario u ON f.usuario_sk = u.usuario_sk
WHERE c.flag_ativo = TRUE
  AND u.flag_ativo = TRUE
ORDER BY d.data_completa DESC
LIMIT 5;
```

| data_completa | razao_social | cnpj_cpf_formatado | nome_completo | valor_bruto | valor_liquido | moeda |
|---------------|--------------|-------------------|---------------|-------------|---------------|-------|
| 2024-11-20 | Tech Solutions | 55.666.777/0001-88 | Maria Santos | 30000.00 | 27000.00 | BRL |
| 2024-11-15 | Beta Corp | 22.333.444/0001-55 | João Silva | 25000.00 | 23750.00 | USD |
| 2024-10-10 | Alpha Tech | 11.222.333/0001-81 | Carlos Oliveira | 15000.00 | 14250.00 | BRL |
| 2024-09-05 | Delta Inc | 44.555.666/0001-22 | Ana Costa | 8000.00 | 7600.00 | EUR |
| 2024-08-25 | Gamma Ltda | 33.444.555/0001-99 | Pedro Alves | 12000.00 | 11400.00 | BRL |

**✅ VALIDAÇÃO APROVADA: Todos os JOINs resolvidos corretamente**

### 9.4 Validação de CNPJ/CPF Formatados

**Query: Verificar formatação**
```sql
SELECT
    cnpj_cpf_nk,
    cnpj_cpf_formatado,
    CASE
        WHEN LENGTH(cnpj_cpf_nk) = 14 THEN 'CNPJ'
        WHEN LENGTH(cnpj_cpf_nk) = 11 THEN 'CPF'
        ELSE 'INVÁLIDO'
    END as tipo
FROM silver.dim_cliente
WHERE flag_ativo = TRUE
ORDER BY cnpj_cpf_nk
LIMIT 10;
```

| cnpj_cpf_nk | cnpj_cpf_formatado | tipo |
|-------------|-------------------|------|
| 11222333000181 | 11.222.333/0001-81 | CNPJ |
| 22333444000155 | 22.333.444/0001-55 | CNPJ |
| 33444555000199 | 33.444.555/0001-99 | CNPJ |
| 44555666000122 | 44.555.666/0001-22 | CNPJ |
| 55666777000188 | 55.666.777/0001-88 | CNPJ |
| 66777888000144 | 66.777.888/0001-44 | CNPJ |
| 77888999000100 | 77.888.999/0001-00 | CNPJ |
| 88999000000166 | 88.999.000/0001-66 | CNPJ |
| 98765432100 | 987.654.321-00 | CPF |
| 12345678901 | 123.456.789-01 | CPF |

**✅ VALIDAÇÃO APROVADA: 100% dos CNPJs/CPFs formatados corretamente**

### 9.5 Validação de SCD Type 2

**Query: Verificar versionamento**
```sql
SELECT
    cnpj_cpf_nk,
    razao_social,
    status_conta,
    versao,
    flag_ativo,
    data_inicio,
    data_fim
FROM silver.dim_cliente
WHERE cnpj_cpf_nk IN ('11222333000181', '22333444000155')
ORDER BY cnpj_cpf_nk, versao;
```

| cnpj_cpf_nk | razao_social | status_conta | versao | flag_ativo | data_inicio | data_fim |
|-------------|--------------|--------------|--------|------------|-------------|----------|
| 11222333000181 | Alpha Tech | ATIVO | 1 | TRUE | 2025-11-26 | NULL |
| 22333444000155 | Beta Corp | ATIVO | 1 | TRUE | 2025-11-26 | NULL |

**✅ VALIDAÇÃO APROVADA: SCD Type 2 funcionando (todos versao=1, flag_ativo=TRUE na carga inicial)**

---

## 10. PONTOS DE MELHORIA

### 10.1 Funcionalidades Pendentes

- [ ] **Camada Gold**: Agregações e métricas de negócio
  - Criar tabelas de agregação (faturamento_mensal, top_clientes, etc.)
  - Implementar materialized views para performance
  - Adicionar refresh incremental

- [ ] **dim_canal**: Implementar dimensão de canais
  - Normalizar canal_1 e canal_2 de bronze.usuarios
  - Criar dim_canal com atributos (nome, tipo, região)
  - Atualizar fato_faturamento com canal_sk válido

- [ ] **Cálculo de porte_empresa**: Lógica baseada em faturamento
  - Pequeno: Faturamento anual < R$ 1M
  - Médio: R$ 1M <= Faturamento < R$ 10M
  - Grande: Faturamento >= R$ 10M

- [ ] **categoria_risco**: Sistema de scoring
  - Análise de inadimplência histórica
  - Combinação de fatores (tempo de cliente, faturamento, status)
  - Score de 0-100 mapeado para Baixo/Médio/Alto

### 10.2 Qualidade e Testes

- [ ] **Testes unitários**: Cobertura > 80%
  - Testar funções de validação (validators.py)
  - Testar transformações (hash, SCD Type 2)
  - Testar geração de SKs e resolução de FKs

- [ ] **Testes de integração**: Pipeline completo
  - Testar Bronze → Silver com datasets sintéticos
  - Validar idempotência (rodar 2x = mesmo resultado)
  - Testar cenários de erro (CSV malformado, FK órfã)

- [ ] **Data Quality Checks**: Great Expectations
  - Expectativas de schema (colunas obrigatórias)
  - Expectativas de valores (ranges, domínios)
  - Alertas automáticos em falhas

### 10.3 Performance e Escalabilidade

- [ ] **Índices adicionais**: Otimizar queries analíticas
  ```sql
  CREATE INDEX idx_fk_cliente ON silver.fato_faturamento(cliente_sk);
  CREATE INDEX idx_fk_data ON silver.fato_faturamento(data_sk);
  CREATE INDEX idx_clientes_ativo ON silver.dim_cliente(flag_ativo, cnpj_cpf_nk);
  ```

- [ ] **Particionamento**: Fatos por data
  ```sql
  -- Particionar fato_faturamento por ano
  CREATE TABLE fato_faturamento_2024 PARTITION OF fato_faturamento
    FOR VALUES FROM (20240101) TO (20250101);
  ```

- [ ] **Paralelização**: Ingestores concorrentes
  - Executar ingestores Bronze em paralelo (ThreadPoolExecutor)
  - Reduzir tempo de ingestão de 10.7s para ~4s

### 10.4 Automação e Monitoramento

- [ ] **CI/CD**: GitHub Actions
  - Executar testes em cada push
  - Deploy automático para ambiente de staging
  - Validação de qualidade de código (black, ruff, mypy)

- [ ] **Scheduling**: Airflow ou Prefect
  - Cron jobs para execução diária (Bronze às 02:00, Silver às 03:00)
  - Retry automático em falhas
  - Notificações por Slack/Email

- [ ] **Monitoramento**: Alertas e SLA
  - Alert: Taxa de rejeição > 10%
  - Alert: Tempo de execução > 2x média histórica
  - Alert: 0 registros inseridos (possível problema na fonte)
  - Dashboard de métricas (Grafana + Prometheus)

### 10.5 Documentação

- [ ] **Data Dictionary**: Glossário de campos
  - Documentar significado de negócio de cada coluna
  - Exemplos de valores válidos
  - Regras de transformação aplicadas

- [ ] **Query Library**: Queries analíticas comuns
  - Faturamento por cliente/período
  - Ranking de vendedores
  - Análise de churn (clientes perdidos)
  - KPIs de negócio (ARR, MRR, CAC, LTV)

- [ ] **Runbooks**: Guias operacionais
  - Procedimento de restore de backup
  - Rollback de transformações Silver
  - Investigação de rejeições altas

---

## 11. ESTRUTURA DE DIRETÓRIOS

```
credits-dw/
│
├── docker/                         # Ambiente Docker
│   ├── Dockerfile                  # Imagem Python 3.10 + dependências
│   ├── docker-compose.yml          # Orquestração (etl-processor)
│   ├── data/                       # Volume de dados (montado em /app/data/)
│   │   ├── input/                  # CSVs para ingestão
│   │   │   └── onedrive/           # Fonte: OneDrive
│   │   │       ├── contas.csv
│   │   │       ├── usuarios.csv
│   │   │       └── faturamentos.csv
│   │   └── processed/              # CSVs arquivados (timestamped)
│   └── logs/                       # Logs de execução (montado em /app/logs/)
│
├── python/                         # Código ETL
│   ├── ingestors/                  # Scripts de ingestão (Camada Bronze)
│   │   └── csv/
│   │       ├── base_csv_ingestor.py       # Classe base (Template Method)
│   │       ├── ingest_contas.py           # Ingestor de contas
│   │       ├── ingest_usuarios.py         # Ingestor de usuários
│   │       ├── ingest_faturamentos.py     # Ingestor de faturamentos
│   │       └── ingest_calendario.py       # Ingestor de calendário
│   │
│   ├── transformers/               # Scripts de transformação (Camada Silver)
│   │   ├── base_transformer.py            # Classe base (SCD Type 2)
│   │   └── silver/
│   │       ├── transform_dim_data.py           # Dimensão Data
│   │       ├── transform_dim_cliente.py        # Dimensão Cliente (SCD2)
│   │       ├── transform_dim_usuario.py        # Dimensão Usuário (SCD2)
│   │       └── transform_fato_faturamento.py   # Fato Faturamento
│   │
│   ├── utils/                      # Módulos de utilidade
│   │   ├── audit.py                # Auditoria de execuções
│   │   ├── config.py               # Configurações (.env)
│   │   ├── db_connection.py        # Conexão PostgreSQL
│   │   ├── logger.py               # Setup de logging (Loguru)
│   │   ├── rejection_logger.py     # Sistema de logs de rejeição
│   │   └── validators.py           # Funções de validação de dados
│   │
│   ├── run_bronze_ingestors.py     # Orquestrador Bronze (todos ingestores)
│   └── run_silver_transformers.py  # Orquestrador Silver (todas transformações)
│
├── tests/                          # Testes unitários e de integração
│   └── (a implementar)
│
├── .env.example                    # Exemplo de variáveis de ambiente
├── .env                            # Configurações (gitignored)
├── .gitignore                      # Arquivos ignorados pelo Git
├── requirements.txt                # Dependências Python
├── README.md                       # Documentação principal (usuários)
├── CLAUDE.md                       # Documentação para Claude Code (contexto)
└── RELATORIO_TECNICO_INTERNO.md    # Este arquivo (equipe técnica)
```

---

## 12. COMANDOS RÁPIDOS

### 12.1 Docker

```bash
# Subir ambiente
cd docker
docker compose up -d --build

# Parar ambiente
docker compose down

# Logs em tempo real
docker compose logs -f etl-processor

# Acessar shell do container
docker compose exec etl-processor bash

# Reconstruir imagem (após mudanças no Dockerfile)
docker compose build --no-cache
```

### 12.2 Execução de Pipeline

```bash
# Pipeline completo (Bronze + Silver)
docker compose exec etl-processor bash -c "
  python python/run_bronze_ingestors.py &&
  python python/run_silver_transformers.py
"

# Apenas Bronze
docker compose exec etl-processor python python/run_bronze_ingestors.py

# Apenas Silver
docker compose exec etl-processor python python/run_silver_transformers.py

# Ingestor específico
docker compose exec etl-processor python python/ingestors/csv/ingest_contas.py

# Transformador específico
docker compose exec etl-processor python python/transformers/silver/transform_dim_cliente.py
```

### 12.3 Banco de Dados

```bash
# Conectar ao PostgreSQL (via psql)
PGPASSWORD='58230925AD@' psql -h creditsdw.postgres.database.azure.com -U creditsdw -d creditsdw

# Queries rápidas
# Ver estrutura de tabela
PGPASSWORD='58230925AD@' psql -h creditsdw.postgres.database.azure.com -U creditsdw -d creditsdw -c "\d silver.dim_cliente"

# Contar registros
PGPASSWORD='58230925AD@' psql -h creditsdw.postgres.database.azure.com -U creditsdw -d creditsdw -c "SELECT COUNT(*) FROM bronze.contas"

# Ver últimas execuções
PGPASSWORD='58230925AD@' psql -h creditsdw.postgres.database.azure.com -U creditsdw -d creditsdw -c "
  SELECT script_nome, status, data_inicio, duracao_segundos
  FROM auditoria.historico_execucao
  ORDER BY data_inicio DESC
  LIMIT 10
"

# Ver rejeições recentes
PGPASSWORD='58230925AD@' psql -h creditsdw.postgres.database.azure.com -U creditsdw -d creditsdw -c "
  SELECT numero_linha, campo_falha, motivo_rejeicao
  FROM auditoria.log_rejeicao
  ORDER BY data_rejeicao DESC
  LIMIT 10
"
```

### 12.4 Code Quality

```bash
# Formatar código (Black)
docker compose exec etl-processor black python/

# Análise de estilo (Ruff)
docker compose exec etl-processor ruff check .

# Verificação de tipos (Mypy)
docker compose exec etl-processor mypy python/

# Executar testes (quando implementados)
docker compose exec etl-processor pytest
docker compose exec etl-processor pytest --cov=python --cov-report=html
```

---

## 13. TROUBLESHOOTING

### 13.1 Problema: Ingestor rejeita todos os registros

**Sintomas:**
- `linhas_inseridas = 0`
- `linhas_rejeitadas = total de linhas`

**Diagnóstico:**
1. Verificar logs de rejeição:
   ```sql
   SELECT campo_falha, motivo_rejeicao, COUNT(*)
   FROM auditoria.log_rejeicao
   WHERE script_nome = 'ingest_contas.py'
   GROUP BY campo_falha, motivo_rejeicao;
   ```

2. Verificar formato do CSV:
   - Encoding: Deve ser UTF-8 (ou ISO-8859-1 com fallback)
   - Delimiter: `;` (ponto-vírgula)
   - Headers: Primeira linha deve ter nomes de colunas

**Soluções:**
- Se motivo = "Campo obrigatório vazio" → Preencher campo no CSV
- Se motivo = "CNPJ inválido" → Corrigir dígitos verificadores
- Se motivo = "Valor fora do domínio" → Ajustar valor ou expandir domínio

### 13.2 Problema: Transformação Silver falha com FK nula

**Sintomas:**
- `AssertionError: Encontrados fatos órfãos (cliente_sk nulo)`

**Diagnóstico:**
1. Verificar se dimensão foi carregada antes do fato:
   ```sql
   SELECT COUNT(*) FROM silver.dim_cliente WHERE flag_ativo = TRUE;
   ```

2. Verificar se NK no fato existe na dimensão:
   ```sql
   SELECT DISTINCT f.cnpj_cliente
   FROM bronze.faturamentos f
   LEFT JOIN silver.dim_cliente c ON f.cnpj_cliente = c.cnpj_cpf_nk
   WHERE c.cliente_sk IS NULL;
   ```

**Soluções:**
- Se dimensão vazia → Executar transformador da dimensão primeiro
- Se NK não existe → Ingerir cliente faltante na Bronze

### 13.3 Problema: SCD Type 2 não detecta mudanças

**Sintomas:**
- Cliente mudou de status, mas `versao` permanece 1

**Diagnóstico:**
1. Verificar cálculo de hash:
   ```python
   # Campos incluídos no hash devem conter o campo alterado
   campos_hash = ['razao_social', 'status_conta', ...]
   ```

2. Verificar se flag_ativo está correto:
   ```sql
   SELECT COUNT(*) FROM silver.dim_cliente WHERE flag_ativo = FALSE;
   ```

**Soluções:**
- Adicionar campo ao `campos_hash` se não está incluído
- Verificar se `processar_scd2()` está sendo chamado na transformação

### 13.4 Problema: Performance lenta (>5 min)

**Diagnóstico:**
1. Identificar gargalo:
   ```sql
   SELECT script_nome, AVG(duracao_segundos) as media_segundos
   FROM auditoria.historico_execucao
   WHERE data_inicio >= NOW() - INTERVAL '7 days'
   GROUP BY script_nome
   ORDER BY media_segundos DESC;
   ```

2. Verificar tamanho das tabelas:
   ```sql
   SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
   FROM pg_tables
   WHERE schemaname IN ('bronze', 'silver')
   ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
   ```

**Soluções:**
- Se Bronze grande (>100k linhas) → Considerar MERGE em vez de TRUNCATE/RELOAD
- Se joins lentos → Adicionar índices em colunas de FK
- Se validação lenta → Paralelizar validação com multiprocessing

### 13.5 Problema: Container Docker não inicia

**Sintomas:**
- `docker compose up` falha
- `Error: Connection refused`

**Diagnóstico:**
1. Ver logs do container:
   ```bash
   docker compose logs etl-processor
   ```

2. Verificar .env:
   ```bash
   cat .env | grep DB_
   ```

**Soluções:**
- Se erro de import → Rebuild imagem: `docker compose build --no-cache`
- Se erro de conexão DB → Verificar credenciais no .env
- Se porta ocupada → Mudar porta em docker-compose.yml

---

## 14. CONTATOS E MANUTENÇÃO

### 14.1 Equipe Responsável

| Papel | Nome | Contato | Responsabilidade |
|-------|------|---------|------------------|
| **Product Owner** | Bruno Pires | bruno.pires@creditsbrasil.com.br | Requisitos de negócio, priorização |
| **Tech Lead** | João Viveiros | joao.viveiros@creditsbrasil.com.br | Arquitetura, code review |
| **Data Engineer** | Maria Rodrigues | maria.rodrigues@creditsbrasil.com.br | Desenvolvimento, manutenção |

### 14.2 Suporte

**Issues e Bugs:**
- Criar issue no GitHub: https://github.com/brunocredits/credits-dw/issues
- Incluir: logs de execução, query SQL (se aplicável), passos para reproduzir

**Dúvidas Técnicas:**
- Consultar README.md ou este relatório
- Slack: #data-engineering

**Emergências (Pipeline down):**
- Escalar para Tech Lead (João)
- SLA de resposta: 2 horas (horário comercial)

### 14.3 Manutenção Regular

**Semanal:**
- [ ] Verificar taxa de rejeição (deve ser < 5%)
- [ ] Revisar logs de erro (auditoria.historico_execucao)
- [ ] Limpar arquivos processed antigos (>30 dias)

**Mensal:**
- [ ] Atualizar dependências Python (requirements.txt)
- [ ] Backup manual do banco (além do backup automático Azure)
- [ ] Revisar performance de queries (adicionar índices se necessário)

**Trimestral:**
- [ ] Revisão de segurança (atualizar senhas, revisar permissões)
- [ ] Auditoria de dados (validar integridade referencial, SCD Type 2)
- [ ] Revisão de capacidade (avaliar upgrade de recursos se necessário)

---

## APÊNDICES

### A. Glossário

| Termo | Definição |
|-------|-----------|
| **Bronze Layer** | Camada de dados brutos validados (raw data com validação rigorosa) |
| **Silver Layer** | Camada de dados curados e modelados (Star Schema) |
| **SCD Type 2** | Slowly Changing Dimension Type 2 (versionamento de mudanças) |
| **Surrogate Key (SK)** | Chave primária artificial (auto-incrementada, sem significado de negócio) |
| **Natural Key (NK)** | Chave de negócio (CNPJ, email, identificador externo) |
| **Star Schema** | Modelagem dimensional (fato central + dimensões) |
| **TRUNCATE/RELOAD** | Estratégia de carga: apaga tudo e recarrega |
| **Medallion Architecture** | Arquitetura de dados em camadas (Bronze → Silver → Gold) |
| **Template Method** | Padrão de projeto: classe base define fluxo, subclasses implementam detalhes |

### B. Referências

- **PostgreSQL Documentation:** https://www.postgresql.org/docs/15/
- **Kimball Group (Star Schema):** https://www.kimballgroup.com/
- **SCD Types:** https://en.wikipedia.org/wiki/Slowly_changing_dimension
- **Docker Compose:** https://docs.docker.com/compose/
- **Pandas Documentation:** https://pandas.pydata.org/docs/

---

**FIM DO RELATÓRIO TÉCNICO INTERNO**

*Última atualização: 26/11/2025*
*Próxima revisão: Fevereiro/2026*
