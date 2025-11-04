# Resumo das Alterações e Decisões

## 1. Tabela `bronze.faturamento` Criada
A tabela `bronze.faturamento` foi adicionada ao arquivo `bronze/01-create-bronze-tables.sql` com as seguintes colunas:
- `ID_Faturamento` (UUID)
- `ID_Conta` (UUID)
- `Data` (TIMESTAMP)
- `Receita` (NUMERIC(18, 2))
- `Moeda` (VARCHAR(3))

## 2. Chaves Primárias (PKs) e Estrangeiras (FKs)
**Decisão:** Não foram adicionadas chaves primárias ou estrangeiras explícitas nas tabelas da camada `bronze`.
**Motivo:** A camada `bronze` serve como uma área de "pouso" para dados brutos. O foco é carregar os dados da fonte (CSV) sem aplicar validações que possam impedir a ingestão. As validações e a definição de relacionamentos (PKs/FKs) serão realizadas nas camadas `silver` e `gold` do Data Warehouse.

## 3. Tabela `bronze.data` e Colunas Condicionais
- A tabela `bronze.data` foi mantida como encontrada, com colunas `TIMESTAMP` para os componentes de data. Uma dimensão de data mais completa é tipicamente construída em camadas posteriores.
- As colunas `Corte` e `Faixa` em `bronze.contas_base_oficial` foram mantidas como `VARCHAR` e podem ser nulas. A lógica condicional associada a elas será tratada em etapas de transformação de dados, não na definição da tabela `bronze`.

## 4. Roles (Funções) Criadas
As seguintes roles foram criadas no script `init/01-create-schemas.sql` para gerenciamento de permissões:
- **`dw_admin`**: Role de administrador com permissões completas sobre o Data Warehouse.
- **`dw_developer`**: Role de desenvolvedor com permissões de leitura e escrita na camada `bronze`, ideal para processos de ETL e ingestão de dados.
