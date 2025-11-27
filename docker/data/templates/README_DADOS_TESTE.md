# Dados de Teste para Credits DW

## Arquivos Disponíveis

Este diretório contém arquivos CSV de teste com dados **VÁLIDOS** e **INVÁLIDOS** misturados para testar o sistema de validação.

### Estrutura dos Arquivos

Cada arquivo contém:
- **10 registros VÁLIDOS** (linhas 2-11): Passam em todas as validações
- **10 registros INVÁLIDOS** (linhas 12-21): Falham em validações específicas

## 1. contas_COMPLETO.csv

**Total:** 20 registros (10 válidos + 10 inválidos)

### Registros Válidos (10)
- CNPJs com dígitos verificadores CORRETOS
- Tipos válidos: 'PJ'
- Status válidos: 'Ativo', 'Inativo', 'Suspenso'
- Datas no formato YYYY-MM-DD

**CNPJs Válidos:**
```
11222333000181
22333444000162
33444555000143
44555666000124
55666777000105
66777888000186
77888999000167
88999000000101
99000111000125
00111222000109
```

### Registros Inválidos (10)

| Linha | Erro | Campo | Motivo |
|-------|------|-------|--------|
| 12 | CNPJ | cnpj_cpf | Dígito verificador incorreto: 12345678000195 |
| 13 | CNPJ | cnpj_cpf | Dígitos repetidos: 11111111111111 |
| 14 | Tipo | tipo | Valor 'INVALIDO' não está no domínio ['PJ', 'PF'] |
| 15 | Status | status | Valor 'StatusErrado' não está no domínio |
| 16 | Data | data_criacao | Formato inválido: 'data-invalida' |
| 17 | Obrigatório | cnpj_cpf | Campo vazio |
| 18 | CNPJ | cnpj_cpf | Dígito verificador incorreto: 33444555000100 |
| 19 | CNPJ | cnpj_cpf | Contém letras: ABC12345678901 |
| 20 | Data | data_criacao | Data impossível: 2024-13-45 |
| 21 | Obrigatório | tipo | Campo vazio |

---

## 2. usuarios_COMPLETO.csv

**Total:** 20 registros (10 válidos + 10 inválidos)

### Registros Válidos (10)
- Emails no formato correto: user@domain.com
- Canal_1 válidos: 'Direto', 'Partner'
- Todos campos obrigatórios preenchidos

**Emails Válidos:**
```
joao.silva@credits.com.br
maria.santos@credits.com.br
pedro.costa@credits.com.br
ana.lima@credits.com.br
carlos.dias@credits.com.br
lucia.melo@credits.com.br
roberto.alves@credits.com.br
fernanda.costa@credits.com.br
marcos.silva@credits.com.br
paula.oliveira@credits.com.br
```

### Registros Inválidos (10)

| Linha | Erro | Campo | Motivo |
|-------|------|-------|--------|
| 12 | Obrigatório | nome_empresa | Campo vazio |
| 13 | Obrigatório | nome | Campo vazio |
| 14 | Obrigatório | canal_1 | Campo vazio |
| 15 | Obrigatório | email | Campo vazio |
| 16 | Email | email | Sem arroba: 'email-sem-arroba' |
| 17 | Email | email | Incompleto: 'lucia@' |
| 18 | Domínio | canal_1 | 'CanalInvalido' não está em ['Direto', 'Partner'] |
| 19 | Email | email_lider | Formato inválido: 'email-lider-invalido' |
| 20 | Email | email | Sem arroba: 'marcos.silva.credits.com.br' |
| 21 | Email | email | Dois arrobas: 'paula@oliveira@credits.com.br' |

---

## 3. faturamentos_COMPLETO.csv

**Total:** 20 registros (10 válidos + 10 inválidos)

### Registros Válidos (10)
- Datas no formato YYYY-MM-DD
- Receitas positivas (> 0)
- Moedas válidas: 'BRL', 'USD', 'EUR'
- CNPJs de clientes VÁLIDOS (com dígitos verificadores corretos)

**Valores de Receita:**
```
15000.50 BRL
25000.00 BRL
8500.75 BRL
45000.00 USD
12000.00 EUR
33000.25 BRL
18500.00 BRL
22000.00 USD
29500.50 BRL
35000.00 EUR
```

### Registros Inválidos (10)

| Linha | Erro | Campo | Motivo |
|-------|------|-------|--------|
| 12 | Data | data | Formato inválido: 'data-invalida' |
| 13 | Receita | receita | Valor negativo: -5000.00 |
| 14 | Moeda | moeda | 'JPY' não está no domínio ['BRL', 'USD', 'EUR'] |
| 15 | CNPJ | cnpj_cliente | Formato inválido: 'cpf-invalido' |
| 16 | Email | email_usuario | Sem arroba: 'email-sem-arroba' |
| 17 | Obrigatório | data | Campo vazio |
| 18 | Receita | receita | Valor zero (não é positivo) |
| 19 | Moeda | moeda | 'PESOS' não está no domínio |
| 20 | Receita | receita | Tipo inválido: 'abc' (não é número) |
| 21 | Data | data | Data impossível: 2024-13-45 |

---

## Como Usar

### 1. Copiar para o diretório de input

```bash
# Copiar arquivos de teste para o diretório de input
cp docker/data/templates/contas_COMPLETO.csv docker/data/input/onedrive/contas.csv
cp docker/data/templates/usuarios_COMPLETO.csv docker/data/input/onedrive/usuarios.csv
cp docker/data/templates/faturamentos_COMPLETO.csv docker/data/input/onedrive/faturamentos.csv
```

### 2. Executar ingestores

```bash
# Executar todos os ingestores
cd docker
docker compose exec etl-processor python python/run_bronze_ingestors.py

# Ou executar individualmente
docker compose exec etl-processor python python/ingestors/csv/ingest_contas.py
docker compose exec etl-processor python python/ingestors/csv/ingest_usuarios.py
docker compose exec etl-processor python python/ingestors/csv/ingest_faturamentos.py
```

### 3. Verificar resultados

**Contas:**
- Esperado: 10 registros inseridos, 10 rejeitados (50% de taxa de rejeição)

**Usuários:**
- Esperado: 10 registros inseridos, 10 rejeitados (50% de taxa de rejeição)

**Faturamentos:**
- Esperado: 10 registros inseridos, 10 rejeitados (50% de taxa de rejeição)

### 4. Consultar rejeições

```sql
-- Ver resumo de rejeições
SELECT
    tabela_destino,
    campo_falha,
    COUNT(*) as total
FROM auditoria.log_rejeicao
GROUP BY tabela_destino, campo_falha
ORDER BY tabela_destino, total DESC;

-- Ver detalhes das rejeições
SELECT
    numero_linha,
    campo_falha,
    motivo_rejeicao,
    valor_recebido
FROM auditoria.log_rejeicao
WHERE script_nome = 'ingest_contas.py'
ORDER BY numero_linha;
```

---

## Características dos Dados Válidos

### CNPJs Gerados
Todos os CNPJs válidos foram gerados com dígitos verificadores corretos usando o algoritmo oficial do CNPJ brasileiro.

### Emails
Todos seguem o padrão RFC 5322: `local-part@domain.tld`

### Datas
Formato ISO 8601: `YYYY-MM-DD`

### Moedas
Códigos ISO 4217: BRL (Real), USD (Dólar), EUR (Euro)

---

## Observações Importantes

1. **CNPJs com Pontuação:** O sistema remove automaticamente a pontuação (`.`, `/`, `-`) antes de validar
2. **Case Sensitivity:** Domínios são case-sensitive por padrão
3. **Campos Opcionais:** Campos não obrigatórios podem estar vazios sem causar rejeição
4. **Duplicatas:** O sistema remove linhas 100% idênticas automaticamente

---

**Última atualização:** 26/11/2025
