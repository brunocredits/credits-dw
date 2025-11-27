# Estratégia Power BI - Credits DW

## 📊 Arquitetura Recomendada

### Opção 1: Power BI → Silver (Recomendado para começar)
```
Power BI ──→ PostgreSQL (silver.*)
             │
             ├─ fato_faturamento
             ├─ dim_data
             ├─ dim_cliente
             └─ dim_usuario
```

**Vantagens:**
- ✅ Star Schema nativo (Power BI é otimizado para isso)
- ✅ Modelo simples e direto
- ✅ Menos manutenção
- ✅ Suficiente para volumes < 10M linhas

**Quando usar:**
- Início do projeto
- Volume de dados pequeno/médio
- Dashboards com < 50 usuários simultâneos

---

### Opção 2: Power BI → Views Materializadas (Melhor performance)
```
Power BI ──→ PostgreSQL (silver.vw_*)
             │
             ├─ vw_faturamento_semanal (agregado)
             ├─ vw_carteira_clientes (snapshot)
             └─ vw_faturamento_mensal_moeda (agregado)
```

**Vantagens:**
- ✅ Performance muito superior (dados pré-agregados)
- ✅ Dashboards carregam instantaneamente
- ✅ Menor carga no servidor durante refresh
- ✅ Pode coexistir com Opção 1

**Quando usar:**
- Volume crescendo (> 1M linhas)
- Dashboards críticos (executivos)
- Mesmas agregações usadas em múltiplos relatórios

**Refresh das Views:**
```sql
-- Atualização diária via cron/scheduled job
REFRESH MATERIALIZED VIEW CONCURRENTLY silver.vw_faturamento_semanal;
REFRESH MATERIALIZED VIEW CONCURRENTLY silver.vw_carteira_clientes;
REFRESH MATERIALIZED VIEW CONCURRENTLY silver.vw_faturamento_mensal_moeda;
```

---

### Opção 3: Power BI → Gold (Apenas se realmente necessário)
```
Power BI ──→ PostgreSQL (gold.*)
             │
             ├─ gold.faturamento_semanal
             ├─ gold.carteira_snapshot_diario
             └─ gold.metricas_consolidadas
```

**Vantagens:**
- ✅ Performance máxima
- ✅ Dados específicos por departamento
- ✅ Histórico de snapshots

**Quando usar:**
- Volume MUITO grande (> 10M linhas)
- Necessidade de snapshots históricos (ponto no tempo)
- Múltiplos departamentos com necessidades diferentes

---

## 🎯 Recomendação Específica: Análise de Carteira

Para acompanhamento de **carteira, consumo semanal, semestral**, sugiro:

### **FASE 1 (Atual):** Começar com Silver
- Conectar Power BI nas tabelas Silver diretamente
- Criar medidas DAX para agregações
- Avaliar performance

### **FASE 2 (Quando crescer):** Adicionar Views Materializadas
- Implementar `vw_faturamento_semanal` para análises semanais
- Implementar `vw_carteira_clientes` para snapshot da carteira
- Power BI consome views + tabelas Silver (híbrido)

### **FASE 3 (Se necessário):** Gold Layer
- Apenas se volume > 10M ou necessidade de snapshots históricos
- Criar tabelas `gold.faturamento_semanal_historico`
- Manter snapshots mensais da carteira

---

## 📋 Modelo Power BI Recomendado

### Tabelas Fato (1-to-Many)
- `fato_faturamento` → Granularidade: 1 linha por transação

### Dimensões (Many-to-1)
- `dim_data` → Relacionamento: `fato.data_sk → dim_data.data_sk`
- `dim_cliente` → Relacionamento: `fato.cliente_sk → dim_cliente.cliente_sk`
- `dim_usuario` → Relacionamento: `fato.usuario_sk → dim_usuario.usuario_sk`

### Medidas DAX Sugeridas

```dax
// Faturamento Total
Faturamento Total = SUM(fato_faturamento[valor_liquido])

// Faturamento YTD (Year-to-Date)
Faturamento YTD =
TOTALYTD(
    SUM(fato_faturamento[valor_liquido]),
    dim_data[data_completa]
)

// Ticket Médio
Ticket Médio =
DIVIDE(
    SUM(fato_faturamento[valor_liquido]),
    COUNTROWS(fato_faturamento),
    0
)

// Número de Clientes Ativos
Clientes Ativos =
CALCULATE(
    DISTINCTCOUNT(dim_cliente[cliente_sk]),
    dim_cliente[flag_ativo] = TRUE
)

// Faturamento Mês Anterior
Faturamento Mês Anterior =
CALCULATE(
    [Faturamento Total],
    DATEADD(dim_data[data_completa], -1, MONTH)
)

// Variação % vs Mês Anterior
Variação % MoM =
VAR FatAtual = [Faturamento Total]
VAR FatAnterior = [Faturamento Mês Anterior]
RETURN
    DIVIDE(FatAtual - FatAnterior, FatAnterior, 0)

// Faturamento Semana Atual
Faturamento Semana =
CALCULATE(
    [Faturamento Total],
    dim_data[semana_ano] = WEEKNUM(TODAY())
)
```

---

## 🔄 Estratégia de Refresh

### Power BI Import Mode (Recomendado)
- Refresh diário/semanal
- Dados armazenados no Power BI Service
- Performance máxima
- Limite: ~10M linhas por tabela

### Power BI DirectQuery (Alternativa)
- Dados sempre atualizados
- Consultas diretas no banco
- Performance depende do banco
- Sem limite de linhas

### Hybrid (Import + DirectQuery)
- Dimensões: Import (raramente mudam)
- Fatos: DirectQuery (sempre atualizados)
- Melhor dos dois mundos

---

## 🚀 Próximos Passos

### Implementação Imediata
1. ✅ Conectar Power BI nas tabelas Silver
2. ✅ Criar modelo dimensional no Power BI
3. ✅ Desenvolver medidas DAX básicas
4. ⏳ Testar performance com dados reais

### Implementação Futura (Se Necessário)
5. ⏳ Executar `sql/views_powerbi.sql` para criar views materializadas
6. ⏳ Criar job para refresh automático das views
7. ⏳ Migrar dashboards críticos para views
8. ⏳ Avaliar necessidade de Gold layer

---

## 📊 Dashboards Sugeridos

### Dashboard 1: Executivo
- KPIs: Faturamento Total, Ticket Médio, Num Clientes
- Gráfico: Evolução Mensal (linha)
- Gráfico: Top 10 Clientes (barra horizontal)
- Tabela: Faturamento por Tipo de Cliente

### Dashboard 2: Carteira
- Tabela: Lista de Clientes com métricas
- Filtros: Status, Tipo Pessoa, Porte
- Drill-through: Detalhes do cliente

### Dashboard 3: Consumo Semanal
- Gráfico: Faturamento por Semana (coluna)
- Comparativo: Semana Atual vs Média
- Tabela: Transações da semana

### Dashboard 4: Análise Semestral
- Gráfico: Tendência Semestral (área)
- Comparativo: Semestre vs Semestre
- Decomposição: Por tipo de cliente, moeda, etc.

---

## 🎓 Boas Práticas

1. **Sempre usar relacionamentos** (não fazer JOINs em DAX)
2. **Medidas ao invés de colunas calculadas** (performance)
3. **Filtrar na origem** (reduzir volume importado)
4. **Marcar dim_data como tabela de datas** (funções de tempo)
5. **Ocultar colunas técnicas** (SKs, hashes, flags internas)
6. **Documentar medidas** (adicionar descrições)

---

## 📞 Troubleshooting

### "Refresh muito lento"
→ Considerar views materializadas ou agregações

### "Memória insuficiente"
→ Filtrar dados na origem (últimos 2 anos, por exemplo)

### "Queries lentas"
→ Verificar índices no PostgreSQL, considerar DirectQuery

### "Dados desatualizados"
→ Aumentar frequência de refresh ou usar DirectQuery
