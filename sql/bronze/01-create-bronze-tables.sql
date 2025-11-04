-- ============================================================================
-- Script: 01-create-bronze-tables.sql
-- Descrição: Criação das tabelas da camada BRONZE
-- Camada: Bronze (Raw Data Layer)
-- Versão: 2.0
-- Data: Novembro 2025
-- ============================================================================

-- ============================================================================
-- Tabela: bronze.usuarios
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.usuarios (
    ID UUID,
    "Nome da Empresa" VARCHAR,
    "Nome PK" VARCHAR,
    "Área" VARCHAR,
    "Seniôridade" VARCHAR,
    "Gestor" VARCHAR,
    "Email" VARCHAR,
    "Canal 1" VARCHAR,
    "Canal 2" VARCHAR,
    "Email Líder" VARCHAR
);

-- ============================================================================
-- Tabela: bronze.data
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.data (
    "Semestre" TIMESTAMP,
    "Trimestre" TIMESTAMP,
    "Quarter" TIMESTAMP,
    "Bimestre" TIMESTAMP,
    "Mês" TIMESTAMP,
    "Dia" TIMESTAMP,
    "Ano" TIMESTAMP
);

-- ============================================================================
-- Tabela: bronze.contas_base_oficial
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.contas_base_oficial (
    ID UUID,
    "CNPJ/CPF PK" VARCHAR,
    "Id_Conta FK" UUID,
    "Tipo" VARCHAR,
    "Status" VARCHAR,
    "Status de Qualificação da conta" VARCHAR,
    "Data de criação" TIMESTAMP,
    "Grupo" VARCHAR,
    "Razão Social" VARCHAR,
    "Responsável da Conta" VARCHAR,
    "Financeiro" VARCHAR,
    "Corte" VARCHAR,
    "Faixa" VARCHAR
);

-- ============================================================================
-- Tabela: bronze.faturamento
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.faturamento (
    "ID_Faturamento" UUID,
    "ID_Conta" UUID,
    "Data" TIMESTAMP,
    "Receita" NUMERIC(18, 2),
    "Moeda" VARCHAR(3)
);
