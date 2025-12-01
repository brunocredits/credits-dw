#!/usr/bin/env python3
"""
Script: setup_bronze_schema.py
Descrição: Cria schemas 'bronze', 'auditoria' e tabelas com Schema ESTRITO definido pelo usuário.
"""

import sys
from pathlib import Path
import psycopg2
from psycopg2 import sql

# Adiciona diretório raiz
sys.path.insert(0, str(Path(__file__).parent))

from utils.db_connection import get_db_connection, get_cursor
from utils.logger import setup_logger

logger = setup_logger('setup_bronze_schema')

DDL_STATEMENTS = [
    # 1. Schemas
    "CREATE SCHEMA IF NOT EXISTS bronze;",
    "CREATE SCHEMA IF NOT EXISTS auditoria;",

    # 2. Tabelas de Auditoria (Mantidas)
    """
    CREATE TABLE IF NOT EXISTS auditoria.historico_execucao (
        id UUID PRIMARY KEY,
        script_nome TEXT NOT NULL,
        camada TEXT NOT NULL,
        tabela_origem TEXT,
        tabela_destino TEXT,
        data_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        data_fim TIMESTAMP,
        status TEXT,
        linhas_processadas INT DEFAULT 0,
        linhas_inseridas INT DEFAULT 0,
        linhas_atualizadas INT DEFAULT 0,
        linhas_erro INT DEFAULT 0,
        mensagem_erro TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS auditoria.log_rejeicao (
        id SERIAL PRIMARY KEY,
        execucao_fk UUID REFERENCES auditoria.historico_execucao(id),
        script_nome TEXT,
        tabela_destino TEXT,
        numero_linha INT,
        campo_falha TEXT,
        motivo_rejeicao TEXT,
        valor_recebido TEXT,
        registro_completo TEXT,
        severidade TEXT,
        data_rejeicao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    
    # 3. DROP TABLES para recriar com novo schema estrito
    "DROP TABLE IF EXISTS bronze.faturamento CASCADE;",
    "DROP TABLE IF EXISTS bronze.base_oficial CASCADE;",
    "DROP TABLE IF EXISTS bronze.usuarios CASCADE;",

    # 4. Tabela Faturamento (Consolidada)
    """
    CREATE TABLE bronze.faturamento (
        status TEXT,
        numero_documento TEXT,
        parcela TEXT,
        nota_fiscal TEXT,
        cliente_nome_fantasia TEXT,
        previsao_recebimento DATE,
        ultimo_recebimento DATE,
        valor_conta DECIMAL(18,2),
        valor_liquido DECIMAL(18,2),
        
        -- Campos Financeiro/Impostos
        impostos_retidos DECIMAL(18,2),
        desconto DECIMAL(18,2),
        juros_multa DECIMAL(18,2),
        valor_recebido DECIMAL(18,2),
        valor_a_receber DECIMAL(18,2),
        categoria TEXT,
        operacao TEXT,
        vendedor TEXT,
        projeto TEXT,
        conta_corrente TEXT,
        numero_boleto TEXT,
        tipo_documento TEXT,
        vencimento DATE,
        data_emissao DATE,
        cliente_razao_social TEXT,
        cliente_sem_pontuacao TEXT,
        tags_cliente TEXT,
        observacao TEXT,
        ultima_alteracao TEXT,
        incluido_por TEXT,
        alterado_por TEXT,
        data_fat TEXT,
        empresa TEXT,
        ms TEXT,
        
        -- Metadata
        ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_filename TEXT
    );
    """,

    # 5. Tabela Base Oficial
    """
    CREATE TABLE bronze.base_oficial (
        cnpj TEXT,
        status TEXT,
        manter_no_baseline TEXT,
        nome_fantasia TEXT,
        canal1 TEXT,
        canal2 TEXT,
        lider TEXT,
        responsavel TEXT,
        empresa TEXT,
        grupo TEXT,
        obs TEXT,
        faixas TEXT,
        mediana TEXT,
        
        -- Metadata
        ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_filename TEXT
    );
    """,

    # 6. Tabela Usuarios
    """
    CREATE TABLE bronze.usuarios (
        cargo TEXT,
        status TEXT,
        nome_usuario TEXT,
        nivel TEXT,
        time TEXT,
        meta_mensal DECIMAL(18,2),
        meta_fidelidade DECIMAL(18,2),
        meta_anual DECIMAL(18,2),
        acesso_vendedor BOOLEAN,
        acesso_gerente BOOLEAN,
        acesso_indireto BOOLEAN,
        acesso_diretoria BOOLEAN,
        acesso_temporario BOOLEAN,
        email_usuario TEXT,
        email_superior TEXT,
        email_gerencia TEXT,
        email_diretoria TEXT,
        
        -- Metadata
        ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_filename TEXT
    );
    """,

    # 7. Tabela Data (Mantida)
    """
    CREATE TABLE IF NOT EXISTS bronze.data (
        data DATE PRIMARY KEY,
        ano INT,
        mes INT,
        dia INT,
        ano_mes TEXT,
        semana_ano INT,
        nome_mes TEXT,
        dia_semana INT,
        nome_dia_semana TEXT,
        bimestre INT,
        trimestre INT,
        quarter INT,
        semestre INT,
        is_weekend BOOLEAN,
        is_holiday BOOLEAN,
        
        -- Metadata
        ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_filename TEXT
    );
    """
]

def setup_schema():
    conn = None
    try:
        logger.info("Conectando ao banco de dados...")
        conn = get_db_connection()
        
        with get_cursor(conn) as cur:
            for i, ddl in enumerate(DDL_STATEMENTS):
                logger.info(f"Executando DDL {i+1}...")
                cur.execute(ddl)
        
        conn.commit()
        logger.success("Schemas Bronze e Auditoria recriados com SUCESSO (Padrão Estrito)!")
        
    except Exception as e:
        logger.error(f"Erro ao configurar schema: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    setup_schema()