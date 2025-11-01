#!/usr/bin/env python3
"""
Script: ingest_onedrive_clientes.py
Descrição: Ingestão de dados de clientes do OneDrive CSV para camada Bronze
Frequência: Diária (6h)
Versão: 1.0
Data: Outubro 2025
"""

import sys
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from pathlib import Path

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent))

from utils.db_connection import get_db_connection
from utils.logger import setup_logger
from utils.audit import registrar_execucao, finalizar_execucao

# Configuração de logging
logger = setup_logger('ingest_onedrive_clientes')

# Configurações
SCRIPT_NAME = 'ingest_onedrive_clientes.py'
CAMADA = 'bronze'
TABELA_DESTINO = 'bronze.onedrive_clientes'
ARQUIVO_CSV = os.getenv('ONEDRIVE_PATH', '/data/onedrive') + '/Clientes.csv'


def ler_csv_clientes(arquivo_path: str) -> pd.DataFrame:
    """
    Lê arquivo CSV de clientes do OneDrive
    
    Args:
        arquivo_path: Caminho completo do arquivo CSV
        
    Returns:
        DataFrame pandas com os dados
    """
    try:
        logger.info(f"Lendo arquivo CSV: {arquivo_path}")
        
        # Ler CSV com encoding adequado
        df = pd.read_csv(
            arquivo_path,
            encoding='utf-8-sig',
            sep=';',  # Ajustar conforme o separador real
            dtype=str,  # Tudo como string na Bronze
            na_values=['', 'NULL', 'null', 'NA']
        )
        
        logger.info(f"Arquivo lido com sucesso. Total de linhas: {len(df)}")
        return df
        
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado: {arquivo_path}")
        raise
    except Exception as e:
        logger.error(f"Erro ao ler CSV: {str(e)}")
        raise


def transformar_para_bronze(df: pd.DataFrame, nome_arquivo: str) -> list:
    """
    Prepara dados para inserção na camada Bronze
    
    Args:
        df: DataFrame com dados do CSV
        nome_arquivo: Nome do arquivo de origem
        
    Returns:
        Lista de tuplas prontas para inserção
    """
    logger.info("Preparando dados para inserção na Bronze")
    
    # Mapear colunas do CSV para colunas da tabela Bronze
    colunas_mapeamento = {
        'ID_Cliente': 'cliente_id',
        'Codigo': 'codigo_cliente',
        'CNPJ_CPF': 'cnpj_cpf',
        'Razao_Social': 'razao_social',
        'Nome_Fantasia': 'nome_fantasia',
        'Tipo': 'tipo_pessoa',
        'Email': 'email',
        'Telefone': 'telefone',
        'Celular': 'celular',
        'Logradouro': 'logradouro',
        'Numero': 'numero',
        'Complemento': 'complemento',
        'Bairro': 'bairro',
        'Cidade': 'cidade',
        'UF': 'estado',
        'CEP': 'cep',
        'Segmento': 'segmento',
        'Porte': 'porte_empresa',
        'Consultor': 'consultor_responsavel',
        'Status': 'status_cliente'
    }
    
    # Renomear colunas
    df_bronze = df.rename(columns=colunas_mapeamento)
    
    # Adicionar metadados
    df_bronze['data_carga_bronze'] = datetime.now()
    df_bronze['nome_arquivo_origem'] = nome_arquivo
    
    # Converter para lista de tuplas
    colunas_bronze = [
        'cliente_id', 'codigo_cliente', 'cnpj_cpf', 'razao_social', 'nome_fantasia',
        'tipo_pessoa', 'email', 'telefone', 'celular', 'logradouro', 'numero',
        'complemento', 'bairro', 'cidade', 'estado', 'cep', 'segmento',
        'porte_empresa', 'consultor_responsavel', 'status_cliente',
        'data_carga_bronze', 'nome_arquivo_origem'
    ]
    
    # Garantir que todas as colunas existem
    for col in colunas_bronze:
        if col not in df_bronze.columns and col not in ['data_carga_bronze', 'nome_arquivo_origem']:
            df_bronze[col] = None
    
    registros = df_bronze[colunas_bronze].values.tolist()
    
    logger.info(f"Total de registros preparados: {len(registros)}")
    return registros, colunas_bronze


def inserir_bronze(conn, registros: list, colunas: list) -> int:
    """
    Insere registros na tabela Bronze
    
    Args:
        conn: Conexão com o banco
        registros: Lista de tuplas com dados
        colunas: Lista de nomes das colunas
        
    Returns:
        Número de linhas inseridas
    """
    logger.info(f"Inserindo {len(registros)} registros na tabela {TABELA_DESTINO}")
    
    try:
        cursor = conn.cursor()
        
        # Limpar tabela Bronze (estratégia: truncate e reload)
        # Alternativa: Fazer UPSERT se quiser manter histórico
        cursor.execute(f"TRUNCATE TABLE {TABELA_DESTINO}")
        logger.info(f"Tabela {TABELA_DESTINO} truncada")
        
        # Inserir dados
        colunas_str = ', '.join(colunas)
        placeholders = ', '.join(['%s'] * len(colunas))
        query = f"""
            INSERT INTO {TABELA_DESTINO} ({colunas_str})
            VALUES ({placeholders})
        """
        
        execute_values(cursor, query, registros, page_size=1000)
        conn.commit()
        
        linhas_inseridas = len(registros)
        logger.info(f"Inserção concluída: {linhas_inseridas} linhas")
        
        cursor.close()
        return linhas_inseridas
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro na inserção: {str(e)}")
        raise


def main():
    """Função principal de execução"""
    conn = None
    execucao_id = None
    
    try:
        logger.info("="*80)
        logger.info(f"Iniciando ingestão: {SCRIPT_NAME}")
        logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)
        
        # Conectar ao banco
        conn = get_db_connection()
        logger.info("Conexão com banco estabelecida")
        
        # Registrar início da execução
        execucao_id = registrar_execucao(
            conn=conn,
            script_nome=SCRIPT_NAME,
            camada=CAMADA,
            tabela_destino=TABELA_DESTINO
        )
        logger.info(f"Execução registrada com ID: {execucao_id}")
        
        # 1. Ler CSV
        df = ler_csv_clientes(ARQUIVO_CSV)
        linhas_lidas = len(df)
        
        # 2. Preparar para Bronze
        nome_arquivo = os.path.basename(ARQUIVO_CSV)
        registros, colunas = transformar_para_bronze(df, nome_arquivo)
        
        # 3. Inserir na Bronze
        linhas_inseridas = inserir_bronze(conn, registros, colunas)
        
        # 4. Finalizar execução com sucesso
        finalizar_execucao(
            conn=conn,
            execucao_id=execucao_id,
            status='sucesso',
            linhas_processadas=linhas_lidas,
            linhas_inseridas=linhas_inseridas
        )
        
        logger.info("="*80)
        logger.info("Ingestão concluída com SUCESSO!")
        logger.info(f"Linhas processadas: {linhas_lidas}")
        logger.info(f"Linhas inseridas: {linhas_inseridas}")
        logger.info("="*80)
        
        return 0
        
    except Exception as e:
        logger.error(f"ERRO na execução: {str(e)}", exc_info=True)
        
        # Registrar erro na auditoria
        if conn and execucao_id:
            finalizar_execucao(
                conn=conn,
                execucao_id=execucao_id,
                status='erro',
                mensagem_erro=str(e)
            )
        
        return 1
        
    finally:
        if conn:
            conn.close()
            logger.info("Conexão com banco fechada")


if __name__ == '__main__':
    sys.exit(main())
