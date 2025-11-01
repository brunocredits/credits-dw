#!/usr/bin/env python3
"""
Script: ingest_ploomes_contacts.py
Descrição: Ingestão de contatos do Ploomes via API para camada Bronze
Frequência: Diária (6h)
Versão: 2.0
"""

import sys
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent.parent))

from ingestors.api.ploomes_client import PloomesClient
from utils.db_connection import get_db_connection
from utils.logger import setup_logger
from utils.audit import registrar_execucao, finalizar_execucao

# Configurações
SCRIPT_NAME = 'ingest_ploomes_contacts.py'
CAMADA = 'bronze'
TABELA_DESTINO = 'bronze.ploomes_contacts'

logger = setup_logger(SCRIPT_NAME)


def extrair_dados_ploomes() -> List[dict]:
    """
    Extrai dados de contatos do Ploomes via API.

    Returns:
        Lista de contatos
    """
    logger.info("Conectando à API do Ploomes...")

    client = PloomesClient()

    try:
        # Buscar todos os contatos
        contacts = client.get_all_contacts()

        logger.info(f"Total de contatos extraídos da API: {len(contacts)}")
        return contacts

    finally:
        client.close()


def transformar_para_bronze(contacts: List[dict]) -> tuple:
    """
    Transforma dados do Ploomes para formato Bronze.

    Args:
        contacts: Lista de contatos da API

    Returns:
        Tupla (registros, colunas)
    """
    logger.info("Transformando dados para formato Bronze...")

    # Converter para DataFrame
    df = pd.DataFrame(contacts)

    # Converter tudo para string (padrão Bronze)
    df = df.astype(str)

    # Adicionar metadados
    df['data_carga_bronze'] = datetime.now()
    df['nome_arquivo_origem'] = 'API_Ploomes_Contacts'

    # Definir colunas da tabela Bronze
    # NOTA: Ajustar conforme estrutura real da tabela bronze.ploomes_contacts
    colunas_bronze = [
        'Id',
        'Name',
        'Email',
        'Phones',
        'Position',
        'CompanyId',
        'OwnerId',
        'TypeId',
        'CreateDate',
        'LastUpdateDate',
        'data_carga_bronze',
        'nome_arquivo_origem'
    ]

    # Garantir que todas as colunas existem
    for col in colunas_bronze:
        if col not in df.columns and col not in ['data_carga_bronze', 'nome_arquivo_origem']:
            df[col] = None

    registros = df[colunas_bronze].values.tolist()

    logger.info(f"Total de registros preparados: {len(registros)}")
    return registros, colunas_bronze


def inserir_bronze(conn, registros: List, colunas: List) -> int:
    """
    Insere registros na tabela Bronze do Ploomes.

    Args:
        conn: Conexão com banco
        registros: Lista de registros
        colunas: Lista de colunas

    Returns:
        Número de linhas inseridas
    """
    from psycopg2.extras import execute_values

    logger.info(f"Inserindo {len(registros)} registros na tabela {TABELA_DESTINO}")

    try:
        cursor = conn.cursor()

        # TRUNCATE/RELOAD (dados sempre vêm completos da API)
        cursor.execute(f"TRUNCATE TABLE {TABELA_DESTINO}")
        logger.info(f"Tabela {TABELA_DESTINO} truncada")

        # Inserir dados
        colunas_str = ', '.join(colunas)
        query = f"INSERT INTO {TABELA_DESTINO} ({colunas_str}) VALUES %s"

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
        logger.info("=" * 80)
        logger.info(f"Iniciando ingestão: {SCRIPT_NAME}")
        logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)

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

        # 1. Extrair dados da API
        contacts = extrair_dados_ploomes()
        linhas_extraidas = len(contacts)

        if linhas_extraidas == 0:
            logger.warning("Nenhum contato extraído da API")
            finalizar_execucao(
                conn=conn,
                execucao_id=execucao_id,
                status='sucesso',
                linhas_processadas=0,
                linhas_inseridas=0
            )
            return 0

        # 2. Transformar para Bronze
        registros, colunas = transformar_para_bronze(contacts)

        # 3. Inserir na Bronze
        linhas_inseridas = inserir_bronze(conn, registros, colunas)

        # 4. Finalizar execução com sucesso
        finalizar_execucao(
            conn=conn,
            execucao_id=execucao_id,
            status='sucesso',
            linhas_processadas=linhas_extraidas,
            linhas_inseridas=linhas_inseridas
        )

        logger.info("=" * 80)
        logger.info("Ingestão concluída com SUCESSO!")
        logger.info(f"Linhas extraídas da API: {linhas_extraidas}")
        logger.info(f"Linhas inseridas no Bronze: {linhas_inseridas}")
        logger.info("=" * 80)

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
