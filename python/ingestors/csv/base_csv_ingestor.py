#!/usr/bin/env python3
"""
Módulo: base_csv_ingestor.py
Descrição: Classe base para ingestão de arquivos CSV na camada Bronze
Versão: 2.0
"""

import os
import sys
import pandas as pd
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from abc import ABC, abstractmethod

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent.parent))

from utils.db_connection import get_db_connection
from utils.logger import setup_logger
from utils.audit import registrar_execucao, finalizar_execucao


class BaseCSVIngestor(ABC):
    """
    Classe base abstrata para ingestores CSV.
    Implementa o padrão Template Method para processamento de arquivos CSV.
    """

    def __init__(self,
                 script_name: str,
                 tabela_destino: str,
                 arquivo_nome: str,
                 input_subdir: str = ''):
        """
        Inicializa o ingestor base.

        Args:
            script_name: Nome do script (para auditoria)
            tabela_destino: Nome completo da tabela Bronze (schema.tabela)
            arquivo_nome: Nome do arquivo CSV a ser processado
            input_subdir: Subdiretório dentro de DATA_INPUT_PATH (ex: 'onedrive', 'faturamento')
        """
        self.script_name = script_name
        self.tabela_destino = tabela_destino
        self.arquivo_nome = arquivo_nome

        # Configurar caminhos
        data_input = os.getenv('DATA_INPUT_PATH', './docker/data/input')
        self.arquivo_path = os.path.join(data_input, input_subdir, arquivo_nome)

        data_processed = os.getenv('DATA_PROCESSED_PATH', './docker/data/processed')
        self.processed_dir = data_processed

        # Logger
        self.logger = setup_logger(script_name)

        # Configurações CSV padrão (podem ser sobrescritas)
        self.csv_separator = ';'
        self.csv_encoding = 'utf-8-sig'

    @abstractmethod
    def get_column_mapping(self) -> Dict[str, str]:
        """
        Retorna o mapeamento de colunas CSV -> Tabela Bronze.
        Deve ser implementado pelas classes filhas.

        Returns:
            Dict com {coluna_csv: coluna_bronze}
        """
        pass

    @abstractmethod
    def get_bronze_columns(self) -> List[str]:
        """
        Retorna a lista de colunas da tabela Bronze na ordem correta.
        Deve ser implementado pelas classes filhas.

        Returns:
            Lista de nomes de colunas
        """
        pass

    def ler_csv(self) -> pd.DataFrame:
        """
        Lê arquivo CSV com configurações padrão.
        Pode ser sobrescrito para comportamentos específicos.
        """
        try:
            self.logger.info(f"Lendo arquivo CSV: {self.arquivo_path}")

            df = pd.read_csv(
                self.arquivo_path,
                encoding=self.csv_encoding,
                sep=self.csv_separator,
                dtype=str,  # Bronze: tudo como string
                na_values=['', 'NULL', 'null', 'NA'],
                keep_default_na=False
            )

            self.logger.info(f"Arquivo lido com sucesso. Total de linhas: {len(df)}")
            return df

        except FileNotFoundError:
            self.logger.error(f"Arquivo não encontrado: {self.arquivo_path}")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao ler CSV: {str(e)}")
            raise

    def transformar_para_bronze(self, df: pd.DataFrame) -> tuple:
        """
        Prepara dados para inserção na Bronze usando mapeamento de colunas.
        """
        self.logger.info("Preparando dados para inserção na Bronze")

        # Aplicar mapeamento de colunas
        df_bronze = df.rename(columns=self.get_column_mapping())

        # Adicionar metadados
        df_bronze['data_carga_bronze'] = datetime.now()
        df_bronze['nome_arquivo_origem'] = self.arquivo_nome

        # Garantir que todas as colunas Bronze existem
        colunas_bronze = self.get_bronze_columns()
        for col in colunas_bronze:
            if col not in df_bronze.columns and col not in ['data_carga_bronze', 'nome_arquivo_origem']:
                df_bronze[col] = None

        # Adicionar metadados às colunas se não estiverem na lista
        if 'data_carga_bronze' not in colunas_bronze:
            colunas_bronze.append('data_carga_bronze')
        if 'nome_arquivo_origem' not in colunas_bronze:
            colunas_bronze.append('nome_arquivo_origem')

        registros = df_bronze[colunas_bronze].values.tolist()

        self.logger.info(f"Total de registros preparados: {len(registros)}")
        return registros, colunas_bronze

    def inserir_bronze(self, conn, registros: List, colunas: List) -> int:
        """
        Insere registros na tabela Bronze usando estratégia TRUNCATE/RELOAD.
        """
        from psycopg2.extras import execute_values

        self.logger.info(f"Inserindo {len(registros)} registros na tabela {self.tabela_destino}")

        try:
            cursor = conn.cursor()

            # Limpar tabela Bronze (TRUNCATE/RELOAD)
            cursor.execute(f"TRUNCATE TABLE {self.tabela_destino}")
            self.logger.info(f"Tabela {self.tabela_destino} truncada")

            # Inserir dados
            colunas_str = ', '.join(colunas)
            placeholders = ', '.join(['%s'] * len(colunas))
            query = f"INSERT INTO {self.tabela_destino} ({colunas_str}) VALUES %s"

            execute_values(cursor, query, registros, page_size=1000)
            conn.commit()

            linhas_inseridas = len(registros)
            self.logger.info(f"Inserção concluída: {linhas_inseridas} linhas")

            cursor.close()
            return linhas_inseridas

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Erro na inserção: {str(e)}")
            raise

    def mover_para_processed(self):
        """
        Move arquivo processado para diretório de backup com timestamp.
        """
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            nome_com_timestamp = f"{timestamp}_{self.arquivo_nome}"
            destino = os.path.join(self.processed_dir, nome_com_timestamp)

            # Criar diretório se não existir
            os.makedirs(self.processed_dir, exist_ok=True)

            shutil.move(self.arquivo_path, destino)
            self.logger.info(f"Arquivo movido para: {destino}")

        except Exception as e:
            self.logger.warning(f"Não foi possível mover arquivo: {str(e)}")

    def executar(self) -> int:
        """
        Executa o processo completo de ingestão.
        Template Method principal.

        Returns:
            0 em caso de sucesso, 1 em caso de erro
        """
        conn = None
        execucao_id = None

        try:
            self.logger.info("=" * 80)
            self.logger.info(f"Iniciando ingestão: {self.script_name}")
            self.logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info("=" * 80)

            # Verificar se arquivo existe
            if not os.path.exists(self.arquivo_path):
                raise FileNotFoundError(f"Arquivo não encontrado: {self.arquivo_path}")

            # Conectar ao banco
            conn = get_db_connection()
            self.logger.info("Conexão com banco estabelecida")

            # Registrar início da execução
            execucao_id = registrar_execucao(
                conn=conn,
                script_nome=self.script_name,
                camada='bronze',
                tabela_destino=self.tabela_destino
            )
            self.logger.info(f"Execução registrada com ID: {execucao_id}")

            # 1. Ler CSV
            df = self.ler_csv()
            linhas_lidas = len(df)

            # 2. Preparar para Bronze
            registros, colunas = self.transformar_para_bronze(df)

            # 3. Inserir na Bronze
            linhas_inseridas = self.inserir_bronze(conn, registros, colunas)

            # 4. Mover arquivo para processed
            self.mover_para_processed()

            # 5. Finalizar execução com sucesso
            finalizar_execucao(
                conn=conn,
                execucao_id=execucao_id,
                status='sucesso',
                linhas_processadas=linhas_lidas,
                linhas_inseridas=linhas_inseridas
            )

            self.logger.info("=" * 80)
            self.logger.info("Ingestão concluída com SUCESSO!")
            self.logger.info(f"Linhas processadas: {linhas_lidas}")
            self.logger.info(f"Linhas inseridas: {linhas_inseridas}")
            self.logger.info("=" * 80)

            return 0

        except Exception as e:
            self.logger.error(f"ERRO na execução: {str(e)}", exc_info=True)

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
                self.logger.info("Conexão com banco fechada")
