#!/usr/bin/env python3
"""
Módulo: base_json_ingestor.py
Descrição: Classe base para ingestão de arquivos JSON na camada Bronze
Versão: 2.0
"""

import os
import sys
import json
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


class BaseJSONIngestor(ABC):
    """
    Classe base abstrata para ingestores JSON.
    Suporta tanto JSON array quanto line-delimited JSON (NDJSON).
    """

    def __init__(self,
                 script_name: str,
                 tabela_destino: str,
                 arquivo_nome: str,
                 input_subdir: str = '',
                 json_lines: bool = False):
        """
        Inicializa o ingestor JSON base.

        Args:
            script_name: Nome do script (para auditoria)
            tabela_destino: Nome completo da tabela Bronze (schema.tabela)
            arquivo_nome: Nome do arquivo JSON a ser processado
            input_subdir: Subdiretório dentro de DATA_INPUT_PATH
            json_lines: True se for NDJSON (uma linha por objeto)
        """
        self.script_name = script_name
        self.tabela_destino = tabela_destino
        self.arquivo_nome = arquivo_nome
        self.json_lines = json_lines

        # Configurar caminhos
        data_input = os.getenv('DATA_INPUT_PATH', './docker/data/input')
        self.arquivo_path = os.path.join(data_input, input_subdir, arquivo_nome)

        data_processed = os.getenv('DATA_PROCESSED_PATH', './docker/data/processed')
        self.processed_dir = data_processed

        # Logger
        self.logger = setup_logger(script_name)

    @abstractmethod
    def get_column_mapping(self) -> Dict[str, str]:
        """Retorna o mapeamento de campos JSON -> colunas Bronze"""
        pass

    @abstractmethod
    def get_bronze_columns(self) -> List[str]:
        """Retorna a lista de colunas da tabela Bronze"""
        pass

    def ler_json(self) -> pd.DataFrame:
        """
        Lê arquivo JSON e converte para DataFrame.
        """
        try:
            self.logger.info(f"Lendo arquivo JSON: {self.arquivo_path}")

            if self.json_lines:
                # NDJSON: uma linha por objeto
                df = pd.read_json(self.arquivo_path, lines=True, dtype=str)
            else:
                # JSON array padrão
                with open(self.arquivo_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        # Se for um objeto com chave 'data' ou similar
                        if 'data' in data:
                            data = data['data']
                        elif 'items' in data:
                            data = data['items']
                        elif 'results' in data:
                            data = data['results']
                    df = pd.DataFrame(data)

            # Converter tudo para string (padrão Bronze)
            df = df.astype(str)

            self.logger.info(f"Arquivo lido com sucesso. Total de linhas: {len(df)}")
            return df

        except FileNotFoundError:
            self.logger.error(f"Arquivo não encontrado: {self.arquivo_path}")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao ler JSON: {str(e)}")
            raise

    def transformar_para_bronze(self, df: pd.DataFrame) -> tuple:
        """Prepara dados JSON para inserção na Bronze"""
        self.logger.info("Preparando dados JSON para inserção na Bronze")

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

        if 'data_carga_bronze' not in colunas_bronze:
            colunas_bronze.append('data_carga_bronze')
        if 'nome_arquivo_origem' not in colunas_bronze:
            colunas_bronze.append('nome_arquivo_origem')

        registros = df_bronze[colunas_bronze].values.tolist()

        self.logger.info(f"Total de registros preparados: {len(registros)}")
        return registros, colunas_bronze

    def inserir_bronze(self, conn, registros: List, colunas: List) -> int:
        """Insere registros na tabela Bronze"""
        from psycopg2.extras import execute_values

        self.logger.info(f"Inserindo {len(registros)} registros na tabela {self.tabela_destino}")

        try:
            cursor = conn.cursor()

            # TRUNCATE/RELOAD
            cursor.execute(f"TRUNCATE TABLE {self.tabela_destino}")
            self.logger.info(f"Tabela {self.tabela_destino} truncada")

            # Inserir dados
            colunas_str = ', '.join(colunas)
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
        """Move arquivo processado para backup"""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            nome_com_timestamp = f"{timestamp}_{self.arquivo_nome}"
            destino = os.path.join(self.processed_dir, nome_com_timestamp)

            os.makedirs(self.processed_dir, exist_ok=True)
            shutil.move(self.arquivo_path, destino)
            self.logger.info(f"Arquivo movido para: {destino}")

        except Exception as e:
            self.logger.warning(f"Não foi possível mover arquivo: {str(e)}")

    def executar(self) -> int:
        """Executa o processo completo de ingestão JSON"""
        conn = None
        execucao_id = None

        try:
            self.logger.info("=" * 80)
            self.logger.info(f"Iniciando ingestão JSON: {self.script_name}")
            self.logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info("=" * 80)

            if not os.path.exists(self.arquivo_path):
                raise FileNotFoundError(f"Arquivo não encontrado: {self.arquivo_path}")

            conn = get_db_connection()
            self.logger.info("Conexão com banco estabelecida")

            execucao_id = registrar_execucao(
                conn=conn,
                script_nome=self.script_name,
                camada='bronze',
                tabela_destino=self.tabela_destino
            )
            self.logger.info(f"Execução registrada com ID: {execucao_id}")

            df = self.ler_json()
            linhas_lidas = len(df)

            registros, colunas = self.transformar_para_bronze(df)
            linhas_inseridas = self.inserir_bronze(conn, registros, colunas)

            self.mover_para_processed()

            finalizar_execucao(
                conn=conn,
                execucao_id=execucao_id,
                status='sucesso',
                linhas_processadas=linhas_lidas,
                linhas_inseridas=linhas_inseridas
            )

            self.logger.info("=" * 80)
            self.logger.info("Ingestão JSON concluída com SUCESSO!")
            self.logger.info(f"Linhas processadas: {linhas_lidas}")
            self.logger.info(f"Linhas inseridas: {linhas_inseridas}")
            self.logger.info("=" * 80)

            return 0

        except Exception as e:
            self.logger.error(f"ERRO na execução: {str(e)}", exc_info=True)

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
