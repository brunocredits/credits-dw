#!/usr/bin/env python3
"""
Módulo: base_csv_ingestor.py
Descrição: Classe base para ingestão de arquivos CSV na camada Bronze
Versão: 3.0

Melhorias:
- Usa Loguru para logging aprimorado
- Configuração centralizada
- Context managers para recursos
- Retry logic integrado
- Métricas de performance
- Validação de schema
"""

import os
import sys
import pandas as pd
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from abc import ABC, abstractmethod
import time

# Adicionar diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.db_connection import get_db_connection, get_cursor
from utils.logger import setup_logger, log_dataframe_info, log_execution_summary
from utils.audit import registrar_execucao, finalizar_execucao
from utils.config import get_config, get_paths_config, get_csv_config, get_etl_config


class BaseCSVIngestor(ABC):
    """
    Classe base abstrata para ingestores CSV.
    Implementa o padrão Template Method para processamento de arquivos CSV.
    """

    def __init__(self,
                 script_name: str,
                 tabela_destino: str,
                 arquivo_nome: str,
                 input_subdir: str = '',
                 format_dates: bool = True):
        """
        Inicializa o ingestor base.

        Args:
            script_name: Nome do script (para auditoria)
            tabela_destino: Nome completo da tabela Bronze (schema.tabela)
            arquivo_nome: Nome do arquivo CSV a ser processado
            input_subdir: Subdiretório dentro de DATA_INPUT_PATH (ex: 'onedrive')
            format_dates: Se as colunas de data devem ser formatadas
        """
        self.script_name = script_name
        self.tabela_destino = tabela_destino
        self.arquivo_nome = arquivo_nome
        self.format_dates = format_dates

        # Carregar configurações
        self.config = get_config()
        self.paths_config = get_paths_config()
        self.csv_config = get_csv_config()
        self.etl_config = get_etl_config()

        # Configurar caminhos
        self.arquivo_path = self.paths_config.data_input_dir / input_subdir / arquivo_nome
        self.processed_dir = self.paths_config.data_processed_dir

        # Criar diretórios se não existirem
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        # Logger
        self.logger = setup_logger(script_name)

        # Métricas de execução
        self.start_time = None
        self.end_time = None

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

    def get_date_columns(self) -> List[str]:
        """
        Retorna lista de colunas que devem ser formatadas como data.
        Pode ser sobrescrito para comportamento específico.

        Returns:
            Lista de nomes de colunas de data (padrão: colunas que começam com 'data_' ou 'dt_')
        """
        return [col for col in self.get_bronze_columns() if col.startswith(('data_', 'dt_'))]

    def validar_arquivo(self) -> bool:
        """
        Valida se o arquivo existe e é acessível.

        Returns:
            bool: True se arquivo válido

        Raises:
            FileNotFoundError: Se arquivo não existe
            PermissionError: Se arquivo não é acessível
        """
        if not self.arquivo_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {self.arquivo_path}")

        if not os.access(self.arquivo_path, os.R_OK):
            raise PermissionError(f"Sem permissão de leitura: {self.arquivo_path}")

        file_size = self.arquivo_path.stat().st_size
        self.logger.info(f"✓ Arquivo válido | Tamanho: {file_size / 1024 / 1024:.2f} MB")

        return True

    def ler_csv(self) -> pd.DataFrame:
        """
        Lê arquivo CSV com configurações centralizadas.

        Returns:
            DataFrame com dados do CSV

        Raises:
            Exception: Em caso de erro na leitura
        """
        try:
            self.logger.info(f"📖 Lendo arquivo CSV: {self.arquivo_path.name}")

            df = pd.read_csv(
                self.arquivo_path,
                encoding=self.csv_config.encoding,
                sep=self.csv_config.separator,
                dtype=str,  # Bronze: tudo como string inicialmente
                na_values=self.csv_config.na_values,
                keep_default_na=False
            )

            self.logger.success(f"✓ Arquivo lido com sucesso | {len(df):,} linhas")

            # Log de informações do DataFrame
            log_dataframe_info(df, f"CSV '{self.arquivo_nome}'")

            return df

        except pd.errors.EmptyDataError:
            self.logger.error("❌ Arquivo CSV está vazio")
            raise
        except pd.errors.ParserError as e:
            self.logger.error(f"❌ Erro ao parsear CSV: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"❌ Erro ao ler CSV: {str(e)}")
            raise

    def validar_colunas(self, df: pd.DataFrame) -> bool:
        """
        Valida se as colunas esperadas do CSV estão presentes.

        Args:
            df: DataFrame lido do CSV

        Returns:
            bool: True se todas as colunas estão presentes

        Raises:
            ValueError: Se colunas obrigatórias estiverem faltando
        """
        colunas_csv = set(df.columns)
        colunas_esperadas = set(self.get_column_mapping().keys())
        colunas_faltando = colunas_esperadas - colunas_csv

        if colunas_faltando:
            self.logger.error(f"❌ Colunas obrigatórias faltando no CSV: {colunas_faltando}")
            raise ValueError(f"Colunas obrigatórias faltando: {colunas_faltando}")

        self.logger.success("✓ Todas as colunas obrigatórias estão presentes")
        return True

    def transformar_para_bronze(self, df: pd.DataFrame) -> Tuple[List, List[str]]:
        """
        Prepara dados para inserção na Bronze usando mapeamento de colunas.

        Args:
            df: DataFrame com dados do CSV

        Returns:
            Tuple com (registros, nomes_das_colunas)
        """
        self.logger.info("🔄 Transformando dados para formato Bronze...")

        # Aplicar mapeamento de colunas
        df_bronze = df.rename(columns=self.get_column_mapping())

        # Garantir que todas as colunas Bronze existem
        colunas_bronze = self.get_bronze_columns()
        for col in colunas_bronze:
            if col not in df_bronze.columns and col not in ['data_carga_bronze', 'nome_arquivo_origem']:
                df_bronze[col] = None
                self.logger.warning(f"⚠️  Coluna '{col}' não encontrada, preenchendo com NULL")

        # Formatar colunas de data, se necessário
        if self.format_dates:
            df_bronze = self._formatar_colunas_data(df_bronze)

        # Remover linhas completamente vazias
        df_bronze = df_bronze.dropna(how='all')

        registros = df_bronze[colunas_bronze].values.tolist()

        self.logger.success(f"✓ Transformação concluída | {len(registros):,} registros preparados")
        return registros, colunas_bronze

    def inserir_bronze(self, conn, registros: List, colunas: List[str]) -> int:
        """
        Insere registros na tabela Bronze usando estratégia TRUNCATE/RELOAD.

        Args:
            conn: Conexão com banco de dados
            registros: Lista de registros a inserir
            colunas: Lista de nomes de colunas

        Returns:
            int: Número de linhas inseridas

        Raises:
            Exception: Em caso de erro na inserção
        """
        from psycopg2.extras import execute_values

        self.logger.info(f"💾 Inserindo {len(registros):,} registros em {self.tabela_destino}...")

        try:
            with get_cursor(conn) as cursor:
                # Limpar tabela Bronze (TRUNCATE/RELOAD strategy)
                self.logger.info(f"🗑️  Truncando tabela {self.tabela_destino}")
                cursor.execute(f"TRUNCATE TABLE {self.tabela_destino}")
                conn.commit()

                # Inserir dados em batches
                colunas_str = ', '.join([f'"{col}"' for col in colunas])
                query = f"INSERT INTO {self.tabela_destino} ({colunas_str}) VALUES %s"

                batch_size = self.etl_config.batch_insert_size
                total_inserted = 0

                for i in range(0, len(registros), batch_size):
                    batch = registros[i:i + batch_size]
                    execute_values(cursor, query, batch, page_size=batch_size)
                    total_inserted += len(batch)

                    if i + batch_size < len(registros):
                        self.logger.info(f"   • Inseridos: {total_inserted:,}/{len(registros):,} registros")

                conn.commit()

            self.logger.success(f"✓ Inserção concluída | {total_inserted:,} linhas inseridas")
            return total_inserted

        except Exception as e:
            conn.rollback()
            self.logger.error(f"❌ Erro na inserção: {str(e)}")
            raise

    def _formatar_colunas_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Formata colunas de data para o padrão YYYY-MM-DD.

        Args:
            df: DataFrame com dados

        Returns:
            DataFrame com datas formatadas
        """
        self.logger.info("📅 Formatando colunas de data...")

        date_columns = self.get_date_columns()

        for col in date_columns:
            if col in df.columns:
                try:
                    # Converter para datetime
                    df[col] = pd.to_datetime(df[col], errors='coerce')

                    # Substituir NaT por None
                    df[col] = df[col].where(df[col].notna(), None)

                    # Formatar para string YYYY-MM-DD
                    df[col] = df[col].apply(
                        lambda x: x.strftime(self.csv_config.date_format) if pd.notna(x) else None
                    )

                    valores_nulos = df[col].isna().sum()
                    if valores_nulos > 0:
                        self.logger.warning(f"   ⚠️  '{col}': {valores_nulos} datas inválidas convertidas para NULL")
                    else:
                        self.logger.debug(f"   ✓ '{col}': formatada com sucesso")

                except Exception as e:
                    self.logger.warning(f"   ⚠️  Não foi possível formatar '{col}': {e}")

        return df

    def mover_para_processed(self) -> None:
        """
        Move arquivo processado para diretório de backup com timestamp.
        """
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            nome_com_timestamp = f"{timestamp}_{self.arquivo_nome}"
            destino = self.processed_dir / nome_com_timestamp

            shutil.move(str(self.arquivo_path), str(destino))
            self.logger.success(f"✓ Arquivo arquivado: {destino.name}")

        except Exception as e:
            self.logger.warning(f"⚠️  Não foi possível arquivar arquivo: {str(e)}")

    def executar(self) -> int:
        """
        Executa o processo completo de ingestão com métricas e auditoria.

        Returns:
            0 em caso de sucesso, 1 em caso de erro
        """
        conn = None
        execucao_id = None
        self.start_time = time.time()

        try:
            self.logger.info("=" * 80)
            self.logger.info(f"🚀 Iniciando ingestão: {self.script_name}")
            self.logger.info(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"🎯 Destino: {self.tabela_destino}")
            self.logger.info("=" * 80)

            # 1. Validar arquivo
            self.validar_arquivo()

            # 2. Conectar ao banco
            conn = get_db_connection()
            self.logger.success("✓ Conexão com banco estabelecida")

            # 3. Registrar início da execução
            execucao_id = registrar_execucao(
                conn=conn,
                script_nome=self.script_name,
                camada='bronze',
                tabela_destino=self.tabela_destino
            )
            self.logger.info(f"📝 Execução registrada com ID: {execucao_id}")

            # 4. Ler CSV
            df = self.ler_csv()
            linhas_lidas = len(df)

            # 5. Validar colunas
            self.validar_colunas(df)

            # 6. Preparar para Bronze
            registros, colunas = self.transformar_para_bronze(df)

            # 7. Inserir na Bronze
            linhas_inseridas = self.inserir_bronze(conn, registros, colunas)

            # 8. Mover arquivo para processed
            self.mover_para_processed()

            # 9. Calcular métricas
            self.end_time = time.time()
            duracao = self.end_time - self.start_time

            # 10. Finalizar execução com sucesso
            finalizar_execucao(
                conn=conn,
                execucao_id=execucao_id,
                status='sucesso',
                linhas_processadas=linhas_lidas,
                linhas_inseridas=linhas_inseridas
            )

            # 11. Log de resumo
            log_execution_summary(
                script_name=self.script_name,
                status='sucesso',
                linhas_processadas=linhas_lidas,
                linhas_inseridas=linhas_inseridas,
                duracao_segundos=duracao
            )

            return 0

        except Exception as e:
            self.logger.error(f"❌ ERRO na execução: {str(e)}", exc_info=True)

            # Calcular duração mesmo em caso de erro
            if self.start_time:
                duracao = time.time() - self.start_time
            else:
                duracao = 0

            # Registrar erro na auditoria
            if conn and execucao_id:
                finalizar_execucao(
                    conn=conn,
                    execucao_id=execucao_id,
                    status='erro',
                    mensagem_erro=str(e)
                )

            # Log de resumo de erro
            log_execution_summary(
                script_name=self.script_name,
                status='erro',
                duracao_segundos=duracao
            )

            return 1

        finally:
            if conn:
                conn.close()
                self.logger.info("🔌 Conexão com banco fechada")
