#!/usr/bin/env python3
"""Classe base para ingestão CSV na camada Bronze"""
import os
import sys
import pandas as pd
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple
from abc import ABC, abstractmethod
from psycopg2.extras import execute_values
from psycopg2 import sql

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.db_connection import get_db_connection, get_cursor
from utils.logger import setup_logger, log_dataframe_info, log_execution_summary
from utils.audit import registrar_execucao, finalizar_execucao
from utils.config import get_paths_config, get_csv_config, get_etl_config

# Whitelist de tabelas permitidas (segurança)
TABELAS_PERMITIDAS = {
    'bronze.contas_base_oficial', 'bronze.usuarios',
    'bronze.faturamento', 'bronze.data'
}

class BaseCSVIngestor(ABC):
    """Classe base para ingestores CSV (Template Method pattern)"""

    def __init__(self, script_name: str, tabela_destino: str,
                 arquivo_nome: str, input_subdir: str = '', format_dates: bool = True):
        if tabela_destino not in TABELAS_PERMITIDAS:
            raise ValueError(f"Tabela não permitida: {tabela_destino}")

        self.script_name = script_name
        self.tabela_destino = tabela_destino
        self.arquivo_nome = arquivo_nome
        self.format_dates = format_dates

        self.paths_config = get_paths_config()
        self.csv_config = get_csv_config()
        self.etl_config = get_etl_config()

        self.arquivo_path = self.paths_config.data_input_dir / input_subdir / arquivo_nome
        self.processed_dir = self.paths_config.data_processed_dir
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        self.logger = setup_logger(script_name)
        self.start_time = None

    @abstractmethod
    def get_column_mapping(self) -> Dict[str, str]:
        """Mapeamento colunas CSV -> Bronze"""
        pass

    @abstractmethod
    def get_bronze_columns(self) -> List[str]:
        """Lista de colunas da tabela Bronze"""
        pass

    def get_date_columns(self) -> List[str]:
        """Colunas de data (auto-detecta por prefixo)"""
        return [c for c in self.get_bronze_columns() if c.startswith(('data_', 'dt_'))]

    def validar_arquivo(self) -> bool:
        """Valida existência e permissões do arquivo"""
        if not self.arquivo_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {self.arquivo_path}")
        if not os.access(self.arquivo_path, os.R_OK):
            raise PermissionError(f"Sem permissão de leitura: {self.arquivo_path}")

        size_mb = self.arquivo_path.stat().st_size / 1024 / 1024
        self.logger.info(f"✓ Arquivo válido | {size_mb:.2f} MB")
        return True

    def ler_csv(self) -> pd.DataFrame:
        """Lê CSV com configurações centralizadas"""
        try:
            self.logger.info(f"📖 Lendo {self.arquivo_path.name}")
            df = pd.read_csv(
                self.arquivo_path,
                encoding=self.csv_config.encoding,
                sep=self.csv_config.separator,
                dtype=str,
                na_values=self.csv_config.na_values,
                keep_default_na=False
            )
            self.logger.success(f"✓ {len(df):,} linhas lidas")
            log_dataframe_info(df, f"CSV '{self.arquivo_nome}'")
            return df
        except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
            self.logger.error(f"❌ Erro ao ler CSV: {e}")
            raise

    def validar_colunas(self, df: pd.DataFrame) -> bool:
        """Valida presença de colunas obrigatórias"""
        esperadas = set(self.get_column_mapping().keys())
        presentes = set(df.columns)
        faltando = esperadas - presentes

        if faltando:
            raise ValueError(f"Colunas faltando: {faltando}")

        self.logger.success("✓ Todas colunas presentes")
        return True

    def transformar_para_bronze(self, df: pd.DataFrame) -> Tuple[List, List[str]]:
        """Prepara dados para Bronze"""
        self.logger.info("🔄 Transformando para Bronze...")

        df_bronze = df.rename(columns=self.get_column_mapping())
        colunas_bronze = self.get_bronze_columns()

        # Adicionar colunas faltantes
        for col in colunas_bronze:
            if col not in df_bronze.columns:
                df_bronze[col] = None
                self.logger.warning(f"⚠️ Coluna '{col}' preenchida com NULL")

        # Formatar datas
        if self.format_dates:
            df_bronze = self._formatar_datas(df_bronze)

        # Limpar e converter
        df_bronze = df_bronze.dropna(how='all')
        df_bronze = df_bronze.replace({pd.NA: None, pd.NaT: None})
        df_bronze = df_bronze.where(pd.notna(df_bronze), None)

        registros = df_bronze[colunas_bronze].values.tolist()
        self.logger.success(f"✓ {len(registros):,} registros preparados")
        return registros, colunas_bronze

    def inserir_bronze(self, conn, registros: List, colunas: List[str]) -> int:
        """Insere dados usando TRUNCATE/RELOAD com batch insert"""
        self.logger.info(f"💾 Inserindo {len(registros):,} registros")

        with get_cursor(conn) as cur:
            # TRUNCATE seguro usando psycopg2.sql
            schema, tabela = self.tabela_destino.split('.')
            truncate_query = sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(schema), sql.Identifier(tabela)
            )
            self.logger.info(f"🗑️ Truncando {self.tabela_destino}")
            cur.execute(truncate_query)

            # INSERT em batch com execute_values
            colunas_sql = sql.SQL(', ').join(map(sql.Identifier, colunas))
            insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                sql.Identifier(schema), sql.Identifier(tabela), colunas_sql
            )

            batch_size = self.etl_config.batch_insert_size
            total = 0

            for i in range(0, len(registros), batch_size):
                batch = registros[i:i + batch_size]
                execute_values(cur, insert_query, batch, page_size=batch_size)
                total += len(batch)
                if i + batch_size < len(registros):
                    self.logger.info(f"   • {total:,}/{len(registros):,}")

        self.logger.success(f"✓ {total:,} linhas inseridas")
        return total

    def _formatar_datas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Formata colunas de data para YYYY-MM-DD"""
        self.logger.info("📅 Formatando datas...")

        for col in self.get_date_columns():
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].where(df[col].notna(), None)
                df[col] = df[col].apply(
                    lambda x: x.strftime(self.csv_config.date_format)
                    if pd.notna(x) else None
                )

                nulos = df[col].isna().sum()
                if nulos > 0:
                    self.logger.warning(f"   ⚠️ '{col}': {nulos} datas inválidas")

        return df

    def mover_para_processed(self) -> None:
        """Move arquivo processado com timestamp"""
        try:
            ts = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            destino = self.processed_dir / f"{ts}_{self.arquivo_nome}"
            shutil.move(str(self.arquivo_path), str(destino))
            self.logger.success(f"✓ Arquivado: {destino.name}")
        except Exception as e:
            self.logger.warning(f"⚠️ Não foi possível arquivar: {e}")

    def executar(self) -> int:
        """Executa pipeline completo de ingestão"""
        conn = None
        execucao_id = None
        self.start_time = time.time()

        try:
            self.logger.info("=" * 80)
            self.logger.info(f"🚀 {self.script_name}")
            self.logger.info(f"🎯 {self.tabela_destino}")
            self.logger.info("=" * 80)

            self.validar_arquivo()

            conn = get_db_connection()
            self.logger.success("✓ Conectado ao banco")

            execucao_id = registrar_execucao(
                conn, self.script_name, 'bronze', None, self.tabela_destino
            )

            df = self.ler_csv()
            self.validar_colunas(df)
            registros, colunas = self.transformar_para_bronze(df)
            linhas_inseridas = self.inserir_bronze(conn, registros, colunas)

            conn.commit()
            self.mover_para_processed()

            duracao = time.time() - self.start_time
            finalizar_execucao(conn, execucao_id, 'sucesso',
                             len(df), linhas_inseridas)

            log_execution_summary(self.script_name, 'sucesso',
                                len(df), linhas_inseridas, duracao)
            return 0

        except Exception as e:
            self.logger.error(f"❌ {e}", exc_info=True)
            duracao = time.time() - self.start_time if self.start_time else 0

            if conn:
                conn.rollback()
                if execucao_id:
                    finalizar_execucao(conn, execucao_id, 'erro', mensagem_erro=str(e))

            log_execution_summary(self.script_name, 'erro', duracao_segundos=duracao)
            return 1

        finally:
            if conn:
                conn.close()
