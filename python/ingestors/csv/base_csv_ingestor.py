#!/usr/bin/env python3
"""
Módulo: base_csv_ingestor.py
Descrição: Classe base para ingestão CSV na camada Bronze (Raw/Permissive)
Versão: 3.0

MUDANÇAS:
- Abordagem "Permissive": Não descarta linhas (Bronze é raw).
- Schema Evolution: Cria colunas automaticamente se não existirem.
- Validação Suave: Corrige tipos e preenche vazios em vez de rejeitar.
- Metadata: Adiciona ingest_timestamp, source_filename, etc.
"""

import os
import sys
import pandas as pd
import numpy as np
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any
from abc import ABC, abstractmethod
from psycopg2.extras import execute_values
from psycopg2 import sql

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.db_connection import get_db_connection, get_cursor
from utils.logger import setup_logger, log_execution_summary
from utils.audit import registrar_execucao, finalizar_execucao
from utils.config import get_paths_config, get_csv_config, get_etl_config


# ============================================================================
# CONFIGURAÇÃO DE SEGURANÇA
# ============================================================================

TABELAS_PERMITIDAS = {
    'bronze.base_oficial', # Antiga contas
    'bronze.usuarios',
    'bronze.faturamento',  # Antiga faturamentos
    'bronze.data',         # Antiga calendario/data
    'bronze.faturamentos', # Compatibilidade
    'bronze.contas',       # Compatibilidade
    'bronze.calendario'    # Compatibilidade
}


class BaseCSVIngestor(ABC):
    """
    Classe base para ingestão CSV na camada Bronze com Schema Evolution e permissividade.
    """

    def __init__(
        self,
        script_name: str,
        tabela_destino: str,
        arquivo_nome: str
    ):
        if tabela_destino not in TABELAS_PERMITIDAS:
            raise ValueError(f"Tabela não permitida: {tabela_destino}")

        self.script_name = script_name
        self.tabela_destino = tabela_destino
        self.arquivo_nome = arquivo_nome

        self.paths_config = get_paths_config()
        self.csv_config = get_csv_config()
        self.etl_config = get_etl_config()

        self.arquivo_path = self.paths_config.data_input_dir / arquivo_nome
        self.processed_dir = self.paths_config.data_processed_dir
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        self.logger = setup_logger(script_name)
        self.start_time = None

    @abstractmethod
    def get_column_mapping(self) -> Dict[str, str]:
        """Mapeamento CSV -> Bronze (Snake Case)"""
        pass

    @abstractmethod
    def get_mandatory_fields(self) -> List[str]:
        """Campos que não podem ser nulos (serão preenchidos com default)"""
        pass

    def get_custom_schema(self) -> Dict[str, str]:
        """
        Retorna tipos SQL customizados para criação da tabela.
        Ex: {'receita': 'DECIMAL(18,2)', 'obs': 'TEXT'}
        Se não definido, usa TEXT ou inferência básica.
        """
        return {}

    # ========================================================================
    # UTILITÁRIOS DE TEXTO E SCHEMA
    # ========================================================================

    def normalize_column_name(self, col: str) -> str:
        """Converte para snake_case e remove caracteres especiais."""
        import re
        import unidecode
        
        # Remove acentos
        col = unidecode.unidecode(col)
        # Converte para minúsculo
        col = col.lower()
        # Substitui caracteres não alfanuméricos por underscore
        col = re.sub(r'[^a-z0-9]+', '_', col)
        # Remove underscores repetidos e nas pontas
        col = re.sub(r'_+', '_', col).strip('_')
        return col

    def detect_sql_type(self, series: pd.Series) -> str:
        """Infere o tipo SQL Postgres baseado na série Pandas."""
        if pd.api.types.is_integer_dtype(series):
            return "BIGINT"
        elif pd.api.types.is_float_dtype(series):
            return "DECIMAL(18,2)" # Seguro para moeda
        elif pd.api.types.is_datetime64_any_dtype(series):
            return "DATE" # ou TIMESTAMP
        else:
            # Verifica se parece data
            try:
                # Amostragem para performance
                sample = series.dropna().head(100)
                if not sample.empty:
                    pd.to_datetime(sample)
                    return "DATE"
            except:
                pass
            return "TEXT"

    # ========================================================================
    # VALIDAÇÃO E CORREÇÃO (PERMISSIVA)
    # ========================================================================

    def validar_arquivo(self) -> bool:
        if not self.arquivo_path.exists():
            # Tenta encontrar arquivo com case insensitive ou similar
            files = list(self.paths_config.data_input_dir.glob("*"))
            for f in files:
                if f.name.lower() == self.arquivo_nome.lower():
                    self.arquivo_path = f
                    self.logger.info(f"Arquivo encontrado (match nome): {f.name}")
                    return True
            raise FileNotFoundError(f"Arquivo não encontrado: {self.arquivo_path}")
        return True

    def ler_csv(self) -> pd.DataFrame:
        self.logger.info(f"Lendo arquivo: {self.arquivo_path.name}")
        try:
            # Ler tudo como string/object primeiro para evitar erros de parse
            df = pd.read_csv(
                self.arquivo_path,
                encoding='utf-8', # Forçar utf-8 ou tentar latin1 se falhar
                sep=self.csv_config.separator,
                dtype=str,
                on_bad_lines='warn' # Não crashar em linhas ruins
            )
        except UnicodeDecodeError:
            self.logger.warning("Falha com UTF-8, tentando Latin-1")
            df = pd.read_csv(
                self.arquivo_path,
                encoding='latin1',
                sep=self.csv_config.separator,
                dtype=str,
                on_bad_lines='warn'
            )
        
        return df

    def normalizar_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica mapeamento e normalização de colunas."""
        
        # 1. Aplicar mapeamento explícito
        mapping = self.get_column_mapping()
        df = df.rename(columns=mapping)
        
        # 2. Normalizar colunas restantes que não estavam no mapeamento
        new_cols = {c: self.normalize_column_name(c) for c in df.columns if c not in mapping.values()}
        df = df.rename(columns=new_cols)
        
        # 3. Adicionar Metadata
        df['ingest_timestamp'] = datetime.now().isoformat()
        df['source_filename'] = self.arquivo_nome
        
        return df

    def corrigir_dados_criticos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Verifica campos obrigatórios e loga avisos se estiverem vazios.
        NÃO preenche automaticamente (mantém NULL/None).
        """
        mandatory = self.get_mandatory_fields()
        
        for col in mandatory:
            if col not in df.columns:
                self.logger.error(f"❌ Coluna obrigatória '{col}' não existe no arquivo!")
                continue
            
            # Identificar linhas com problema
            # (Pandas isna() pega None e NaN)
            bad_rows = df[df[col].isna() | (df[col] == '')].index
            
            total_bad = len(bad_rows)
            
            if total_bad > 0:
                self.logger.warning(f"⚠️  Campo '{col}' tem {total_bad} valores vazios.")
                # Logar amostra das primeiras 5 linhas ruins
                for i in bad_rows[:5]:
                    # +2 para aproximar do numero da linha do CSV (header + 0-index)
                    self.logger.warning(f"   -> Linha {i+2}: Valor ausente para '{col}'")
                
                if total_bad > 5:
                    self.logger.warning(f"   -> ... e mais {total_bad - 5} linhas.")

        return df

    def transform_custom(self, df: pd.DataFrame) -> pd.DataFrame:
        """Hook para transformações customizadas nas subclasses"""
        return df

    # ========================================================================
    # SCHEMA SYNC (NO EVOLUTION - STRICT)
    # ========================================================================

    def sync_schema(self, conn, df: pd.DataFrame) -> pd.DataFrame:
        """
        Verifica schema no banco.
        - Se não existe: Cria.
        - Se existe: Filtra DF para manter apenas colunas que existem no banco.
        
        Returns:
            DataFrame filtrado (apenas colunas validas)
        """
        schema, table = self.tabela_destino.split('.')
        
        with get_cursor(conn) as cur:
            # Verificar se tabela existe
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                );
            """, (schema, table))
            exists = cur.fetchone()[0]
            
            if not exists:
                self.logger.info(f"Tabela {self.tabela_destino} não existe. Criando...")
                self._create_table(cur, schema, table, df)
                return df
            else:
                # Obter colunas existentes no banco
                cur.execute("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s;
                """, (schema, table))
                db_cols = {row[0] for row in cur.fetchall()}
                
                # Identificar colunas no DF que não estão no banco
                extra_cols = [c for c in df.columns if c not in db_cols]
                if extra_cols:
                    self.logger.warning(f"Ignorando {len(extra_cols)} colunas que não existem no banco: {extra_cols[:5]}...")
                    df = df.drop(columns=extra_cols)
                
                # Garantir ordem (opcional, mas bom)
                common_cols = [c for c in df.columns if c in db_cols]
                return df[common_cols]

    def _create_table(self, cur, schema, table, df):
        """Cria tabela baseada nas colunas do DataFrame."""
        cols_def = []
        custom_types = self.get_custom_schema()
        
        for col in df.columns:
            if col in custom_types:
                ctype = custom_types[col]
            else:
                ctype = "TEXT" # Default safe para Bronze
            
            cols_def.append(sql.SQL("{} {}").format(
                sql.Identifier(col),
                sql.SQL(ctype)
            ))
        
        query = sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(cols_def)
        )
        cur.execute(query)

    # ========================================================================
    # EXECUÇÃO
    # ========================================================================

    def executar(self) -> int:
        conn = None
        execucao_fk = None
        try:
            self.validar_arquivo()
            
            conn = get_db_connection()
            execucao_fk = registrar_execucao(conn, self.script_name, 'bronze', None, self.tabela_destino)
            
            # 1. Ler
            df = self.ler_csv()
            
            # 2. Normalizar (Rename, Snake Case, Metadata)
            df = self.normalizar_dados(df)
            
            # 3. Transformações Customizadas (Antes de validar obrigatórios)
            df = self.transform_custom(df)
            
            # 4. Validar/Corrigir (Mandatory fields)
            df = self.corrigir_dados_criticos(df)
            
            # 5. Sync Schema (Strict - Drop extra cols)
            df = self.sync_schema(conn, df)
            
            if df.empty:
                self.logger.warning("DataFrame vazio após filtragem de colunas.")
                return 0

            # 6. Inserir (Truncate + Insert)
            registros = df.where(pd.notna(df), None).values.tolist()
            cols = list(df.columns)
            
            with get_cursor(conn) as cur:
                schema, table = self.tabela_destino.split('.')
                
                # Truncate
                cur.execute(sql.SQL("TRUNCATE TABLE {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                ))
                
                # Batch Insert
                insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.SQL(', ').join(map(sql.Identifier, cols))
                )
                
                execute_values(cur, insert_query, registros, page_size=1000)
            
            conn.commit()
            
            # Audit
            finalizar_execucao(conn, execucao_fk, 'sucesso', len(df), len(df), 0)
            self.logger.success(f"Sucesso: {len(df)} linhas em {self.tabela_destino}")
            
            # Move processed
            if self.arquivo_path.exists():
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                try:
                    shutil.move(str(self.arquivo_path), str(self.processed_dir / f"{ts}_{self.arquivo_nome}"))
                    self.logger.info(f"Arquivo movido para processed: {ts}_{self.arquivo_nome}")
                except Exception as move_err:
                    self.logger.warning(f"Erro ao mover arquivo: {move_err}")
            
            return 0

        except Exception as e:
            self.logger.error(f"Erro fatal: {e}", exc_info=True)
            if conn:
                conn.rollback()
                if execucao_fk:
                    finalizar_execucao(conn, execucao_fk, 'erro', mensagem_erro=str(e))
            return 1
        finally:
            if conn: conn.close()