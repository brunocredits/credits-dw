# python/transformers/base_transformer.py
"""
Classe base para transformações Silver
Adaptada para estrutura Credits Brasil
"""

import pandas as pd
import hashlib
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import numpy as np

from utils.logger import setup_logger, log_dataframe_info
from utils.db_connection import get_connection, get_cursor
from utils.config import get_config


class BaseSilverTransformer(ABC):
    """
    Template Method pattern para transformações Silver
    """
    
    def __init__(self,
                 script_name: str,
                 tabela_origem: str,
                 tabela_destino: str,
                 tipo_carga: str = 'full',  # full, incremental, scd2
                 chave_natural: str = None):
        
        self.script_name = script_name
        self.tabela_origem = tabela_origem
        self.tabela_destino = tabela_destino
        self.tipo_carga = tipo_carga
        self.chave_natural = chave_natural
        self.logger = setup_logger(f"silver_{script_name}")
        self.config = get_config()
        
    @abstractmethod
    def extrair_bronze(self, conn) -> pd.DataFrame:
        """Extrai dados da camada Bronze"""
        pass
    
    @abstractmethod
    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica regras de negócio e transformações"""
        pass
    
    @abstractmethod
    def validar_qualidade(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Valida qualidade dos dados
        Returns: (sucesso: bool, erros: List[str])
        """
        pass
    
    def calcular_hash_registro(self, row: pd.Series, colunas: List[str]) -> str:
        """
        Calcula hash MD5 de um registro para detectar mudanças
        """
        valores = '|'.join([str(row[col]) for col in colunas if col in row.index])
        return hashlib.md5(valores.encode()).hexdigest()
    
    def processar_scd2(self, df_novo: pd.DataFrame, conn) -> pd.DataFrame:
        """
        Processa Slowly Changing Dimension Type 2
        """
        if not self.chave_natural:
            raise ValueError("Chave natural deve ser definida para SCD2")
        
        # Buscar registros atuais
        query = f"""
        SELECT * FROM {self.tabela_destino}
        WHERE flag_ativo = TRUE
        """
        df_atual = pd.read_sql(query, conn)
        
        if df_atual.empty:
            # Primeira carga
            df_novo['data_inicio'] = datetime.now().date()
            df_novo['data_fim'] = None
            df_novo['flag_ativo'] = True
            df_novo['versao'] = 1
            return df_novo
        
        # Identificar mudanças comparando hash
        df_merge = df_novo.merge(
            df_atual[[self.chave_natural, 'hash_registro', 'sk_cliente']],
            on=self.chave_natural,
            how='left',
            suffixes=('_novo', '_atual')
        )
        
        # Registros novos
        novos = df_merge[df_merge['sk_cliente'].isna()].copy()
        
        # Registros alterados
        alterados = df_merge[
            (df_merge['sk_cliente'].notna()) & 
            (df_merge['hash_registro_novo'] != df_merge['hash_registro_atual'])
        ].copy()
        
        # Processar alterações
        if not alterados.empty:
            # Fechar registros antigos
            for nk in alterados[self.chave_natural].unique():
                query_update = f"""
                UPDATE {self.tabela_destino}
                SET data_fim = CURRENT_DATE - INTERVAL '1 day',
                    flag_ativo = FALSE
                WHERE {self.chave_natural} = %s AND flag_ativo = TRUE
                """
                with get_cursor(conn) as cursor:
                    cursor.execute(query_update, (nk,))
            
            # Preparar novos registros versionados
            alterados['data_inicio'] = datetime.now().date()
            alterados['data_fim'] = None
            alterados['flag_ativo'] = True
            alterados['versao'] = alterados['versao'] + 1
        
        return pd.concat([novos, alterados], ignore_index=True)
    
    def executar(self) -> int:
        """
        Executa pipeline de transformação Silver
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info(f"🚀 Iniciando transformação Silver: {self.script_name}")
            self.logger.info(f"📊 Origem: {self.tabela_origem} → Destino: {self.tabela_destino}")
            self.logger.info(f"⚙️ Tipo de carga: {self.tipo_carga}")
            
            with get_connection() as conn:
                # 1. Extrair dados da Bronze
                self.logger.info("📥 Extraindo dados da camada Bronze...")
                df_bronze = self.extrair_bronze(conn)
                log_dataframe_info(df_bronze, "Bronze")
                
                # 2. Aplicar transformações
                self.logger.info("🔄 Aplicando transformações...")
                df_silver = self.aplicar_transformacoes(df_bronze)
                log_dataframe_info(df_silver, "Silver Transformado")
                
                # 3. Validar qualidade
                self.logger.info("✅ Validando qualidade dos dados...")
                valido, erros = self.validar_qualidade(df_silver)
                
                if not valido:
                    self.logger.error(f"❌ Validação falhou: {', '.join(erros)}")
                    return 1
                
                # 4. Processar conforme tipo de carga
                if self.tipo_carga == 'scd2':
                    df_silver = self.processar_scd2(df_silver, conn)
                
                # 5. Carregar na Silver
                self.logger.info(f"💾 Carregando {len(df_silver)} registros em {self.tabela_destino}...")

                with get_cursor(conn) as cursor:
                    if self.tipo_carga == 'full':
                        cursor.execute(f"TRUNCATE TABLE {self.tabela_destino} CASCADE")

                    # Inserir usando psycopg2 diretamente
                    cols = list(df_silver.columns)
                    placeholders = ','.join(['%s'] * len(cols))
                    insert_query = f"INSERT INTO {self.tabela_destino} ({','.join(cols)}) VALUES ({placeholders})"

                    for _, row in df_silver.iterrows():
                        cursor.execute(insert_query, tuple(row))

                    conn.commit()
                
                self.logger.success(f"✅ Transformação concluída com sucesso!")
                return 0
                
        except Exception as e:
            self.logger.error(f"❌ Erro na transformação: {str(e)}")
            return 1
