"""
Módulo: base_transformer.py
Descrição: Classe base para transformações Silver com SCD Type 2
Versão: 2.0
"""

import pandas as pd
import hashlib
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple
from datetime import datetime
from psycopg2.extras import execute_values
from psycopg2 import sql

from utils.logger import setup_logger, log_dataframe_info
from utils.db_connection import get_connection, get_cursor

# Whitelists de segurança
TABELAS_BRONZE_PERMITIDAS = {
    'bronze.contas_base_oficial', 'bronze.usuarios',
    'bronze.faturamento', 'bronze.data'
}
TABELAS_SILVER_PERMITIDAS = {
    'silver.dim_clientes', 'silver.dim_usuarios', 'silver.dim_tempo',
    'silver.dim_canal', 'silver.fact_faturamento'
}
TABELAS_CONTROLE_PERMITIDAS = {
    'credits.historico_atualizacoes', 'credits.silver_control'
}

class BaseSilverTransformer(ABC):
    """
    Classe base para transformações Silver (Template Method pattern).

    Implementa pipeline padrão: extrair -> transformar -> validar -> carregar
    Com suporte a SCD Type 2 para dimensões.

    Args:
        script_name: Nome do script para logs
        tabela_origem: Tabela Bronze de origem
        tabela_destino: Tabela Silver de destino
        tipo_carga: 'full', 'incremental' ou 'scd2'
        chave_natural: Chave natural para SCD Type 2 (obrigatória se tipo_carga='scd2')
    """

    def __init__(self, script_name: str, tabela_origem: str,
                 tabela_destino: str, tipo_carga: str = 'full',
                 chave_natural: str = None):
        # Validações de segurança
        if tabela_origem not in TABELAS_BRONZE_PERMITIDAS:
            raise ValueError(f"Tabela origem não permitida: {tabela_origem}")
        if tabela_destino not in TABELAS_SILVER_PERMITIDAS:
            raise ValueError(f"Tabela destino não permitida: {tabela_destino}")
        if tipo_carga not in ('full', 'incremental', 'scd2'):
            raise ValueError(f"Tipo de carga inválido: {tipo_carga}")

        self.script_name = script_name
        self.tabela_origem = tabela_origem
        self.tabela_destino = tabela_destino
        self.tipo_carga = tipo_carga
        self.chave_natural = chave_natural
        self.logger = setup_logger(f"silver_{script_name}")

    @abstractmethod
    def extrair_bronze(self, conn) -> pd.DataFrame:
        """
        Extrai dados da Bronze.

        Args:
            conn: Conexão com o banco de dados

        Returns:
            DataFrame com dados da Bronze
        """
        pass

    @abstractmethod
    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica regras de negócio e transformações.

        Args:
            df: DataFrame extraído da Bronze

        Returns:
            DataFrame transformado para Silver
        """
        pass

    @abstractmethod
    def validar_qualidade(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Valida qualidade dos dados transformados.

        Args:
            df: DataFrame transformado

        Returns:
            Tupla (sucesso, lista_de_erros)
        """
        pass

    def calcular_hash_registro(self, row: pd.Series, colunas: List[str]) -> str:
        """
        Calcula hash MD5 para detectar mudanças em SCD Type 2.

        Args:
            row: Linha do DataFrame
            colunas: Colunas a incluir no hash

        Returns:
            Hash MD5 hexadecimal
        """
        valores = '|'.join([str(row[col]) for col in colunas if col in row.index])
        return hashlib.md5(valores.encode()).hexdigest()

    def _obter_nome_pk(self) -> str:
        """
        Obtém nome da coluna PK baseado no nome da tabela.

        Returns:
            Nome da coluna PK (ex: 'sk_cliente', 'sk_usuario')
        """
        tabela_nome = self.tabela_destino.split('.')[-1]
        pk_base = tabela_nome.replace('dim_', '').replace('fact_', '')
        # Remover 's' final se for plural (clientes -> cliente, usuarios -> usuario)
        if pk_base.endswith('s') and pk_base != 'status':
            pk_base = pk_base[:-1]
        return f"sk_{pk_base}"

    def processar_scd2(self, df_novo: pd.DataFrame, conn) -> pd.DataFrame:
        """
        Processa Slowly Changing Dimension Type 2.

        Compara registros novos com registros atuais na dimensão.
        Fecha registros alterados e cria novas versões.

        Args:
            df_novo: DataFrame com novos dados
            conn: Conexão com o banco de dados

        Returns:
            DataFrame com registros SCD2 processados
        """
        if not self.chave_natural:
            raise ValueError("Chave natural obrigatória para SCD2")

        pk_col = self._obter_nome_pk()

        schema, tabela = self.tabela_destino.split('.')
        query_str = f"SELECT * FROM {schema}.{tabela} WHERE flag_ativo = TRUE"
        df_atual = pd.read_sql(query_str, conn)

        if df_atual.empty:
            # Primeira carga: marcar todos como novos
            df_novo['data_inicio'] = datetime.now().date()
            df_novo['data_fim'] = None
            df_novo['flag_ativo'] = True
            df_novo['versao'] = 1
            return df_novo

        # Comparar novos x atuais
        df_merge = df_novo.merge(
            df_atual[[self.chave_natural, 'hash_registro', pk_col]],
            on=self.chave_natural,
            how='left',
            suffixes=('_novo', '_atual')
        )

        # Separar novos e alterados
        novos = df_merge[df_merge[pk_col].isna()].copy()
        alterados = df_merge[
            (df_merge[pk_col].notna()) &
            (df_merge['hash_registro_novo'] != df_merge['hash_registro_atual'])
        ].copy()

        # Fechar registros antigos (SCD Type 2)
        if not alterados.empty:
            self.logger.info(f"{len(alterados)} registros alterados detectados")
            for nk in alterados[self.chave_natural].unique():
                update_query = sql.SQL("""
                    UPDATE {}.{}
                    SET data_fim = CURRENT_DATE - INTERVAL '1 day',
                        flag_ativo = FALSE
                    WHERE {} = %s AND flag_ativo = TRUE
                """).format(
                    sql.Identifier(schema),
                    sql.Identifier(tabela),
                    sql.Identifier(self.chave_natural)
                )
                with get_cursor(conn) as cur:
                    cur.execute(update_query, (nk,))

            alterados['data_inicio'] = datetime.now().date()
            alterados['data_fim'] = None
            alterados['flag_ativo'] = True
            alterados['versao'] = alterados['versao'] + 1

        # Consolidar resultados
        df_result = pd.concat([novos, alterados], ignore_index=True)
        df_result.columns = df_result.columns.str.replace('_(novo|atual)$', '', regex=True)
        df_result = df_result.loc[:, ~df_result.columns.duplicated()]

        self.logger.info(f"SCD2: {len(novos)} novos, {len(alterados)} alterados")
        return df_result

    def executar(self) -> int:
        """
        Executa pipeline completo de transformação Silver.

        Fluxo:
        1. Extrair dados da Bronze
        2. Aplicar transformações e regras de negócio
        3. Validar qualidade dos dados
        4. Processar SCD Type 2 (se aplicável)
        5. Carregar na Silver

        Returns:
            0 se sucesso, 1 se erro
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info(f"{self.script_name}")
            self.logger.info(f"{self.tabela_origem} → {self.tabela_destino}")
            self.logger.info(f"Tipo de carga: {self.tipo_carga}")

            with get_connection() as conn:
                # Extrair
                self.logger.info("Extraindo dados da Bronze...")
                df_bronze = self.extrair_bronze(conn)
                log_dataframe_info(df_bronze, "Bronze")

                # Transformar
                self.logger.info("Aplicando transformações...")
                df_silver = self.aplicar_transformacoes(df_bronze)
                log_dataframe_info(df_silver, "Silver")

                # Validar
                self.logger.info("Validando qualidade...")
                valido, erros = self.validar_qualidade(df_silver)
                if not valido:
                    self.logger.error(f"Validação falhou: {', '.join(erros)}")
                    return 1

                # SCD Type 2
                if self.tipo_carga == 'scd2':
                    self.logger.info("Processando SCD Type 2...")
                    df_silver = self.processar_scd2(df_silver, conn)

                # Carregar
                self.logger.info(f"Carregando {len(df_silver)} registros...")
                schema, tabela = self.tabela_destino.split('.')

                with get_cursor(conn) as cur:
                    if self.tipo_carga == 'full':
                        truncate_query = sql.SQL("TRUNCATE TABLE {}.{} CASCADE").format(
                            sql.Identifier(schema), sql.Identifier(tabela)
                        )
                        cur.execute(truncate_query)

                    pk_col = self._obter_nome_pk()
                    cols = [c for c in df_silver.columns if c != pk_col]

                    colunas_sql = sql.SQL(', ').join(map(sql.Identifier, cols))
                    insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                        sql.Identifier(schema), sql.Identifier(tabela), colunas_sql
                    )

                    df_insert = df_silver[cols].copy()
                    df_insert = df_insert.replace({pd.NA: None, pd.NaT: None})
                    df_insert = df_insert.where(pd.notna(df_insert), None)

                    # Limpar valores antes da inserção
                    registros = []
                    for _, row in df_insert.iterrows():
                        registro_limpo = []
                        for val in row:
                            if val is None or pd.isna(val):
                                registro_limpo.append(None)
                            elif isinstance(val, float) and val == int(val):
                                registro_limpo.append(int(val))
                            else:
                                registro_limpo.append(val)
                        registros.append(registro_limpo)

                    execute_values(cur, insert_query, registros, page_size=1000)

                self.logger.success("Transformação concluída com sucesso")
                return 0

        except Exception as e:
            self.logger.error(f"Erro na execução: {e}", exc_info=True)
            return 1
