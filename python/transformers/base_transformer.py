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
    'bronze.contas', 'bronze.usuarios',
    'bronze.faturamentos', 'bronze.data'
}
TABELAS_SILVER_PERMITIDAS = {
    'silver.dim_cliente', 'silver.dim_usuario', 'silver.dim_data',
    'silver.dim_canal', 'silver.fato_faturamento'
}
TABELAS_CONTROLE_PERMITIDAS = {
    'auditoria.historico_execucao', 'auditoria.controle_silver'
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
        tabelas_origem = [t.strip() for t in tabela_origem.split(',')]
        for t_origem in tabelas_origem:
            if t_origem not in TABELAS_BRONZE_PERMITIDAS:
                raise ValueError(f"Tabela origem não permitida: {t_origem}")

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
        Obtém o nome da coluna de chave primária (surrogate key)
        seguindo o padrão 'entidade_sk'.

        Returns:
            Nome da coluna PK (ex: 'cliente_sk', 'faturamento_sk')
        """
        tabela_nome = self.tabela_destino.split('.')[-1]
        # Remove prefixos para obter a entidade base
        # Ex: 'dim_cliente' -> 'cliente', 'fato_faturamento' -> 'faturamento'
        entidade = tabela_nome.replace('dim_', '').replace('fato_', '')
        return f"{entidade}_sk"

    def processar_scd2(self, df_novo: pd.DataFrame, conn) -> pd.DataFrame:
        """
        Processa Slowly Changing Dimension Type 2.

        Compara registros novos com registros atuais na dimensão,
        fecha registros alterados e cria novas versões.

        Args:
            df_novo: DataFrame com novos dados da fonte.
            conn: Conexão com o banco de dados.

        Returns:
            DataFrame com registros a serem inseridos (novos e novas versões de alterados).
        """
        if not self.chave_natural:
            raise ValueError("Chave natural é obrigatória para o tipo de carga 'scd2'")

        pk_col = self._obter_nome_pk()
        data_carga = datetime.now().date()
        data_fim_antigos = data_carga - pd.Timedelta(days=1)

        schema, tabela = self.tabela_destino.split('.')
        query_str = f"SELECT * FROM {schema}.{tabela} WHERE flag_ativo = TRUE"
        df_atual = pd.read_sql(query_str, conn)

        self.logger.info(f"[SILVER][INFO] {self.script_name}: Buscando registros ativos em {self.tabela_destino}. Encontrados: {len(df_atual)}.")

        if df_atual.empty:
            self.logger.info(f"[SILVER][INFO] {self.script_name}: Primeira carga. Todos os {len(df_novo)} registros são novos.")
            df_novo.loc[:, 'data_inicio'] = data_carga
            df_novo.loc[:, 'data_fim'] = None
            df_novo.loc[:, 'flag_ativo'] = True
            df_novo.loc[:, 'versao'] = 1
            return df_novo

        # Merge para encontrar registros novos e alterados
        df_merge = df_novo.merge(
            df_atual[[self.chave_natural, 'hash_registro', pk_col, 'versao']],
            on=self.chave_natural,
            how='left',
            suffixes=('_novo', '_atual')
        )

        # 1. Identificar registros novos (existem na fonte, mas não na Silver)
        novos = df_merge[df_merge[pk_col].isna()].copy()
        if not novos.empty:
            novos.loc[:, 'data_inicio'] = data_carga
            novos.loc[:, 'data_fim'] = None
            novos.loc[:, 'flag_ativo'] = True
            novos.loc[:, 'versao'] = 1

        # 2. Identificar registros alterados (existem em ambos, mas o hash mudou)
        alterados = df_merge[
            (df_merge[pk_col].notna()) &
            (df_merge['hash_registro_novo'] != df_merge['hash_registro_atual'])
        ].copy()

        # 3. Fechar registros antigos que foram alterados
        if not alterados.empty:
            self.logger.info(f"[SILVER][INFO] {self.script_name}: {len(alterados)} registros alterados detectados.")
            chaves_para_fechar = tuple(alterados[self.chave_natural].unique())

            update_query = sql.SQL("""
                UPDATE {tabela_destino}
                SET data_fim = %s, flag_ativo = FALSE
                WHERE {chave_natural} IN %s AND flag_ativo = TRUE
            """).format(
                tabela_destino=sql.Identifier(schema, tabela),
                chave_natural=sql.Identifier(self.chave_natural)
            )
            with get_cursor(conn) as cur:
                cur.execute(update_query, (data_fim_antigos, chaves_para_fechar))
                self.logger.info(f"[SILVER][INFO] {self.script_name}: {cur.rowcount} registros antigos foram versionados (fechados).")

            # Preparar as novas versões dos registros alterados para inserção
            alterados.loc[:, 'data_inicio'] = data_carga
            alterados.loc[:, 'data_fim'] = None
            alterados.loc[:, 'flag_ativo'] = True
            # Converter versao para int antes de incrementar (vem com sufixo _atual do merge)
            if 'versao_atual' in alterados.columns:
                alterados.loc[:, 'versao'] = alterados['versao_atual'].astype(int) + 1
            else:
                # Fallback: versão 2 se não houver versao_atual
                alterados.loc[:, 'versao'] = 2

        # 4. Consolidar registros a serem inseridos (novos + novas versões dos alterados)
        df_final = pd.concat([novos, alterados], ignore_index=True)
        # Limpar colunas de sufixo (_novo, _atual) do merge
        df_final.columns = df_final.columns.str.replace(r'_(novo|atual)$', '', regex=True)
        # Remover colunas duplicadas que podem ter sido geradas pelo merge
        df_final = df_final.loc[:, ~df_final.columns.duplicated()]

        self.logger.info(f"[SILVER][INFO] {self.script_name}: SCD2 finalizado. {len(novos)} registros novos e {len(alterados)} alterados para inserção.")
        return df_final

    def executar(self) -> int:
        """
        Executa o pipeline completo de transformação para a camada Silver.
        """
        self.logger.info(f"[SILVER][INICIO] ----------------------------------------------------------------")
        self.logger.info(f"[SILVER][INICIO] {self.script_name}: Iniciando transformação.")
        self.logger.info(f"[SILVER][INICIO] Origem: {self.tabela_origem} | Destino: {self.tabela_destino} | Carga: {self.tipo_carga}")

        try:
            with get_connection() as conn:
                # 1. Extrair
                self.logger.info(f"[SILVER][INFO] {self.script_name}: Extraindo dados da Bronze...")
                df_bronze = self.extrair_bronze(conn)
                log_dataframe_info(df_bronze, "Bronze", self.logger)

                if df_bronze.empty:
                    self.logger.warning(f"[SILVER][AVISO] {self.script_name}: A tabela de origem na Bronze está vazia. Nenhuma ação a ser tomada.")
                    return 0

                # 2. Transformar
                self.logger.info(f"[SILVER][INFO] {self.script_name}: Aplicando transformações de negócio...")
                df_silver = self.aplicar_transformacoes(df_bronze)
                log_dataframe_info(df_silver, "Silver (pré-validação)", self.logger)

                # 3. Validar
                self.logger.info(f"[SILVER][INFO] {self.script_name}: Validando qualidade dos dados transformados...")
                valido, erros = self.validar_qualidade(df_silver)
                if not valido:
                    self.logger.error(f"[SILVER][FALHA] {self.script_name}: Validação de qualidade falhou. Erros: {', '.join(erros)}")
                    return 1
                self.logger.success(f"[SILVER][INFO] {self.script_name}: Validação de qualidade concluída com sucesso.")

                # 4. Processar SCD Type 2
                if self.tipo_carga == 'scd2':
                    df_a_inserir = self.processar_scd2(df_silver, conn)
                else:
                    df_a_inserir = df_silver

                # 5. Carregar
                if df_a_inserir.empty:
                    self.logger.info(f"[SILVER][INFO] {self.script_name}: Nenhum registro novo ou alterado para carregar.")
                    self.logger.success(f"[SILVER][SUCESSO] {self.script_name}: Transformação concluída sem necessidade de carga.")
                    return 0

                self.logger.info(f"[SILVER][INFO] {self.script_name}: Carregando {len(df_a_inserir)} registros em {self.tabela_destino}.")
                schema, tabela = self.tabela_destino.split('.')

                with get_cursor(conn) as cur:
                    if self.tipo_carga == 'full':
                        self.logger.info(f"[SILVER][INFO] {self.script_name}: Executando TRUNCATE em {self.tabela_destino} (carga full).")
                        truncate_query = sql.SQL("TRUNCATE TABLE {}.{} CASCADE").format(
                            sql.Identifier(schema), sql.Identifier(tabela)
                        )
                        cur.execute(truncate_query)

                    pk_col = self._obter_nome_pk()
                    cols = [c for c in df_a_inserir.columns if c != pk_col]
                    colunas_sql = sql.SQL(', ').join(map(sql.Identifier, cols))
                    insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                        sql.Identifier(schema), sql.Identifier(tabela), colunas_sql
                    )

                    # Preparar dados para inserção, garantindo que nulos e tipos estão corretos
                    df_insert = df_a_inserir[cols].copy()
                    df_insert = df_insert.replace({pd.NA: None, pd.NaT: None}).where(pd.notna(df_insert), None)

                    registros = [tuple(row) for row in df_insert.itertuples(index=False)]
                    
                    execute_values(cur, insert_query, registros, page_size=1000)
                    self.logger.success(f"[SILVER][SUCESSO] {self.script_name}: {cur.rowcount} registros carregados em {self.tabela_destino}.")

                self.logger.success(f"[SILVER][SUCESSO] {self.script_name}: Transformação concluída com sucesso.")
                return 0

        except Exception as e:
            self.logger.error(f"[SILVER][ERRO] {self.script_name}: Erro inesperado na execução: {e}", exc_info=True)
            return 1
