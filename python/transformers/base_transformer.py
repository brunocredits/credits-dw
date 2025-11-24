"""Classe base para transformações Silver"""
import pandas as pd
import hashlib
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple
from datetime import datetime
from psycopg2.extras import execute_values
from psycopg2 import sql

from utils.logger import setup_logger, log_dataframe_info
from utils.db_connection import get_connection, get_cursor

# Whitelist de tabelas permitidas
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
    """Template Method pattern para transformações Silver"""

    def __init__(self, script_name: str, tabela_origem: str,
                 tabela_destino: str, tipo_carga: str = 'full',
                 chave_natural: str = None):
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
        """Extrai dados da Bronze"""
        pass

    @abstractmethod
    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica regras de negócio"""
        pass

    @abstractmethod
    def validar_qualidade(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Valida qualidade. Retorna (sucesso: bool, erros: List[str])"""
        pass

    def calcular_hash_registro(self, row: pd.Series, colunas: List[str]) -> str:
        """Hash MD5 para detectar mudanças"""
        valores = '|'.join([str(row[col]) for col in colunas if col in row.index])
        return hashlib.md5(valores.encode()).hexdigest()

    def processar_scd2(self, df_novo: pd.DataFrame, conn) -> pd.DataFrame:
        """Processa Slowly Changing Dimension Type 2"""
        if not self.chave_natural:
            raise ValueError("Chave natural obrigatória para SCD2")

        tabela_nome = self.tabela_destino.split('.')[-1]
        pk_col = f"sk_{tabela_nome.replace('dim_', '')}"

        # Buscar registros atuais
        schema, tabela = self.tabela_destino.split('.')
        query = sql.SQL("SELECT * FROM {}.{} WHERE flag_ativo = TRUE").format(
            sql.Identifier(schema), sql.Identifier(tabela)
        )
        df_atual = pd.read_sql(query, conn)

        if df_atual.empty:
            # Primeira carga
            df_novo['data_inicio'] = datetime.now().date()
            df_novo['data_fim'] = None
            df_novo['flag_ativo'] = True
            df_novo['versao'] = 1
            return df_novo

        # Identificar mudanças
        df_merge = df_novo.merge(
            df_atual[[self.chave_natural, 'hash_registro', pk_col]],
            on=self.chave_natural, how='left', suffixes=('_novo', '_atual')
        )

        novos = df_merge[df_merge[pk_col].isna()].copy()
        alterados = df_merge[
            (df_merge[pk_col].notna()) &
            (df_merge['hash_registro_novo'] != df_merge['hash_registro_atual'])
        ].copy()

        # Fechar registros antigos
        if not alterados.empty:
            for nk in alterados[self.chave_natural].unique():
                update_query = sql.SQL("""
                    UPDATE {}.{} SET data_fim = CURRENT_DATE - INTERVAL '1 day',
                                     flag_ativo = FALSE
                    WHERE {} = %s AND flag_ativo = TRUE
                """).format(
                    sql.Identifier(schema), sql.Identifier(tabela),
                    sql.Identifier(self.chave_natural)
                )
                with get_cursor(conn) as cur:
                    cur.execute(update_query, (nk,))

            alterados['data_inicio'] = datetime.now().date()
            alterados['data_fim'] = None
            alterados['flag_ativo'] = True
            alterados['versao'] = alterados['versao'] + 1

        # Concatenar e limpar
        df_result = pd.concat([novos, alterados], ignore_index=True)
        df_result.columns = df_result.columns.str.replace('_(novo|atual)$', '', regex=True)
        df_result = df_result.loc[:, ~df_result.columns.duplicated()]

        return df_result

    def executar(self) -> int:
        """Executa pipeline Silver"""
        try:
            self.logger.info("=" * 80)
            self.logger.info(f"🚀 {self.script_name}")
            self.logger.info(f"📊 {self.tabela_origem} → {self.tabela_destino}")
            self.logger.info(f"⚙️ Carga: {self.tipo_carga}")

            with get_connection() as conn:
                # Extrair
                self.logger.info("📥 Extraindo Bronze...")
                df_bronze = self.extrair_bronze(conn)
                log_dataframe_info(df_bronze, "Bronze")

                # Transformar
                self.logger.info("🔄 Transformando...")
                df_silver = self.aplicar_transformacoes(df_bronze)
                log_dataframe_info(df_silver, "Silver")

                # Validar
                self.logger.info("✅ Validando qualidade...")
                valido, erros = self.validar_qualidade(df_silver)
                if not valido:
                    self.logger.error(f"❌ Validação falhou: {', '.join(erros)}")
                    return 1

                # SCD2 se necessário
                if self.tipo_carga == 'scd2':
                    df_silver = self.processar_scd2(df_silver, conn)

                # Carregar
                self.logger.info(f"💾 Carregando {len(df_silver)} registros...")
                schema, tabela = self.tabela_destino.split('.')

                with get_cursor(conn) as cur:
                    if self.tipo_carga == 'full':
                        truncate_query = sql.SQL("TRUNCATE TABLE {}.{} CASCADE").format(
                            sql.Identifier(schema), sql.Identifier(tabela)
                        )
                        cur.execute(truncate_query)

                    # Remover PK autoincrement
                    tabela_nome = self.tabela_destino.split('.')[-1]
                    pk_col = f"sk_{tabela_nome.replace('fact_', '').replace('dim_', '')}"
                    cols = [c for c in df_silver.columns if c != pk_col]

                    # Batch insert com execute_values
                    colunas_sql = sql.SQL(', ').join(map(sql.Identifier, cols))
                    insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                        sql.Identifier(schema), sql.Identifier(tabela), colunas_sql
                    )

                    # Converter NaN/NaT para None e garantir tipos corretos
                    df_insert = df_silver[cols].copy()
                    df_insert = df_insert.replace({pd.NA: None, pd.NaT: None})
                    df_insert = df_insert.where(pd.notna(df_insert), None)

                    # Converter para lista e limpar valores
                    registros = []
                    for _, row in df_insert.iterrows():
                        registro_limpo = []
                        for val in row:
                            # None permanece None
                            if val is None or pd.isna(val):
                                registro_limpo.append(None)
                            # Floats que são inteiros (1.0, 2.0) converter para int
                            elif isinstance(val, float) and val == int(val):
                                registro_limpo.append(int(val))
                            else:
                                registro_limpo.append(val)
                        registros.append(registro_limpo)

                    execute_values(cur, insert_query, registros, page_size=1000)

                self.logger.success("✅ Transformação concluída!")
                return 0

        except Exception as e:
            self.logger.error(f"❌ Erro: {e}")
            return 1
