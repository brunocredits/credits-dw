"""
Módulo: transform_dim_data.py
Descrição: Transformador para a dimensão de data (dim_data).
Versão: 2.1 - Refatorado para usar nomes padronizados.

Abordagem superior: Gera a dimensão dinamicamente via SQL,
eliminando a necessidade de um arquivo CSV estático e frágil.
"""

import sys
import time
import pandas as pd
from typing import List, Tuple
from pathlib import Path
from psycopg2 import sql

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from transformers.base_transformer import BaseSilverTransformer
from utils.db_connection import get_cursor, get_db_connection
from utils.logger import log_execution_summary


class TransformDimData(BaseSilverTransformer):
    """
    Transformador para a dimensão de data.

    Gera a dimensão dinamicamente com base no intervalo de datas
    presente na camada Bronze.
    """

    def __init__(self):
        """Inicializa o transformador com seus parâmetros específicos."""
        super().__init__(
            script_name='transform_dim_data.py',
            tabela_origem='bronze.faturamentos,bronze.contas',
            tabela_destino='silver.dim_data',
            tipo_carga='full'
        )

    # Métodos abstratos da classe base (não usados nesta implementação de SQL puro)
    def extrair_bronze(self, conn) -> pd.DataFrame:
        """Não aplicável para este transformador."""
        pass

    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Não aplicável para este transformador."""
        pass

    def validar_qualidade(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Não aplicável para este transformador."""
        return True, []

    def executar_sql(self, conn) -> int:
        """
        Executa a query SQL para gerar e popular a dimensão de data.
        Retorna o número de linhas inseridas.
        """
        query = """
        WITH RECURSIVE date_series AS (
            -- 1. Descobre o intervalo de datas necessário a partir dos dados da Bronze
            WITH date_ranges AS (
                SELECT MIN(data) as min_date, MAX(data) as max_date FROM bronze.faturamentos
                UNION ALL
                SELECT MIN(data_criacao), MAX(data_criacao) FROM bronze.contas
            ),
            min_max_dates AS (
                SELECT MIN(min_date) as start_date, MAX(max_date) as end_date
                FROM date_ranges
                WHERE min_date IS NOT NULL AND max_date IS NOT NULL
            )
            SELECT
                (SELECT start_date FROM min_max_dates) as calendar_date
            UNION ALL
            SELECT
                (calendar_date + INTERVAL '1 day')::date
            FROM date_series
            WHERE calendar_date < (SELECT end_date FROM min_max_dates)
        ),

        -- 2. Gera todos os atributos da dimensão de tempo a partir da série de datas
        full_calendar AS (
            SELECT
                ds.calendar_date,
                -- Componentes da Data (data_sk será auto-gerado pelo SERIAL)
                EXTRACT(YEAR FROM ds.calendar_date)::smallint AS ano,
                EXTRACT(MONTH FROM ds.calendar_date)::smallint AS mes,
                EXTRACT(DAY FROM ds.calendar_date)::smallint AS dia,

                -- Períodos
                EXTRACT(QUARTER FROM ds.calendar_date)::smallint AS trimestre,
                (CASE WHEN EXTRACT(MONTH FROM ds.calendar_date) <= 6 THEN 1 ELSE 2 END)::smallint AS semestre,
                ((EXTRACT(MONTH FROM ds.calendar_date) - 1) / 2 + 1)::smallint AS bimestre,
                'Q' || EXTRACT(QUARTER FROM ds.calendar_date)::text AS nome_trimestre,

                -- Nomes (em português)
                TRIM(TO_CHAR(ds.calendar_date, 'TMMonth')) AS nome_mes,
                TRIM(TO_CHAR(ds.calendar_date, 'TMMon')) AS nome_mes_abrev,
                TRIM(TO_CHAR(ds.calendar_date, 'TMDay')) AS nome_dia_semana,
                TRIM(TO_CHAR(ds.calendar_date, 'TMDy')) AS nome_dia_semana_abrev,

                -- Semana
                EXTRACT(ISODOW FROM ds.calendar_date)::smallint as dia_semana, -- 1=Segunda, 7=Domingo
                EXTRACT(WEEK FROM ds.calendar_date)::smallint AS semana_ano,
                ((EXTRACT(DAY FROM ds.calendar_date) - 1) / 7 + 1)::smallint AS semana_mes,

                -- Flags
                (EXTRACT(ISODOW FROM ds.calendar_date) IN (6, 7)) as flag_fim_semana,
                (EXTRACT(ISODOW FROM ds.calendar_date) NOT IN (6, 7)) as flag_dia_util -- Simplificado, não considera feriados

            FROM date_series ds
            WHERE ds.calendar_date IS NOT NULL
        )

        -- 3. Insere os dados na tabela Silver (data_sk é auto-gerado pelo SERIAL)
        INSERT INTO silver.dim_data (
            data_completa, ano, mes, dia, trimestre, semestre, bimestre,
            nome_trimestre, nome_mes, nome_mes_abrev, nome_dia_semana, nome_dia_semana_abrev,
            dia_semana, semana_ano, semana_mes, flag_fim_semana, flag_dia_util
        )
        SELECT
            calendar_date, ano, mes, dia, trimestre, semestre, bimestre,
            nome_trimestre, nome_mes, nome_mes_abrev, nome_dia_semana, nome_dia_semana_abrev,
            dia_semana, semana_ano, semana_mes, flag_fim_semana, flag_dia_util
        FROM full_calendar
        ON CONFLICT (data_completa) DO NOTHING;
        """
        with get_cursor(conn) as cur:
            self.logger.info(f"[SILVER][INFO] {self.script_name}: Truncando tabela {self.tabela_destino}.")
            cur.execute(sql.SQL("TRUNCATE TABLE {schema}.{tabela} CASCADE").format(
                schema=sql.Identifier('silver'),
                tabela=sql.Identifier('dim_data')
            ))
            self.logger.info(f"[SILVER][INFO] {self.script_name}: Gerando e inserindo dados dinamicamente em {self.tabela_destino}.")
            cur.execute(query)
            inserted_count = cur.rowcount
            self.logger.success(f"[SILVER][INFO] {self.script_name}: {inserted_count} registros inseridos.")
            return inserted_count

    def executar(self) -> int:
        """Sobrescreve o método base para usar execução de SQL puro."""
        conn = None
        start_time = time.time()
        try:
            self.logger.info(f"[SILVER][INICIO] {self.script_name}: Iniciando transformação.")
            conn = get_db_connection()
            self.logger.success(f"[SILVER][INFO] {self.script_name}: Conectado ao banco de dados.")

            linhas_inseridas = self.executar_sql(conn)

            conn.commit()
            self.logger.success(f"[SILVER][INFO] {self.script_name}: Transação confirmada (COMMIT).")

            duracao = time.time() - start_time
            log_execution_summary(
                script_name=self.script_name, status='sucesso',
                linhas_processadas=linhas_inseridas, linhas_inseridas=linhas_inseridas,
                duracao_segundos=duracao
            )
            return 0
        except Exception as e:
            self.logger.error(f"[SILVER][ERRO] {self.script_name}: Erro inesperado: {e}", exc_info=True)
            if conn:
                conn.rollback()
            return 1
        finally:
            if conn:
                conn.close()

if __name__ == '__main__':
    # Permite a execução do script diretamente
    sys.exit(TransformDimData().executar())
