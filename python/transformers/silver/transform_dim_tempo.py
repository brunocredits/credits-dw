#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Transformador para dim_tempo - Dimensao de Tempo"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
from datetime import datetime
from transformers.base_transformer import BaseSilverTransformer


class TransformDimTempo(BaseSilverTransformer):
    """
    Transformador para dimens�o de tempo.

    Enriquece dados de bronze.data com atributos de calend�rio:
    - Nomes de meses e dias da semana
    - Flags de fim de semana e dia �til
    - Semanas do ano e do m�s
    - Formata��o de trimestres
    """

    def __init__(self):
        super().__init__(
            script_name='transform_dim_tempo.py',
            tabela_origem='bronze.data',
            tabela_destino='silver.dim_tempo',
            tipo_carga='full'
        )

    def extrair_bronze(self, conn) -> pd.DataFrame:
        """Extrai dados da tabela bronze.data"""
        query = """
        SELECT
            data_completa,
            ano,
            mes,
            dia,
            trimestre,
            semestre,
            bimestre,
            quarter
        FROM bronze.data
        ORDER BY data_completa
        """
        return pd.read_sql(query, conn)

    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica enriquecimento de calend�rio aos dados.

        Adiciona:
        - sk_data (surrogate key no formato YYYYMMDD)
        - Nomes de meses e dias da semana (portugu�s e ingl�s)
        - Semanas do ano e do m�s
        - Flags de fim de semana e dia �til
        - Informa��es de trimestre formatadas
        """
        self.logger.info("Enriquecendo dados de calend�rio...")

        # Converter data_completa para datetime
        df['data_completa'] = pd.to_datetime(df['data_completa'])

        # Surrogate Key: YYYYMMDD como integer
        df['sk_data'] = df['data_completa'].dt.strftime('%Y%m%d').astype(int)

        # Nomes de m�s em portugu�s (manter ingl�s da origem)
        meses_pt = {
            1: 'Janeiro', 2: 'Fevereiro', 3: 'Mar�o', 4: 'Abril',
            5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
            9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
        }
        meses_abrev = {
            1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
            7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
        }

        # Usar ingl�s para consist�ncia com dados existentes
        df['nome_mes'] = df['data_completa'].dt.strftime('%B')  # January, February...
        df['nome_mes_abrev'] = df['data_completa'].dt.strftime('%b')  # Jan, Feb...

        # Trimestre formatado (Q1, Q2, Q3, Q4)
        df['nome_trimestre'] = df['trimestre'].apply(lambda x: f'Q{x}')

        # Semana do ano (ISO 8601)
        df['semana_ano'] = df['data_completa'].dt.isocalendar().week.astype('int16')

        # Semana do m�s (aproxima��o: dia // 7 + 1)
        df['semana_mes'] = ((df['dia'] - 1) // 7 + 1).astype('int16')

        # Dia da semana (0=Domingo, 6=S�bado) - usar weekday ajustado
        df['dia_semana'] = ((df['data_completa'].dt.weekday + 1) % 7).astype('int16')

        # Nomes de dia da semana
        dias_semana = {
            0: 'Sunday', 1: 'Monday', 2: 'Tuesday', 3: 'Wednesday',
            4: 'Thursday', 5: 'Friday', 6: 'Saturday'
        }
        dias_semana_abrev = {
            0: 'Sun', 1: 'Mon', 2: 'Tue', 3: 'Wed',
            4: 'Thu', 5: 'Fri', 6: 'Sat'
        }

        df['nome_dia_semana'] = df['dia_semana'].map(dias_semana)
        df['nome_dia_semana_abrev'] = df['dia_semana'].map(dias_semana_abrev)

        # Flags
        df['flag_fim_semana'] = df['dia_semana'].isin([0, 6])  # Domingo ou S�bado
        df['flag_dia_util'] = ~df['flag_fim_semana']  # Simplifica��o: n�o considera feriados
        df['flag_feriado'] = False  # TODO: Implementar calend�rio de feriados brasileiro
        df['nome_feriado'] = None

        # Ano fiscal (simplifica��o: igual ao ano civil)
        df['ano_fiscal'] = df['ano']

        # Quarter fiscal (simplifica��o: igual ao trimestre)
        df['quarter_fiscal'] = df['nome_trimestre']

        # Data de carga
        df['data_carga'] = datetime.now()

        # Converter data_completa de volta para date
        df['data_completa'] = df['data_completa'].dt.date

        # Converter tipos
        df['ano'] = df['ano'].astype('int16')
        df['mes'] = df['mes'].astype('int16')
        df['dia'] = df['dia'].astype('int16')
        df['trimestre'] = df['trimestre'].astype('int16')
        df['semestre'] = df['semestre'].astype('int16')
        df['bimestre'] = df['bimestre'].astype('int16')

        # Selecionar colunas na ordem correta
        colunas_finais = [
            'sk_data', 'data_completa', 'ano', 'mes', 'dia',
            'trimestre', 'semestre', 'bimestre',
            'nome_mes', 'nome_mes_abrev', 'nome_trimestre',
            'semana_ano', 'semana_mes',
            'dia_semana', 'nome_dia_semana', 'nome_dia_semana_abrev',
            'flag_fim_semana', 'flag_dia_util', 'flag_feriado', 'nome_feriado',
            'ano_fiscal', 'quarter_fiscal',
            'data_carga'
        ]

        self.logger.success(f"Enriquecimento conclu�do: {len(df):,} datas processadas")

        return df[colunas_finais]

    def validar_qualidade(self, df: pd.DataFrame):
        """
        Valida qualidade dos dados da dimens�o tempo.

        Valida��es:
        - N�o pode haver datas duplicadas
        - Todos os campos obrigat�rios devem estar preenchidos
        - Valores de m�s devem estar entre 1-12
        - Valores de dia devem estar entre 1-31
        """
        erros = []
        warnings = []

        # Valida��es cr�ticas
        if df['data_completa'].isnull().any():
            erros.append("Datas nulas encontradas")

        if df.duplicated('data_completa').any():
            count = df.duplicated('data_completa').sum()
            erros.append(f"{count} datas duplicadas encontradas")

        if df.duplicated('sk_data').any():
            count = df.duplicated('sk_data').sum()
            erros.append(f"{count} sk_data duplicados encontrados")

        # Valida��es de range
        if (df['mes'] < 1).any() or (df['mes'] > 12).any():
            erros.append("Valores de m�s fora do range 1-12")

        if (df['dia'] < 1).any() or (df['dia'] > 31).any():
            erros.append("Valores de dia fora do range 1-31")

        if (df['trimestre'] < 1).any() or (df['trimestre'] > 4).any():
            erros.append("Valores de trimestre fora do range 1-4")

        # Valida��es informativas
        total_registros = len(df)
        dias_uteis = df['flag_dia_util'].sum()
        fins_semana = df['flag_fim_semana'].sum()

        self.logger.info(f"Estatisticas de qualidade:")
        self.logger.info(f"  • Total de datas: {total_registros:,}")
        self.logger.info(f"  • Dias uteis: {dias_uteis:,} ({dias_uteis/total_registros*100:.1f}%)")
        self.logger.info(f"  • Fins de semana: {fins_semana:,} ({fins_semana/total_registros*100:.1f}%)")
        self.logger.info(f"  • Range: {df['data_completa'].min()} a {df['data_completa'].max()}")

        return len(erros) == 0, erros


if __name__ == '__main__':
    import sys
    sys.exit(TransformDimTempo().executar())
