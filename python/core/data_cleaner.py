"""
Este módulo, `DataCleaner`, é responsável pela limpeza e padronização de dados,
focando na conversão de formatos brasileiros para um padrão universalmente
reconhecido por bancos de dados e sistemas analíticos.
"""

import pandas as pd
import numpy as np

class DataCleaner:
    """
    Classe utilitária que encapsula a lógica para limpeza de dados,
    especificamente para colunas numéricas e de data.
    """

    @staticmethod
    def clean_numeric(series: pd.Series) -> pd.Series:
        """
        Limpa e converte uma série de dados para o tipo numérico.

        A função realiza as seguintes operações:
        - Remove espaços em branco no início e no fim.
        - Substitui hífens ('-') por zero.
        - Converte o formato numérico brasileiro (ex: "1.000,00") para o formato
          padrão americano (ex: "1000.00").
        - Converte a série para o tipo numérico, tratando valores inválidos como NaN.

        Args:
            series (pd.Series): A série de dados a ser limpa.

        Returns:
            pd.Series: A série com dados numéricos limpos.
        """
        s = series.astype(str).str.strip()
        s = s.replace('-', '0')
        s = s.str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        return pd.to_numeric(s, errors='coerce')

    @staticmethod
    def clean_date(series: pd.Series) -> pd.Series:
        """
        Converte uma série de dados para o tipo data, assumindo o formato DD/MM/YYYY.

        Valores que não seguem o formato esperado ou são inválidos são
        convertidos para NaT (Not a Time).

        Args:
            series (pd.Series): A série de dados a ser convertida.

        Returns:
            pd.Series: A série com os dados no tipo datetime.
        """
        return pd.to_datetime(series, dayfirst=True, errors='coerce')

    @staticmethod
    def identify_errors(original_series: pd.Series, cleaned_series: pd.Series) -> pd.Series:
        """
        Identifica as linhas onde a conversão de tipo de dado falhou.

        A função compara a série original com a série limpa e retorna uma
        máscara booleana que é `True` para as posições onde a conversão
        resultou em um valor nulo (NaN ou NaT), mas o valor original não era
        nulo ou vazio.

        Args:
            original_series (pd.Series): A série de dados antes da limpeza.
            cleaned_series (pd.Series): A série de dados após a limpeza.

        Returns:
            pd.Series: Uma máscara booleana indicando as linhas com erro.
        """
        non_empty = original_series.notna() & (original_series.astype(str).str.strip() != '')
        failed = cleaned_series.isna()
        return failed & non_empty
