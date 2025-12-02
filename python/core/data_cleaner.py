"""
DataCleaner - Limpeza e padronização de dados
Responsável por converter formatos brasileiros e validar tipos de dados
"""

import pandas as pd
import numpy as np

class DataCleaner:
    """Classe utilitária para limpeza de dados numéricos e datas"""

    @staticmethod
    def clean_numeric(series: pd.Series) -> pd.Series:
        """
        Limpa valores numéricos:
        - Converte hífen para zero
        - Converte formato BR (1.000,00) para US (1000.00)
        - Retorna NaN para valores inválidos
        """
        s = series.astype(str).str.strip()
        s = s.replace('-', '0')
        s = s.str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        return pd.to_numeric(s, errors='coerce')

    @staticmethod
    def clean_date(series: pd.Series) -> pd.Series:
        """
        Limpa datas no formato DD/MM/YYYY
        Retorna NaT para valores inválidos
        """
        return pd.to_datetime(series, dayfirst=True, errors='coerce')

    @staticmethod
    def identify_errors(original_series: pd.Series, cleaned_series: pd.Series) -> pd.Series:
        """
        Identifica linhas onde a conversão falhou
        Retorna máscara booleana (True = erro)
        """
        non_empty = original_series.notna() & (original_series.astype(str).str.strip() != '')
        failed = cleaned_series.isna()
        return failed & non_empty
