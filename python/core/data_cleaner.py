import pandas as pd
import numpy as np

class DataCleaner:
    """
    Responsável pela limpeza e padronização de dados (Numéricos e Datas).
    Utiliza operações vetorizadas do Pandas para alta performance.
    """

    @staticmethod
    def clean_numeric(series: pd.Series) -> pd.Series:
        """
        Limpa colunas numéricas:
        - Trata '-' como 0
        - Converte formato BR (1.000,00) para US (1000.00)
        - Retorna série numérica (float), com NaN para erros
        """
        # 1. Pre-clean strings (strip)
        s = series.astype(str).str.strip()
        
        # 2. Handle '-' as '0'
        s = s.replace('-', '0')
        
        # 3. Handle Brazilian format: Remove dots, replace comma with dot
        # Regex=False is faster for simple replacements
        s = s.str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        
        # 4. Convert to numeric (coerce errors to NaN)
        return pd.to_numeric(s, errors='coerce')

    @staticmethod
    def clean_date(series: pd.Series) -> pd.Series:
        """
        Limpa colunas de data:
        - Tenta converter formato DD/MM/YYYY
        - Retorna série datetime, com NaT para erros
        """
        # dayfirst=True é essencial para DD/MM/YYYY
        return pd.to_datetime(series, dayfirst=True, errors='coerce')

    @staticmethod
    def identify_errors(original_series: pd.Series, cleaned_series: pd.Series) -> pd.Series:
        """
        Identifica índices onde a conversão falhou.
        Erro se: Original não era vazio/nulo E Resultado é NaN/NaT
        """
        non_empty = original_series.notna() & (original_series.astype(str).str.strip() != '')
        failed = cleaned_series.isna() & non_empty
        return failed
