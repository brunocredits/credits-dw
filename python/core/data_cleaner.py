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
        Converte uma série de dados para o tipo data, suportando múltiplos formatos:
        - DD/MM/YYYY (padrão brasileiro com ano de 4 dígitos)
        - DD/MM/YY (padrão brasileiro com ano de 2 dígitos)
        - MMM/YYYY (mês abreviado/ano, ex: "out/2025" → "01/10/2025")
        
        Para o formato MMM/YYYY, assume-se o primeiro dia do mês.
        Para anos de 2 dígitos, assume-se 20XX para valores 00-49 e 19XX para 50-99.
        Valores que não seguem nenhum formato esperado ou são inválidos são
        convertidos para NaT (Not a Time).

        Args:
            series (pd.Series): A série de dados a ser convertida.

        Returns:
            pd.Series: A série com os dados no tipo datetime.
        """
        # Mapeamento de meses em português (abreviados) para números
        meses_pt = {
            'jan': '01', 'fev': '02', 'mar': '03', 'abr': '04',
            'mai': '05', 'jun': '06', 'jul': '07', 'ago': '08',
            'set': '09', 'out': '10', 'nov': '11', 'dez': '12'
        }
        
        def convert_value(value):
            """Converte diferentes formatos de data para um formato padrão"""
            if pd.isna(value) or not isinstance(value, str):
                return value
                
            value = value.strip().lower()
            
            # Formato "mês/ano" (ex: "out/2025" → "01/10/2025")
            for mes_abrev, mes_num in meses_pt.items():
                if value.startswith(mes_abrev + '/'):
                    ano = value.split('/')[1]
                    return f'01/{mes_num}/{ano}'
            
            return value
        
        # Aplica a conversão de formatos especiais
        converted_series = series.apply(convert_value)
        
        # Tenta converter com múltiplos formatos
        # Primeiro tenta com formatos explícitos para evitar ambiguidade
        result = pd.Series([pd.NaT] * len(converted_series), index=converted_series.index)
        
        # Formato DD/MM/YYYY (4 dígitos no ano)
        mask_not_parsed = result.isna()
        result[mask_not_parsed] = pd.to_datetime(
            converted_series[mask_not_parsed], 
            format='%d/%m/%Y', 
            errors='coerce'
        )
        
        # Formato DD/MM/YY (2 dígitos no ano) - assume 20YY para 00-49, 19YY para 50-99
        mask_not_parsed = result.isna()
        result[mask_not_parsed] = pd.to_datetime(
            converted_series[mask_not_parsed], 
            format='%d/%m/%y', 
            errors='coerce'
        )
        
        # Fallback: deixa o pandas inferir o formato (para casos edge)
        mask_not_parsed = result.isna()
        result[mask_not_parsed] = pd.to_datetime(
            converted_series[mask_not_parsed], 
            dayfirst=True, 
            errors='coerce'
        )
        
        return result



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
