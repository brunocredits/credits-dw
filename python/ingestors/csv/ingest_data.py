#!/usr/bin/env python3
"""
Script: ingest_data.py
Descrição: Ingestão de dados de data do OneDrive CSV para a camada Bronze
Frequência: Diária
Versão: 1.0
"""

import sys
from pathlib import Path
import pandas as pd
from typing import Dict, List

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor

class IngestData(BaseCSVIngestor):
    """
    Ingestor para os dados de data.
    """

    def __init__(self):
        super().__init__(
            script_name='ingest_data.py',
            tabela_destino='bronze.data',
            arquivo_nome='data.csv',
            input_subdir='onedrive',
            format_dates=False
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """
        Retorna o mapeamento de colunas do CSV para a tabela Bronze.
        """
        return {
            'semestre': 'semestre',
            'trimestre': 'trimestre',
            'quarter': 'quarter',
            'bimestre': 'bimestre',
            'mes': 'mes',
            'dia': 'dia',
            'ano': 'ano'
        }

    def get_bronze_columns(self) -> List[str]:
        """
        Retorna a lista de colunas da tabela Bronze.
        """
        return [
            'data_completa',
            'semestre',
            'trimestre',
            'quarter',
            'bimestre',
            'mes',
            'dia',
            'ano'
        ]

    def transformar_para_bronze(self, df):
        """
        Sobrescreve o método base para criar a coluna data_completa.
        """
        df_for_datetime = df.rename(columns={'ano': 'year', 'mes': 'month', 'dia': 'day'})
        df['data_completa'] = pd.to_datetime(df_for_datetime[['year', 'month', 'day']])
        return super().transformar_para_bronze(df)

if __name__ == '__main__':
    ingestor = IngestData()
    sys.exit(ingestor.executar())
