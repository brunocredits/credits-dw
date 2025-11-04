#!/usr/bin/env python3
"""
Script: ingest_data.py
Descrição: Ingestão de dados de data do OneDrive CSV para a camada Bronze
Frequência: Diária
Versão: 1.0
"""

import sys
from pathlib import Path
from typing import Dict, List

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

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
            input_subdir='onedrive'
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
            'semestre',
            'trimestre',
            'quarter',
            'bimestre',
            'mes',
            'dia',
            'ano'
        ]

if __name__ == '__main__':
    ingestor = IngestData()
    sys.exit(ingestor.executar())
