#!/usr/bin/env python3
"""
Script: ingest_faturamento.py
Descrição: Ingestão de dados de faturamento do OneDrive CSV para a camada Bronze
Frequência: Diária
Versão: 1.0
"""

import sys
from pathlib import Path
from typing import Dict, List

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor

class IngestFaturamento(BaseCSVIngestor):
    """
    Ingestor para os dados de faturamento.
    """

    def __init__(self):
        super().__init__(
            script_name='ingest_faturamento.py',
            tabela_destino='bronze.faturamento',
            arquivo_nome='faturamento.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """
        Retorna o mapeamento de colunas do CSV para a tabela Bronze.
        """
        return {
            'ID_Faturamento': 'id_faturamento',
            'ID_Conta': 'id_conta',
            'Data': 'data',
            'Receita': 'receita',
            'Moeda': 'moeda'
        }

    def get_bronze_columns(self) -> List[str]:
        """
        Retorna a lista de colunas da tabela Bronze.
        """
        return [
            'id_faturamento',
            'id_conta',
            'data',
            'receita',
            'moeda'
        ]

if __name__ == '__main__':
    ingestor = IngestFaturamento()
    sys.exit(ingestor.executar())
