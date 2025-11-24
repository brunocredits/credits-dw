#!/usr/bin/env python3
"""Ingestão de faturamento para Bronze"""
import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor

class IngestFaturamento(BaseCSVIngestor):
    """Ingestor para dados de faturamento"""

    def __init__(self):
        super().__init__(
            script_name='ingest_faturamento.py',
            tabela_destino='bronze.faturamento',
            arquivo_nome='faturamento.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        return {
            'Data': 'data',
            'Receita': 'receita',
            'Moeda': 'moeda',
            'CNPJ Cliente': 'cnpj_cliente',
            'Email Usuario': 'email_usuario'
        }

    def get_bronze_columns(self) -> List[str]:
        return ['data', 'receita', 'moeda', 'cnpj_cliente', 'email_usuario']

    def get_date_columns(self) -> List[str]:
        return ['data']

if __name__ == '__main__':
    sys.exit(IngestFaturamento().executar())
