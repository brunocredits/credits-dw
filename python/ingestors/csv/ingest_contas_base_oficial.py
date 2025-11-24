#!/usr/bin/env python3
"""Ingestão de contas base oficial para Bronze"""
import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor

class IngestContasBaseOficial(BaseCSVIngestor):
    """Ingestor para dados de contas da base oficial"""

    def __init__(self):
        super().__init__(
            script_name='ingest_contas_base_oficial.py',
            tabela_destino='bronze.contas_base_oficial',
            arquivo_nome='contas_base_oficial.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        return {
            'CNPJ/CPF PK': 'cnpj_cpf',
            'Tipo': 'tipo',
            'Status': 'status',
            'Status de Qualificação da conta': 'status_qualificação_da_conta',
            'Data de criação': 'data_criacao',
            'Grupo': 'grupo',
            'Razão Social': 'razao_social',
            'Responsável da Conta': 'responsavel_conta',
            'Financeiro': 'financeiro',
            'Corte': 'corte',
            'Faixa': 'faixa'
        }

    def get_bronze_columns(self) -> List[str]:
        return [
            'cnpj_cpf', 'tipo', 'status', 'status_qualificação_da_conta',
            'data_criacao', 'grupo', 'razao_social', 'responsavel_conta',
            'financeiro', 'corte', 'faixa'
        ]

if __name__ == '__main__':
    sys.exit(IngestContasBaseOficial().executar())
