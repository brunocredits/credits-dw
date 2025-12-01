#!/usr/bin/env python3
"""
Módulo: ingest_faturamentos.py
Descrição: Ingestão de dados de faturamentos para a camada Bronze.
"""

import sys
from pathlib import Path
from typing import Dict, List

# Adiciona o diretório raiz ao path para permitir importações de outros módulos
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestFaturamentos(BaseCSVIngestor):
    """
    Ingestor para dados de faturamentos, aplicando validações rigorosas.
    """

    def __init__(self):
        """Inicializa o ingestor com seus parâmetros específicos."""
        super().__init__(
            script_name='ingest_faturamentos.py',
            tabela_destino='bronze.faturamentos',
            arquivo_nome='faturamentos.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """Mapeia colunas do CSV para colunas do banco de dados."""
        return {
            'Data': 'data',
            'Receita': 'receita',
            'Moeda': 'moeda',
            'CNPJ Cliente': 'cnpj_cliente',
            'Email Usuario': 'email_usuario'
        }

    def get_bronze_columns(self) -> List[str]:
        """Retorna a lista ordenada de colunas da tabela de destino."""
        return ['data', 'receita', 'moeda', 'cnpj_cliente', 'email_usuario']

    def get_date_columns(self) -> List[str]:
        """Retorna as colunas que devem ser tratadas como data."""
        return ['data']

    def get_validation_rules(self) -> Dict[str, dict]:
        """Define as regras de validação rigorosas para cada campo."""
        return {
            'data': {
                'obrigatorio': True,
                'tipo': 'data',
                'formato_data': '%Y-%m-%d'
            },
            'receita': {
                'obrigatorio': True,
                'tipo': 'decimal',
                'positivo': True
            },
            'moeda': {
                'obrigatorio': True,
                'tipo': 'string',
                'dominio': ['BRL', 'USD', 'EUR'],
                'case_sensitive': False
            },
            'cnpj_cliente': {
                'obrigatorio': True,
                'tipo': 'cnpj_cpf'
            },
            'email_usuario': {
                'obrigatorio': True,
                'tipo': 'email'
            }
        }


if __name__ == '__main__':
    # Permite a execução do script diretamente
    sys.exit(IngestFaturamentos().executar())