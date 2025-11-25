#!/usr/bin/env python3
"""
Módulo: ingest_faturamento.py
Descrição: Ingestão de dados de faturamento para camada Bronze
Versão: 2.0

Este ingestor processa dados de faturamento com validação rigorosa.
Apenas registros válidos são inseridos na Bronze.
"""

import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestFaturamento(BaseCSVIngestor):
    """
    Ingestor para dados de faturamento.

    Validações aplicadas:
    - Data: obrigatória, formato de data válido
    - Receita: obrigatória, número decimal positivo
    - Moeda: obrigatória, código de moeda válido (BRL, USD, EUR)
    - CNPJ Cliente: obrigatório, CNPJ/CPF válido
    - Email Usuario: obrigatório, formato de email válido
    """

    def __init__(self):
        super().__init__(
            script_name='ingest_faturamento.py',
            tabela_destino='bronze.faturamento',
            arquivo_nome='faturamento.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """Mapeamento de colunas CSV -> Bronze"""
        return {
            'Data': 'data',
            'Receita': 'receita',
            'Moeda': 'moeda',
            'CNPJ Cliente': 'cnpj_cliente',
            'Email Usuario': 'email_usuario'
        }

    def get_bronze_columns(self) -> List[str]:
        """Colunas da tabela bronze.faturamento"""
        return ['data', 'receita', 'moeda', 'cnpj_cliente', 'email_usuario']

    def get_date_columns(self) -> List[str]:
        """Colunas de data para formatação automática"""
        return ['data']

    def get_validation_rules(self) -> Dict[str, dict]:
        """
        Regras de validação rigorosas para cada campo.

        Returns:
            Dicionário com regras de validação
        """
        return {
            # Data de faturamento: obrigatória e formato válido
            'data': {
                'obrigatorio': True,
                'tipo': 'data',
                'formato_data': '%Y-%m-%d'
            },

            # Receita: obrigatória, decimal positivo
            'receita': {
                'obrigatorio': True,
                'tipo': 'decimal',
                'positivo': True
            },

            # Moeda: obrigatória, domínio fechado
            'moeda': {
                'obrigatorio': True,
                'tipo': 'string',
                'dominio': ['BRL', 'USD', 'EUR'],
                'case_sensitive': False
            },

            # CNPJ/CPF do cliente: obrigatório e válido
            'cnpj_cliente': {
                'obrigatorio': True,
                'tipo': 'cnpj_cpf'
            },

            # Email do usuário: obrigatório e formato válido
            'email_usuario': {
                'obrigatorio': True,
                'tipo': 'email'
            }
        }


if __name__ == '__main__':
    sys.exit(IngestFaturamento().executar())
