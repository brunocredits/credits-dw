#!/usr/bin/env python3
"""
Módulo: ingest_contas.py
Descrição: Ingestão de dados de contas para a camada Bronze.
"""

import sys
from pathlib import Path
from typing import Dict, List

# Adiciona o diretório raiz ao path para permitir importações de outros módulos
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestContas(BaseCSVIngestor):
    """
    Ingestor para dados de contas, aplicando validações rigorosas.
    """

    def __init__(self):
        """Inicializa o ingestor com seus parâmetros específicos."""
        super().__init__(
            script_name='ingest_contas.py',
            tabela_destino='bronze.contas',
            arquivo_nome='contas.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """Mapeia colunas do CSV para colunas do banco de dados."""
        return {
            'cnpj_cpf': 'cnpj_cpf',
            'tipo': 'tipo',
            'status': 'status',
            'status_qualificacao': 'status_qualificacao',
            'data_criacao': 'data_criacao',
            'grupo': 'grupo',
            'razao_social': 'razao_social',
            'responsavel_conta': 'responsavel_conta',
            'financeiro': 'financeiro',
            'corte': 'corte',
            'faixa': 'faixa'
        }

    def get_bronze_columns(self) -> List[str]:
        """Retorna a lista ordenada de colunas da tabela de destino."""
        return [
            'cnpj_cpf', 'tipo', 'status', 'status_qualificacao',
            'data_criacao', 'grupo', 'razao_social', 'responsavel_conta',
            'financeiro', 'corte', 'faixa'
        ]

    def get_date_columns(self) -> List[str]:
        """Retorna as colunas que devem ser tratadas como data."""
        return ['data_criacao']

    def get_validation_rules(self) -> Dict[str, dict]:
        """Define as regras de validação rigorosas para cada campo."""
        return {
            'cnpj_cpf': {
                'obrigatorio': True,
                'tipo': 'cnpj_cpf'
            },
            'tipo': {
                'obrigatorio': True,
                'tipo': 'string',
                'dominio': ['PJ', 'PF']
            },
            'status': {
                'obrigatorio': True,
                'tipo': 'string',
                'dominio': ['Ativo', 'Inativo', 'Suspenso']
            },
            'status_qualificacao': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 100
            },
            'data_criacao': {
                'obrigatorio': True,
                'tipo': 'data',
                'formato_data': '%Y-%m-%d'
            },
            'razao_social': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 255
            },
        }


if __name__ == '__main__':
    # Permite a execução do script diretamente
    sys.exit(IngestContas().executar())