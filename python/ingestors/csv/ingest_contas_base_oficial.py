#!/usr/bin/env python3
"""
Módulo: ingest_contas_base_oficial.py
Descrição: Ingestão de dados de contas da base oficial para camada Bronze
Versão: 2.0

Este ingestor processa dados de contas com validação rigorosa.
Apenas registros válidos são inseridos na Bronze.
"""

import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestContasBaseOficial(BaseCSVIngestor):
    """
    Ingestor para dados de contas da base oficial.

    Validações aplicadas:
    - CNPJ/CPF: obrigatório e válido
    - Tipo: obrigatório
    - Status: obrigatório
    - Data Criação: obrigatória, formato válido
    - Demais campos: opcionais
    """

    def __init__(self):
        super().__init__(
            script_name='ingest_contas_base_oficial.py',
            tabela_destino='bronze.contas_base_oficial',
            arquivo_nome='contas_base_oficial.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """Mapeamento de colunas CSV -> Bronze"""
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
        """Colunas da tabela bronze.contas_base_oficial"""
        return [
            'cnpj_cpf', 'tipo', 'status', 'status_qualificação_da_conta',
            'data_criacao', 'grupo', 'razao_social', 'responsavel_conta',
            'financeiro', 'corte', 'faixa'
        ]

    def get_date_columns(self) -> List[str]:
        """Colunas de data para formatação automática"""
        return ['data_criacao']

    def get_validation_rules(self) -> Dict[str, dict]:
        """
        Regras de validação rigorosas para cada campo.

        Returns:
            Dicionário com regras de validação
        """
        return {
            # CNPJ/CPF: obrigatório e válido
            'cnpj_cpf': {
                'obrigatorio': True,
                'tipo': 'cnpj_cpf'
            },

            # Tipo: obrigatório
            'tipo': {
                'obrigatorio': True,
                'tipo': 'string',
                'min_len': 2,
                'max_len': 50
            },

            # Status: obrigatório
            'status': {
                'obrigatorio': True,
                'tipo': 'string',
                'min_len': 2,
                'max_len': 50
            },

            # Status de qualificação: opcional
            'status_qualificação_da_conta': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 100
            },

            # Data de criação: obrigatória e formato válido
            'data_criacao': {
                'obrigatorio': True,
                'tipo': 'data',
                'formato_data': '%Y-%m-%d'
            },

            # Grupo: opcional
            'grupo': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 100
            },

            # Razão social: opcional
            'razao_social': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 255
            },

            # Responsável da conta: opcional
            'responsavel_conta': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 255
            },

            # Financeiro: opcional
            'financeiro': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 100
            },

            # Corte: opcional
            'corte': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 50
            },

            # Faixa: opcional
            'faixa': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 50
            }
        }


if __name__ == '__main__':
    sys.exit(IngestContasBaseOficial().executar())
