#!/usr/bin/env python3
"""
Módulo: ingest_data.py
Descrição: Ingestão de dimensão data para camada Bronze
Versão: 2.0

Este ingestor processa dados da dimensão tempo/data com validação rigorosa.
Apenas registros válidos são inseridos na Bronze.
"""

import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestData(BaseCSVIngestor):
    """
    Ingestor para dados de dimensão data.

    Validações aplicadas:
    - Data Completa: obrigatória, formato de data válido
    - Ano: obrigatório, inteiro entre 1900 e 2100
    - Mês: obrigatório, inteiro entre 1 e 12
    - Dia: obrigatório, inteiro entre 1 e 31
    - Bimestre, Trimestre, Quarter, Semestre: obrigatórios, inteiros válidos
    """

    def __init__(self):
        super().__init__(
            script_name='ingest_data.py',
            tabela_destino='bronze.data',
            arquivo_nome='data.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """Mapeamento de colunas CSV -> Bronze"""
        return {
            'Data Completa': 'data_completa',
            'Ano': 'ano',
            'Mês': 'mes',
            'Dia': 'dia',
            'Bimestre': 'bimestre',
            'Trimestre': 'trimestre',
            'Quarter': 'quarter',
            'Semestre': 'semestre'
        }

    def get_bronze_columns(self) -> List[str]:
        """Colunas da tabela bronze.data"""
        return [
            'data_completa', 'ano', 'mes', 'dia',
            'bimestre', 'trimestre', 'quarter', 'semestre'
        ]

    def get_date_columns(self) -> List[str]:
        """Colunas de data para formatação automática"""
        return ['data_completa']

    def get_validation_rules(self) -> Dict[str, dict]:
        """
        Regras de validação rigorosas para cada campo.

        Returns:
            Dicionário com regras de validação
        """
        return {
            # Data completa: obrigatória e formato válido
            'data_completa': {
                'obrigatorio': True,
                'tipo': 'data',
                'formato_data': '%Y-%m-%d'
            },

            # Ano: obrigatório, inteiro entre 1900 e 2100
            'ano': {
                'obrigatorio': True,
                'tipo': 'int',
                'minimo': 1900,
                'maximo': 2100
            },

            # Mês: obrigatório, inteiro entre 1 e 12
            'mes': {
                'obrigatorio': True,
                'tipo': 'int',
                'minimo': 1,
                'maximo': 12
            },

            # Dia: obrigatório, inteiro entre 1 e 31
            'dia': {
                'obrigatorio': True,
                'tipo': 'int',
                'minimo': 1,
                'maximo': 31
            },

            # Bimestre: obrigatório, inteiro entre 1 e 6
            'bimestre': {
                'obrigatorio': True,
                'tipo': 'int',
                'minimo': 1,
                'maximo': 6
            },

            # Trimestre: obrigatório, inteiro entre 1 e 4
            'trimestre': {
                'obrigatorio': True,
                'tipo': 'int',
                'minimo': 1,
                'maximo': 4
            },

            # Quarter: obrigatório, inteiro entre 1 e 4
            'quarter': {
                'obrigatorio': True,
                'tipo': 'int',
                'minimo': 1,
                'maximo': 4
            },

            # Semestre: obrigatório, inteiro entre 1 e 2
            'semestre': {
                'obrigatorio': True,
                'tipo': 'int',
                'minimo': 1,
                'maximo': 2
            }
        }


if __name__ == '__main__':
    sys.exit(IngestData().executar())
