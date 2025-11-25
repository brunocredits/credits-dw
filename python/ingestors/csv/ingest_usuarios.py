#!/usr/bin/env python3
"""
Módulo: ingest_usuarios.py
Descrição: Ingestão de dados de usuários para camada Bronze
Versão: 2.0

Este ingestor processa dados de usuários com validação rigorosa.
Apenas registros válidos são inseridos na Bronze.
"""

import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestUsuarios(BaseCSVIngestor):
    """
    Ingestor para dados de usuários.

    Validações aplicadas:
    - Nome Empresa: obrigatório
    - Nome: obrigatório
    - Email: obrigatório, formato válido
    - Canal 1: obrigatório
    - Área, Senioridade, Gestor, Canal 2, Email Líder: opcionais
    """

    def __init__(self):
        super().__init__(
            script_name='ingest_usuarios.py',
            tabela_destino='bronze.usuarios',
            arquivo_nome='usuarios.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """Mapeamento de colunas CSV -> Bronze"""
        return {
            'nome_empresa': 'nome_empresa',
            'Nome': 'Nome',
            'area': 'area',
            'senioridade': 'senioridade',
            'gestor': 'gestor',
            'email': 'email',
            'canal 1': 'canal_1',
            'canal 2': 'canal_2',
            'email_lider': 'email_lider'
        }

    def get_bronze_columns(self) -> List[str]:
        """Colunas da tabela bronze.usuarios"""
        return [
            'nome_empresa', 'Nome', 'area', 'senioridade',
            'gestor', 'email', 'canal_1', 'canal_2', 'email_lider'
        ]

    def get_validation_rules(self) -> Dict[str, dict]:
        """
        Regras de validação rigorosas para cada campo.

        Returns:
            Dicionário com regras de validação
        """
        return {
            # Nome da empresa: obrigatório
            'nome_empresa': {
                'obrigatorio': True,
                'tipo': 'string',
                'min_len': 2,
                'max_len': 255
            },

            # Nome do usuário: obrigatório
            'Nome': {
                'obrigatorio': True,
                'tipo': 'string',
                'min_len': 2,
                'max_len': 255
            },

            # Área: opcional
            'area': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 100
            },

            # Senioridade: opcional
            'senioridade': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 50
            },

            # Gestor: opcional
            'gestor': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 255
            },

            # Email: obrigatório e formato válido
            'email': {
                'obrigatorio': True,
                'tipo': 'email'
            },

            # Canal 1: obrigatório (canal principal do usuário)
            'canal_1': {
                'obrigatorio': True,
                'tipo': 'string',
                'min_len': 2,
                'max_len': 100
            },

            # Canal 2: opcional (canal secundário)
            'canal_2': {
                'obrigatorio': False,
                'tipo': 'string',
                'max_len': 100
            },

            # Email do líder: opcional, mas se preenchido deve ser válido
            'email_lider': {
                'obrigatorio': False,
                'tipo': 'email'
            }
        }


if __name__ == '__main__':
    sys.exit(IngestUsuarios().executar())
