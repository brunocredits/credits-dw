#!/usr/bin/env python3
"""
Módulo: ingest_usuarios.py
Descrição: Ingestão de dados de usuários para a camada Bronze.
"""

import sys
from pathlib import Path
from typing import Dict, List

# Adiciona o diretório raiz ao path para permitir importações de outros módulos
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestUsuarios(BaseCSVIngestor):
    """
    Ingestor para dados de usuários, aplicando validações rigorosas.
    """

    def __init__(self):
        """Inicializa o ingestor com seus parâmetros específicos."""
        super().__init__(
            script_name='ingest_usuarios.py',
            tabela_destino='bronze.usuarios',
            arquivo_nome='usuarios.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """Mapeia colunas do CSV para colunas do banco de dados."""
        return {
            'nome_empresa': 'nome_empresa',
            'nome': 'nome',
            'area': 'area',
            'senioridade': 'senioridade',
            'gestor': 'gestor',
            'email': 'email',
            'canal_1': 'canal_1',
            'canal_2': 'canal_2',
            'email_lider': 'email_lider'
        }

    def get_bronze_columns(self) -> List[str]:
        """Retorna a lista ordenada de colunas da tabela de destino."""
        return [
            'nome_empresa', 'nome', 'area', 'senioridade',
            'gestor', 'email', 'canal_1', 'canal_2', 'email_lider'
        ]

    def get_validation_rules(self) -> Dict[str, dict]:
        """Define as regras de validação rigorosas para cada campo."""
        return {
            'nome_empresa': {
                'obrigatorio': True,
                'tipo': 'string',
                'max_len': 255
            },
            'nome': {
                'obrigatorio': True,
                'tipo': 'string',
                'max_len': 255
            },
            'email': {
                'obrigatorio': True,
                'tipo': 'email'
            },
            'canal_1': {
                'obrigatorio': True,
                'tipo': 'string',
                'max_len': 100
            },
            'email_lider': {
                'obrigatorio': False,
                'tipo': 'email'
            }
        }


if __name__ == '__main__':
    # Permite a execução do script diretamente
    sys.exit(IngestUsuarios().executar())