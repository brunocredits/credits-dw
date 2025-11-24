#!/usr/bin/env python3
"""Ingestão de usuários para Bronze"""
import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor

class IngestUsuarios(BaseCSVIngestor):
    """Ingestor para dados de usuários"""

    def __init__(self):
        super().__init__(
            script_name='ingest_usuarios.py',
            tabela_destino='bronze.usuarios',
            arquivo_nome='usuarios.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
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
        return [
            'nome_empresa', 'Nome', 'area', 'senioridade',
            'gestor', 'email', 'canal_1', 'canal_2', 'email_lider'
        ]

if __name__ == '__main__':
    sys.exit(IngestUsuarios().executar())
