#!/usr/bin/env python3
"""
Script: ingest_usuarios.py
Descrição: Ingestão de dados de usuarios do OneDrive CSV para a camada Bronze
"""

import sys
from pathlib import Path
from typing import Dict, List

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestUsuarios(BaseCSVIngestor):
    """
    Ingestor para os dados de usuarios.
    """

    def __init__(self):
        super().__init__(
            script_name='ingest_usuarios.py',
            tabela_destino='bronze.usuarios',
            arquivo_nome='usuarios.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """
        Retorna o mapeamento de colunas do CSV para a tabela Bronze.
        """
        # ATENÇÃO: Verifique se os nomes das colunas do CSV correspondem aos valores neste mapeamento.
        # Se os nomes das colunas no arquivo CSV forem diferentes, ajuste as chaves do dicionário abaixo.
        return {
            'id': 'id',
            'nome_empresa': 'nome_empresa',
            'Nome': 'Nome',
            'area': 'area',
            'senioridade': 'senioridade',
            'gestor': 'gestor',
            'email': 'email',
            'canal 1': 'canal 1',
            'canal 2': 'canal 2',
            'email_lider': 'email_lider'
        }

    def get_bronze_columns(self) -> List[str]:
        """
        Retorna a lista de colunas da tabela Bronze.
        """
        return [
            'sk_id',
            'id',
            'nome_empresa',
            'Nome',
            'area',
            'senioridade',
            'gestor',
            'email',
            'canal 1',
            'canal 2',
            'email_lider'
        ]

if __name__ == '__main__':
    ingestor = IngestUsuarios()
    sys.exit(ingestor.executar())
