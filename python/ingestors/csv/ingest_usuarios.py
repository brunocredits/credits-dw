#!/usr/bin/env python3
"""
Script: ingest_usuarios.py
Descrição: Ingestão de dados de usuários CSV para a camada Bronze
Frequência: Diária
Versão: 1.0
"""

import sys
from pathlib import Path
from typing import Dict, List

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor

class IngestUsuarios(BaseCSVIngestor):
    """
    Ingestor para os dados de usuários.
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
        # TODO: Ajustar este mapeamento de acordo com as colunas reais do arquivo usuarios.csv
        return {
            'ID_USUARIO': 'id_usuario',
            'NOME_USUARIO': 'nome_usuario',
            'EMAIL_USUARIO': 'email_usuario'
        }

    def get_bronze_columns(self) -> List[str]:
        """
        Retorna a lista de colunas da tabela Bronze.
        """
        # TODO: Ajustar esta lista de colunas de acordo com as colunas reais do arquivo usuarios.csv
        return [
            'id_usuario',
            'nome_usuario',
            'email_usuario'
        ]

if __name__ == '__main__':
    ingestor = IngestUsuarios()
    sys.exit(ingestor.executar())
