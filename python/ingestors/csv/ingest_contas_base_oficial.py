#!/usr/bin/env python3
"""
Script: ingest_contas_base_oficial.py
Descrição: Ingestão de dados de contas do OneDrive CSV para a camada Bronze
Frequência: Diária
Versão: 1.0
"""

import sys
from pathlib import Path
from typing import Dict, List

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor

class IngestContasBaseOficial(BaseCSVIngestor):
    """
    Ingestor para os dados de contas da base oficial.
    """

    def __init__(self):
        super().__init__(
            script_name='ingest_contas_base_oficial.py',
            tabela_destino='bronze.contas_base_oficial',
            arquivo_nome='contas_base_oficial.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """
        Retorna o mapeamento de colunas do CSV para a tabela Bronze.
        """
        return {
            'CNPJ/CPF PK': 'cnpj_cpf',
            'Tipo': 'tipo',
            'Status': 'status',
            'Status de Qualificação da conta': 'status_qualificação_da_conta',
            'Data de criação': 'data_criacao',
            'Grupo': 'grupo',
            'Razão Social': 'razao_social',
            'Responsável da Conta': 'resposanvel_conta',
            'Financeiro': 'financeiro',
            'Corte': 'corte',
            'Faixa': 'faixa'
        }

    def get_bronze_columns(self) -> List[str]:
        """
        Retorna a lista de colunas da tabela Bronze.
        """
        return [
            'cnpj_cpf',
            'tipo',
            'status',
            'status_qualificação_da_conta',
            'data_criacao',
            'grupo',
            'razao_social',
            'resposanvel_conta',
            'financeiro',
            'corte',
            'faixa'
        ]

if __name__ == '__main__':
    ingestor = IngestContasBaseOficial()
    sys.exit(ingestor.executar())