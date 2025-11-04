#!/usr/bin/env python3
"""
Script: ingest_contas_base_oficial.py
Descrição: Ingestão de dados da base oficial de contas para a camada Bronze
"""

import sys
from pathlib import Path

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class ContasBaseOficialIngestor(BaseCSVIngestor):
    """
    Ingestor para a base oficial de contas.
    """

    def __init__(self):
        super().__init__(
            script_name="ingest_contas_base_oficial.py",
            tabela_destino="bronze.contas_base_oficial",
            arquivo_nome="contas_base_oficial.csv",
            input_subdir="onedrive"
        )

    def get_column_mapping(self) -> dict:
        """
        Retorna o mapeamento de colunas do CSV para a tabela Bronze.
        """
        return {
            'ID_Cliente': 'cliente_id',
            'Codigo': 'codigo_cliente',
            'CNPJ_CPF': 'cnpj_cpf',
            'Razao_Social': 'razao_social',
            'Nome_Fantasia': 'nome_fantasia',
            'Tipo': 'tipo_pessoa',
            'Email': 'email',
            'Telefone': 'telefone',
            'Celular': 'celular',
            'Logradouro': 'logradouro',
            'Numero': 'numero',
            'Complemento': 'complemento',
            'Bairro': 'bairro',
            'Cidade': 'cidade',
            'UF': 'estado',
            'CEP': 'cep',
            'Segmento': 'segmento',
            'Porte': 'porte_empresa',
            'Consultor': 'consultor_responsavel',
            'Status': 'status_cliente'
        }

    def get_bronze_columns(self) -> list:
        """
        Retorna a lista de colunas da tabela Bronze na ordem correta.
        """
        return [
            'cliente_id', 'codigo_cliente', 'cnpj_cpf', 'razao_social', 'nome_fantasia',
            'tipo_pessoa', 'email', 'telefone', 'celular', 'logradouro', 'numero',
            'complemento', 'bairro', 'cidade', 'estado', 'cep', 'segmento',
            'porte_empresa', 'consultor_responsavel', 'status_cliente'
        ]


if __name__ == "__main__":
    ingestor = ContasBaseOficialIngestor()
    sys.exit(ingestor.executar())