#!/usr/bin/env python3
"""
Módulo: ingest_faturamentos.py
Descrição: Ingestão de Faturamentos para Bronze (Permissive)
"""

import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestFaturamentos(BaseCSVIngestor):
    
    def __init__(self):
        super().__init__(
            script_name='ingest_faturamentos.py',
            tabela_destino='bronze.faturamento',
            arquivo_nome='faturamentos.csv'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        # Standardizer já entrega com nomes corretos (snake_case do schema)
        cols = [
            'status', 'numero_documento', 'parcela', 'nota_fiscal', 'cliente_nome_fantasia',
            'previsao_recebimento', 'ultimo_recebimento', 'valor_conta', 'valor_liquido',
            'impostos_retidos', 'desconto', 'juros_multa', 'valor_recebido', 'valor_a_receber',
            'categoria', 'operacao', 'vendedor', 'projeto', 'conta_corrente', 'numero_boleto',
            'tipo_documento', 'vencimento', 'data_emissao', 'cliente_razao_social',
            'cliente_sem_pontuacao', 'tags_cliente', 'observacao', 'ultima_alteracao',
            'incluido_por', 'alterado_por', 'data_fat', 'empresa', 'ms'
        ]
        return {c: c for c in cols}

    def get_mandatory_fields(self) -> List[str]:
        return [
            'numero_documento',
            'data_emissao',
            'vencimento',
            'vendedor',
            'valor_conta'
        ]

    def get_custom_schema(self) -> Dict[str, str]:
        return {
            'valor_conta': 'DECIMAL(18,2)',
            'valor_liquido': 'DECIMAL(18,2)',
            'valor_recebido': 'DECIMAL(18,2)',
            'valor_a_receber': 'DECIMAL(18,2)',
            'data_emissao': 'DATE',
            'vencimento': 'DATE',
            'previsao_recebimento': 'DATE',
            'ultimo_recebimento': 'DATE'
        }

if __name__ == '__main__':
    sys.exit(IngestFaturamentos().executar())