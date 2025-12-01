#!/usr/bin/env python3
"""
Módulo: ingest_base_oficial.py
Descrição: Ingestão da Base Oficial (antiga contas)
"""

import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestBaseOficial(BaseCSVIngestor):
    
    def __init__(self):
        super().__init__(
            script_name='ingest_base_oficial.py',
            tabela_destino='bronze.base_oficial',
            arquivo_nome='base_oficial.csv'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        cols = [
            'cnpj', 'status', 'manter_no_baseline', 'nome_fantasia', 'canal1', 'canal2',
            'lider', 'responsavel', 'empresa', 'grupo', 'obs', 'faixas', 'mediana'
        ]
        return {c: c for c in cols}

    def get_mandatory_fields(self) -> List[str]:
        return ['cnpj', 'status', 'responsavel']

    def get_custom_schema(self) -> Dict[str, str]:
        # FAIXAS e MEDIANA são formulas, manter texto
        return {
            'cnpj': 'TEXT',
            'faixas': 'TEXT',
            'mediana': 'TEXT'
        }

if __name__ == '__main__':
    sys.exit(IngestBaseOficial().executar())
