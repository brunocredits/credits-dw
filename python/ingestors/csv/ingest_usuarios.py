#!/usr/bin/env python3
"""
Módulo: ingest_usuarios.py
Descrição: Ingestão de Usuários e Permissões
"""

import sys
from pathlib import Path
from typing import Dict, List
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestUsuarios(BaseCSVIngestor):
    
    def __init__(self):
        super().__init__(
            script_name='ingest_usuarios.py',
            tabela_destino='bronze.usuarios',
            arquivo_nome='usuarios.csv'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        # Mapeamento direto pois o standardizer já normalizou para os nomes finais
        return {
            'cargo': 'cargo',
            'status': 'status',
            'nome_usuario': 'nome_usuario',
            'nivel': 'nivel',
            'time': 'time',
            'meta_mensal': 'meta_mensal',
            'meta_fidelidade': 'meta_fidelidade',
            'meta_anual': 'meta_anual',
            'acesso_vendedor': 'acesso_vendedor',
            'acesso_gerente': 'acesso_gerente',
            'acesso_indireto': 'acesso_indireto',
            'acesso_diretoria': 'acesso_diretoria',
            'acesso_temporario': 'acesso_temporario',
            'email_usuario': 'email_usuario',
            'email_superior': 'email_superior',
            'email_gerencia': 'email_gerencia',
            'email_diretoria': 'email_diretoria'
        }

    def get_mandatory_fields(self) -> List[str]:
        # Campos essenciais para identificação e hierarquia
        return ['nome_usuario', 'email_usuario', 'time', 'cargo']

    def get_custom_schema(self) -> Dict[str, str]:
        return {
            'meta_mensal': 'DECIMAL(18,2)',
            'meta_anual': 'DECIMAL(18,2)',
            'meta_fidelidade': 'DECIMAL(18,2)',
            'acesso_vendedor': 'BOOLEAN',
            'acesso_gerente': 'BOOLEAN',
            'acesso_indireto': 'BOOLEAN',
            'acesso_diretoria': 'BOOLEAN',
            'acesso_temporario': 'BOOLEAN'
        }

    def transform_custom(self, df: pd.DataFrame) -> pd.DataFrame:
        # Converter campos de email/acesso para Boolean (True se tiver conteudo)
        bool_cols = ['acesso_vendedor', 'acesso_gerente', 'acesso_indireto', 'acesso_diretoria', 'acesso_temporario']
        
        for col in bool_cols:
            if col in df.columns:
                # Se tiver texto (len > 3 assumindo x@x), vira True. Senão False.
                # Mas cuidado com "None" string
                df[col] = df[col].apply(lambda x: True if x and str(x).strip() not in ['None', '', 'nan'] else False)
        
        return df

if __name__ == '__main__':
    sys.exit(IngestUsuarios().executar())