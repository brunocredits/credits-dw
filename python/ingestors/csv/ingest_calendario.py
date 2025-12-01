#!/usr/bin/env python3
"""
Módulo: ingest_calendario.py
Descrição: Geração e Ingestão da Tabela de Datas (bronze.data)
"""

import sys
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class IngestCalendario(BaseCSVIngestor):
    
    def __init__(self):
        # Não requer arquivo real, mas passamos um nome fictício
        super().__init__(
            script_name='ingest_calendario.py',
            tabela_destino='bronze.data',
            arquivo_nome='generated_calendar'
        )

    def validar_arquivo(self) -> bool:
        # Bypass validação de arquivo pois geramos em memória
        return True

    def ler_csv(self) -> pd.DataFrame:
        self.logger.info("Gerando calendario 2020-2030...")
        
        start_date = date(2020, 1, 1)
        end_date = date(2030, 12, 31)
        delta = end_date - start_date
        
        dates = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
        
        df = pd.DataFrame({'data': dates})
        df['data'] = pd.to_datetime(df['data'])
        
        # Colunas Simples
        df['ano'] = df['data'].dt.year
        df['mes'] = df['data'].dt.month
        df['dia'] = df['data'].dt.day
        df['ano_mes'] = df['data'].dt.strftime('%Y-%m')
        df['semana_ano'] = df['data'].dt.isocalendar().week
        
        # Nomes (Pt-Br requer locale ou map manual. Map é mais seguro)
        meses = {1:'Janeiro', 2:'Fevereiro', 3:'Março', 4:'Abril', 5:'Maio', 6:'Junho',
                 7:'Julho', 8:'Agosto', 9:'Setembro', 10:'Outubro', 11:'Novembro', 12:'Dezembro'}
        dias = {0:'Segunda', 1:'Terça', 2:'Quarta', 3:'Quinta', 4:'Sexta', 5:'Sábado', 6:'Domingo'}
        
        df['nome_mes'] = df['mes'].map(meses)
        # df['dia_semana'] = df['data'].dt.dayofweek + 1 # 1=Segunda, 7=Domingo (iso)
        # Ajuste para request: "dia_semana" e "nome_dia_semana".
        # Vou manter padrão ISO (1-7)
        df['dia_semana'] = df['data'].dt.dayofweek + 1
        
        df['nome_dia_semana'] = df['data'].dt.dayofweek.map(dias)
        
        # Estruturas
        df['bimestre'] = ((df['mes'] - 1) // 2) + 1
        df['trimestre'] = df['data'].dt.quarter
        df['quarter'] = df['trimestre']
        df['semestre'] = ((df['mes'] - 1) // 6) + 1
        
        # Marcadores
        df['is_weekend'] = df['data'].dt.dayofweek.isin([5, 6]) # Sat, Sun
        df['is_holiday'] = False # Placeholder - Default False
        
        # Converter data para string DATE
        df['data'] = df['data'].dt.date
        
        return df.astype(str)

    def get_column_mapping(self) -> Dict[str, str]:
        # Já geramos com nomes corretos
        return {c: c for c in [
            'data', 'ano', 'mes', 'dia', 'ano_mes', 'semana_ano',
            'nome_mes', 'dia_semana', 'nome_dia_semana',
            'bimestre', 'trimestre', 'quarter', 'semestre',
            'is_weekend', 'is_holiday'
        ]}

    def get_mandatory_fields(self) -> List[str]:
        return ['data', 'ano', 'mes', 'dia']

    def get_custom_schema(self) -> Dict[str, str]:
        return {
            'data': 'DATE PRIMARY KEY',
            'ano': 'INT',
            'mes': 'INT',
            'dia': 'INT',
            'bimestre': 'INT',
            'trimestre': 'INT',
            'quarter': 'INT',
            'semestre': 'INT',
            'is_weekend': 'BOOLEAN',
            'is_holiday': 'BOOLEAN'
        }

if __name__ == '__main__':
    sys.exit(IngestCalendario().executar())