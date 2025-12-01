#!/usr/bin/env python3
"""
Script: standardize_files.py
Descrição: Padroniza arquivos CSV para o Schema ESTRITO definido.
"""

import sys
import os
import pandas as pd
import numpy as np
import unicodedata
import re
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / 'data' / 'input'
OUTPUT_DIR = BASE_DIR / 'data' / 'ready'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger('Standardizer')

class FileStandardizer:
    def __init__(self):
        self.summary = []

    def get_strict_schema(self, target_name):
        schemas = {
            "faturamentos.csv": [
                'status', 'numero_documento', 'parcela', 'nota_fiscal', 'cliente_nome_fantasia',
                'previsao_recebimento', 'ultimo_recebimento', 'valor_conta', 'valor_liquido',
                'impostos_retidos', 'desconto', 'juros_multa', 'valor_recebido', 'valor_a_receber',
                'categoria', 'operacao', 'vendedor', 'projeto', 'conta_corrente', 'numero_boleto',
                'tipo_documento', 'vencimento', 'data_emissao', 'cliente_razao_social',
                'cliente_sem_pontuacao', 'tags_cliente', 'observacao', 'ultima_alteracao',
                'incluido_por', 'alterado_por', 'data_fat', 'empresa', 'ms'
            ],
            "base_oficial.csv": [
                'cnpj', 'status', 'manter_no_baseline', 'nome_fantasia', 'canal1', 'canal2',
                'lider', 'responsavel', 'empresa', 'grupo', 'obs', 'faixas', 'mediana'
            ],
            "usuarios.csv": [
                'cargo', 'status', 'nome_usuario', 'nivel', 'time', 
                'meta_mensal', 'meta_fidelidade', 'meta_anual', 
                'acesso_vendedor', 'acesso_gerente', 'acesso_indireto', 'acesso_diretoria', 'acesso_temporario',
                'email_usuario', 'email_superior', 'email_gerencia', 'email_diretoria'
            ]
        }
        return schemas.get(target_name, [])

    def normalize_text(self, text):
        if not isinstance(text, str): return str(text)
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII').lower()
        text = re.sub(r'[^a-z0-9]+', '_', text).strip('_')
        return text

    def clean_numeric(self, value):
        if pd.isna(value) or value == '': return None # Retorna None para NULL no banco
        if isinstance(value, (int, float)): return float(value)
        s = str(value).strip().replace('R$', '').replace(' ', '')
        if ',' in s and '.' in s:
            if s.find(',') > s.find('.'): s = s.replace('.', '').replace(',', '.')
            else: s = s.replace(',', '')
        elif ',' in s: s = s.replace(',', '.')
        try: return float(s)
        except: return None

    def apply_custom_mapping(self, df, target_name):
        rename_map = {}
        
        # Mapeamento Raw Header -> Target Column
        if target_name == "faturamentos.csv":
            custom_map = {
                'valor da conta': 'valor_conta',
                'valor liquido': 'valor_liquido',
                'valor recebido': 'valor_recebido',
                'valor a receber': 'valor_a_receber',
                'numero do documento': 'numero_documento',
                'data de emissao': 'data_emissao',
                'vencimento': 'vencimento',
                'nota fiscal / cupom fiscal': 'nota_fiscal',
                'nota fiscal': 'nota_fiscal',
                'cliente (nome fantasia)': 'cliente_nome_fantasia',
                'cliente': 'cliente_nome_fantasia',
                'razao social': 'cliente_razao_social',
                'impostos retidos': 'impostos_retidos',
                'juros e multa': 'juros_multa'
            }
        elif target_name == "base_oficial.csv":
            custom_map = {
                'razao social': 'nome_fantasia', 
                'nome fantasia': 'nome_fantasia',
                'cnpj': 'cnpj',
                'status': 'status',
                'responsavel': 'responsavel',
                'lider': 'lider',
                'canal 1': 'canal1',
                'canal 2': 'canal2',
                'manter no baseline': 'manter_no_baseline'
            }
        elif target_name == "usuarios.csv":
            custom_map = {
                'consultor(a)': 'nome_usuario',
                'nome': 'nome_usuario',
                'status vendedor': 'status',
                'meta': 'meta_mensal',
                'meta_anual': 'meta_anual',
                'mensal_fidelidade': 'meta_fidelidade',
                'email': 'email_usuario',
                'email lider': 'email_superior',
                'acesso vendedor': 'acesso_vendedor',
                'acesso gerente': 'acesso_gerente',
                'acesso indireto': 'acesso_indireto',
                'acesso diretoria': 'acesso_diretoria',
                'acesso temporario': 'acesso_temporario'
            }
        else:
            custom_map = {}

        for col in df.columns:
            col_clean = str(col).lower().strip()
            # Tenta match exato
            if col_clean in custom_map:
                rename_map[col] = custom_map[col_clean]
            else:
                # Tenta match parcial seguro
                for k, v in custom_map.items():
                    if k == col_clean: # Já checado
                        continue
        
        if rename_map:
            logger.info(f"   🔄 Mapeando colunas: {rename_map}")
            df = df.rename(columns=rename_map)
            
        return df

    def process_file(self, file_path):
        file_name = file_path.name
        logger.info(f"Processando: {file_name}")
        file_ext = Path(file_name).suffix.lower()
        if file_ext not in ['.csv', '.xls', '.xlsx']: return False

        df = pd.DataFrame()
        
        # Leitura Excel Inteligente
        if file_ext in ['.xls', '.xlsx']:
            try:
                xls = pd.ExcelFile(file_path)
                sheets = xls.sheet_names
                best_sheet = None
                
                if "base" in file_name.lower():
                    for s in sheets:
                        if "base" in s.lower():
                            best_sheet = s; break
                
                if not best_sheet:
                    # Maior aba
                    max_cells = 0
                    for s in sheets:
                        temp = pd.read_excel(file_path, sheet_name=s, header=None)
                        cells = temp.size
                        if cells > max_cells: max_cells = cells; best_sheet = s
                
                if best_sheet:
                    logger.info(f"   👉 Aba selecionada: '{best_sheet}'")
                    # Detect header row inside sheet
                    head = pd.read_excel(file_path, sheet_name=best_sheet, header=None, nrows=20)
                    # Simple detection: row with most strings
                    best_idx = 0; max_str = 0
                    for idx, row in head.iterrows():
                        strs = sum(1 for x in row.dropna() if isinstance(x, str))
                        if strs > max_str: max_str = strs; best_idx = idx
                    
                    df = pd.read_excel(file_path, sheet_name=best_sheet, header=best_idx)
                else: return False
            except: return False
        else:
            # CSV
            try:
                df = pd.read_csv(file_path, sep=';', encoding='utf-8') # Tenta padrão primeiro
            except:
                try: df = pd.read_csv(file_path, sep=',', encoding='latin1')
                except: return False

        # Inferir Destino
        target_name = "unknown"
        fn = self.normalize_text(file_name)
        if "faturamento" in fn: target_name = "faturamentos.csv"
        elif "base" in fn: target_name = "base_oficial.csv"
        elif "usuario" in fn: target_name = "usuarios.csv"
        
        # Renomear
        df = self.apply_custom_mapping(df, target_name)
        
        # Normalizar restantes
        df.columns = [self.normalize_text(c) if c not in self.get_strict_schema(target_name) else c for c in df.columns]
        
        # SCHEMA STRICT
        strict = self.get_strict_schema(target_name)
        if strict:
            # Manter apenas o que está no schema
            df = df[[c for c in df.columns if c in strict]]
            
            # Adicionar faltantes como vazio (para CSV template)
            for c in strict:
                if c not in df.columns:
                    df[c] = None
            
            # Reordenar
            df = df[strict]

        # Limpar Dados (Datas e Numeros) sem preencher com default
        # ... (simplificado para manter NULLs)
        
        output_path = OUTPUT_DIR / target_name
        df.to_csv(output_path, sep=';', index=False, encoding='utf-8', date_format='%Y-%m-%d', float_format='%.2f')
        
        self.summary.append({'Arquivo': file_name, 'Destino': target_name, 'Linhas': len(df)})
        logger.info(f"✅ Gerado: {target_name}")
        return True

    def run(self):
        for f in list(INPUT_DIR.glob('*')):
            if f.is_file(): self.process_file(f)

if __name__ == "__main__":
    FileStandardizer().run()