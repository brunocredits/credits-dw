"""
Este módulo, `Validator`, é focado na validação da estrutura dos arquivos de
entrada. Sua principal responsabilidade é garantir que os cabeçalhos dos
arquivos de dados correspondam exatamente aos templates oficiais definidos
para cada tipo de ingestão.
"""

import pandas as pd
from pathlib import Path
from typing import List
from python.core.file_handler import FileHandler

class Validator:
    """
    Valida a conformidade estrutural de arquivos de dados contra seus templates.
    """
    
    def __init__(self, template_dir: Path):
        """
        Inicializa o validador.

        Args:
            template_dir (Path): O diretório onde os arquivos de template estão localizados.
        """
        self.template_dir = template_dir

    def validate_headers(self, ingestor_name: str, file_cols: List[str]) -> None:
        """
        Valida se os cabeçalhos (colunas) de um arquivo de dados correspondem
        ao seu template oficial.

        A função busca por um arquivo de template que corresponda ao nome do
        ingestor (ex: `template_faturamento.csv`). Se os cabeçalhos não forem
        idênticos (ordem e nome), a função lança um `ValueError` com detalhes
        sobre as colunas faltantes ou extras.

        Args:
            ingestor_name (str): O nome do ingestor (usado para encontrar o template).
            file_cols (List[str]): A lista de colunas do arquivo de dados.

        Raises:
            ValueError: Se os cabeçalhos não corresponderem ao template ou se
                        ocorrer um erro ao ler o template.
        """
        # Busca pelo arquivo de template correspondente ao ingestor
        template_files = list(self.template_dir.glob(f"template_{ingestor_name}.*"))
        if not template_files:
            print(f"   ⚠️  Template para '{ingestor_name}' não encontrado. Validação de cabeçalho pulada.")
            return

        template_path = template_files[0]
        
        try:
            # Lê apenas os cabeçalhos do template, dependendo da extensão do arquivo
            if template_path.suffix == '.csv':
                sep = FileHandler.detect_separator(template_path)
                tpl_df = pd.read_csv(template_path, sep=sep, nrows=0, encoding='utf-8')
            elif template_path.suffix == '.ods':
                tpl_df = pd.read_excel(template_path, engine='odf', nrows=0)
            else: # Assume outros formatos de Excel (.xls, .xlsx)
                tpl_df = pd.read_excel(template_path, nrows=0)
            
            expected_cols = list(tpl_df.columns)
            input_cols = list(file_cols)

            # Compara as colunas do arquivo com as colunas esperadas do template
            if expected_cols != input_cols:
                missing = set(expected_cols) - set(input_cols)
                extra = set(input_cols) - set(expected_cols)
                
                error_msg = (f"Os cabeçalhos do arquivo são inválidos em comparação com o template '{template_path.name}'.\n"
                             f"   - Esperado: {expected_cols}\n"
                             f"   - Encontrado: {input_cols}")
                if missing:
                    error_msg += f"\n   - Colunas Faltando: {list(missing)}"
                if extra:
                    error_msg += f"\n   - Colunas Extras: {list(extra)}"
                raise ValueError(error_msg)
                
            print(f"   ✅ Cabeçalhos validados com sucesso contra o template: {template_path.name}")

        except Exception as e:
            raise ValueError(f"Ocorreu um erro ao validar o template '{template_path.name}': {e}")
