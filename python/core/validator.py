import pandas as pd
from pathlib import Path
from typing import List
from python.core.file_handler import FileHandler

class Validator:
    """
    Responsável por validações estruturais (Headers, Templates).
    """
    def __init__(self, template_dir: Path):
        self.template_dir = template_dir

    def validate_headers(self, ingestor_name: str, file_cols: List[str]) -> None:
        """
        Valida se os headers do arquivo batem com o template.
        Lança ValueError se inválido.
        """
        # Find template file
        template_files = list(self.template_dir.glob(f"template_{ingestor_name}.*"))
        if not template_files:
            print(f"   ⚠️  Template não encontrado para {ingestor_name}. Pulando validação de headers.")
            return

        template_path = template_files[0]
        try:
            # Read template headers
            if template_path.suffix == '.csv':
                sep = FileHandler.detect_separator(template_path)
                tpl_df = pd.read_csv(template_path, sep=sep, nrows=0, encoding='utf-8')
            elif template_path.suffix == '.ods':
                tpl_df = pd.read_excel(template_path, engine='odf', nrows=0)
            else:
                tpl_df = pd.read_excel(template_path, nrows=0)
            
            expected_cols = list(tpl_df.columns)
            input_cols = list(file_cols)

            if expected_cols != input_cols:
                missing = set(expected_cols) - set(input_cols)
                extra = set(input_cols) - set(expected_cols)
                error_msg = f"Headers inválidos! \n   Esperado: {expected_cols}\n   Encontrado: {input_cols}"
                if missing: error_msg += f"\n   Faltando: {missing}"
                if extra: error_msg += f"\n   Sobrando: {extra}"
                raise ValueError(error_msg)
                
            print(f"   ✅ Headers validados com sucesso contra {template_path.name}")

        except Exception as e:
            raise ValueError(f"Erro na validação de template: {e}")
