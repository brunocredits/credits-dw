"""
FileHandler - Gerenciamento de arquivos
Responsável por hash, movimentação e detecção de separadores
"""

import hashlib
import shutil
from pathlib import Path
from datetime import datetime

class FileHandler:
    """Gerencia operações de arquivo do pipeline"""
    
    def __init__(self, processed_dir: Path):
        self.processed_dir = processed_dir

    @staticmethod
    def calculate_hash(file_path: Path) -> str:
        """Calcula MD5 do arquivo em chunks (eficiente em memória)"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def move_to_processed(self, file_path: Path, is_duplicate: bool = False) -> Path:
        """
        Move arquivo para pasta processados
        - Normal: processed/YYYY/MM/DD/HHMMSS_arquivo.csv
        - Duplicado: processed/duplicates/HHMMSS_arquivo.csv
        """
        today = datetime.now()
        timestamp = today.strftime("%H%M%S")
        
        if is_duplicate:
            dest_dir = self.processed_dir / "duplicates"
        else:
            dest_dir = self.processed_dir / today.strftime("%Y") / today.strftime("%m") / today.strftime("%d")
            
        dest_dir.mkdir(parents=True, exist_ok=True)
        new_filename = f"{timestamp}_{file_path.name}"
        dest = dest_dir / new_filename
        
        shutil.move(str(file_path), str(dest))
        return dest

    @staticmethod
    def detect_separator(file_path: Path) -> str:
        """Detecta separador CSV (prioridade: ; , tab)"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline()
        except UnicodeDecodeError:
            with open(file_path, 'r', encoding='latin-1') as f:
                first_line = f.readline()
        
        if ';' in first_line: return ';'
        if ',' in first_line: return ','
        if '\t' in first_line: return '\t'
        return ','
