"""
Este módulo, `FileHandler`, centraliza as operações de manipulação de arquivos
para o pipeline de ingestão. Ele é responsável por tarefas como o cálculo de
hash de arquivos, movimentação para diretórios de processados e detecção
automática de separadores em arquivos CSV.
"""

import hashlib
import shutil
from pathlib import Path
from datetime import datetime

class FileHandler:
    """
    Gerencia as operações de I/O de arquivos para o pipeline de ingestão.
    """
    
    def __init__(self, processed_dir: Path):
        """
        Inicializa o handler de arquivos.

        Args:
            processed_dir (Path): O diretório base para onde os arquivos 
                                  processados serão movidos.
        """
        self.processed_dir = processed_dir

    @staticmethod
    def calculate_hash(file_path: Path) -> str:
        """
        Calcula o hash MD5 de um arquivo de forma eficiente em termos de memória.

        A leitura é feita em blocos (chunks) para evitar carregar arquivos
        grandes inteiramente na memória.

        Args:
            file_path (Path): O caminho do arquivo.

        Returns:
            str: O hash MD5 hexadecimal do conteúdo do arquivo.
        """
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def move_to_processed(self, file_path: Path, is_duplicate: bool = False) -> Path:
        """
        Move um arquivo para o diretório de processados com uma estrutura organizada.

        - Arquivos normais são movidos para: `processed/YYYY/MM/DD/HHMMSS_nomeoriginal.ext`
        - Arquivos duplicados são movidos para: `processed/duplicates/HHMMSS_nomeoriginal.ext`

        Args:
            file_path (Path): O caminho do arquivo a ser movido.
            is_duplicate (bool): Flag que indica se o arquivo é uma duplicata.

        Returns:
            Path: O caminho de destino do arquivo movido.
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
        """
        Detecta o separador de um arquivo CSV analisando a primeira linha.

        A detecção segue a prioridade: ponto e vírgula (`;`), vírgula (`,`), e
        depois tabulação (`\t`). Se nenhum for encontrado, assume a vírgula como padrão.
        Tenta a leitura com encoding UTF-8 e fallback para Latin-1.

        Args:
            file_path (Path): O caminho do arquivo CSV.

        Returns:
            str: O separador detectado.
        """
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
