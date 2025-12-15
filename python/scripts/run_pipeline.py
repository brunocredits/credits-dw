"""
Este script é o orquestrador principal do pipeline de ingestão de dados para a
camada Bronze. Ele automatiza a descoberta de arquivos no diretório de entrada
e aciona o ingestor correspondente para cada arquivo encontrado.
"""
import sys
import time
from pathlib import Path

# Adiciona o diretório raiz do projeto ao path do sistema para permitir
# importações de outros módulos do projeto.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from python.ingestors.ingest_base_oficial import IngestBaseOficial
from python.ingestors.ingest_faturamento import IngestFaturamento
from python.ingestors.ingest_usuarios import IngestUsuarios

# Mapeia padrões de nomes de arquivo para suas respectivas classes de ingestor.
# Isso permite que o pipeline descubra automaticamente qual ingestor usar
# com base no nome do arquivo.
INGESTOR_MAPPING = {
    'base_oficial': IngestBaseOficial,
    'faturamento': IngestFaturamento,
    'usuarios': IngestUsuarios,
}

def auto_discover_files():
    """
    Busca por arquivos CSV e XLSX no diretório de entrada e os associa a um
    ingestor com base no mapeamento `INGESTOR_MAPPING`.

    Returns:
        list: Uma lista de tuplas, onde cada tupla contém o nome do arquivo
              e uma instância da classe de ingestor correspondente.
    """
    input_dir = Path("docker/data/input")
    files = list(input_dir.glob('*.csv')) + list(input_dir.glob('*.xlsx'))
    
    discovered = []
    for file in files:
        file_stem = file.stem.lower()  # Usa o nome do arquivo sem extensão, em minúsculas
        for pattern, ingestor_class in INGESTOR_MAPPING.items():
            if pattern in file_stem:
                discovered.append((file.name, ingestor_class()))
                break  # Para no primeiro match encontrado
    
    return discovered

def run_pipeline():
    """
    Executa o pipeline de ingestão completo.

    Esta função orquestra todo o processo:
    1. Imprime um cabeçalho inicial.
    2. Descobre automaticamente os arquivos e seus respectivos ingestores.
    3. Itera sobre os arquivos descobertos e executa cada ingestor.
    4. Mede e imprime o tempo total de execução do pipeline.
    """
    print("="*60)
    print("🚀 PIPELINE DE INGESTÃO BRONZE (RAW-FIRST)")
    print("="*60)
    
    start = time.time()
    
    # Descoberta automática de arquivos
    discovered_files = auto_discover_files()
    
    if not discovered_files:
        print("⚠️  Nenhum arquivo encontrado no diretório de entrada para processar.")
        return
    
    print(f"📋 Arquivos detectados para processamento: {len(discovered_files)}")
    print()
    
    # Execução dos ingestores para cada arquivo encontrado
    for filename, ingestor in discovered_files:
        ingestor.run(filename)
    
    duration = time.time() - start
    print()
    print("="*60)
    print(f"✅ PIPELINE CONCLUÍDO EM {duration:.2f}s")
    print("="*60)

if __name__ == "__main__":
    # Ponto de entrada para a execução do script
    run_pipeline()
