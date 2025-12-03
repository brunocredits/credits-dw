import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from python.ingestors.ingest_base_oficial import IngestBaseOficial
from python.ingestors.ingest_faturamento import IngestFaturamento
from python.ingestors.ingest_usuarios import IngestUsuarios

# Mapeamento de ingestores com auto-discovery
INGESTOR_MAPPING = {
    'base_oficial': IngestBaseOficial,
    'faturamento': IngestFaturamento,
    'usuario': IngestUsuarios,
    'usuarios': IngestUsuarios,  # Aceita ambos dos nomes
}

def auto_discover_files():
    """
   Descoberta automatica dos arquvos em input e faz o match nos ingestores
    """
    input_dir = Path("docker/data/input")
    files = list(input_dir.glob('*.csv')) + list(input_dir.glob('*.xlsx'))
    
    discovered = []
    for file in files:
        file_stem = file.stem.lower()
        for pattern, ingestor_class in INGESTOR_MAPPING.items():
            if pattern in file_stem:
                discovered.append((file.name, ingestor_class()))
                break
    
    return discovered

def run_pipeline():
    print("="*60)
    print("🚀 PIPELINE DE INGESTÃO BRONZE (RAW-FIRST)")
    print("="*60)
    
    start = time.time()
    
    # Auto-discover and run
    discovered = auto_discover_files()
    
    if not discovered:
        print("⚠️  Nenhum arquivo encontrado para processar")
        return
    
    print(f"📋 Arquivos detectados: {len(discovered)}")
    print()
    
    for filename, ingestor in discovered:
        ingestor.run(filename)
    
    duration = time.time() - start
    print()
    print("="*60)
    print(f"✅ PIPELINE CONCLUÍDO EM {duration:.2f}s")
    print("="*60)

if __name__ == "__main__":
    run_pipeline()
