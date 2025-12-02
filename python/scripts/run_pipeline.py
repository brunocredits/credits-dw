import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from python.ingestors.ingest_base_oficial import IngestBaseOficial
from python.ingestors.ingest_faturamento import IngestFaturamento
from python.ingestors.ingest_usuarios import IngestUsuarios

def run_pipeline():
    print("="*50)
    print("🚀 INICIANDO PIPELINE DE INGESTÃO (DOCKERIZED)")
    print("="*50)
    
    start = time.time()
    
    IngestBaseOficial().run("base_oficial.csv")
    IngestFaturamento().run("faturamento.csv")
    IngestUsuarios().run("usuario.csv")
    
    print("="*50)
    print(f"✅ PIPELINE CONCLUÍDO EM {time.time() - start:.2f}s")
    print("="*50)

if __name__ == "__main__":
    run_pipeline()
