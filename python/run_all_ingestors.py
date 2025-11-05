import sys
from pathlib import Path

# Adicionar diretório raiz ao path para garantir que os módulos sejam encontrados
sys.path.append(str(Path(__file__).resolve().parent))

from ingestors.csv.ingest_contas_base_oficial import IngestContasBaseOficial
from ingestors.csv.ingest_data import IngestData
from ingestors.csv.ingest_faturamento import IngestFaturamento
from ingestors.csv.ingest_usuarios import IngestUsuarios

def run_all_csv_ingestors():
    """
    Executa todos os ingestores CSV disponíveis.
    """
    ingestors = [
        IngestContasBaseOficial(),
        IngestData(),
        IngestFaturamento(),
        IngestUsuarios()
    ]

    print("Iniciando a execução de todos os ingestores CSV...")
    for ingestor in ingestors:
        try:
            print(f"Executando {ingestor.script_name}...")
            ingestor.executar()
            print(f"{ingestor.script_name} concluído com sucesso.")
        except Exception as e:
            print(f"Erro ao executar {ingestor.script_name}: {e}")
    print("Execução de todos os ingestores CSV finalizada.")

if __name__ == '__main__':
    run_all_csv_ingestors()
