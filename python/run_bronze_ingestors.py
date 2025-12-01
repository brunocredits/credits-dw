#!/usr/bin/env python3
"""
Módulo: run_bronze_ingestors.py
Descrição: Orquestrador para execução dos ingestores da camada Bronze.
"""

import sys
import argparse
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple

# Adicionar diretório raiz ao path
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Importar as classes de ingestão refatoradas
from ingestors.csv.ingest_base_oficial import IngestBaseOficial
from ingestors.csv.ingest_faturamentos import IngestFaturamentos
from ingestors.csv.ingest_usuarios import IngestUsuarios
from ingestors.csv.ingest_calendario import IngestCalendario
from utils.logger import setup_logger
from utils.config import get_etl_config


# Mapeamento de nomes para classes de ingestores
INGESTORS_REGISTRY = {
    'calendario': IngestCalendario,
    'base_oficial': IngestBaseOficial,
    'faturamentos': IngestFaturamentos,
    'usuarios': IngestUsuarios
}


class OrquestradorBronze:
    """Orquestrador de execução de ingestores com métricas e paralelização."""

    def __init__(self):
        self.logger = setup_logger('run_bronze_ingestors.py')
        self.config = get_etl_config()
        self.resultados: List[Dict] = []

    def executar_ingestor(self, nome: str, ingestor_class) -> Dict:
        """
        Executa um ingestor e retorna métricas de execução.
        """
        start_time = time.time()
        self.logger.info(f"[BRONZE][INICIO] Executando ingestor: {nome}")
        ingestor = None
        try:
            ingestor = ingestor_class()
            exit_code = ingestor.executar()
            duracao = time.time() - start_time
            sucesso = exit_code == 0

            resultado = {
                'nome': nome,
                'script': ingestor.script_name,
                'tabela': ingestor.tabela_destino,
                'sucesso': sucesso,
                'duracao': duracao,
                'erro': None
            }

            if sucesso:
                self.logger.success(f"[BRONZE][SUCESSO] Ingestor '{nome}' concluído em {duracao:.1f}s.")
            else:
                self.logger.error(f"[BRONZE][FALHA] Ingestor '{nome}' falhou após {duracao:.1f}s.")
            return resultado

        except Exception as e:
            duracao = time.time() - start_time
            tabela_destino = ingestor.tabela_destino if ingestor else 'N/A'
            self.logger.error(f"[BRONZE][ERRO] Ingestor '{nome}' falhou com exceção: {e}", exc_info=True)
            return {
                'nome': nome,
                'script': ingestor_class.__name__,
                'tabela': tabela_destino,
                'sucesso': False,
                'duracao': duracao,
                'erro': str(e)
            }

    def executar_sequencial(self, ingestors: List[Tuple[str, type]]) -> List[Dict]:
        """Executa ingestores sequencialmente."""
        self.logger.info("[BRONZE][INFO] Modo de execução: Sequencial.")
        return [self.executar_ingestor(nome, cls) for nome, cls in ingestors]

    def executar_paralelo(self, ingestors: List[Tuple[str, type]], max_workers: int = None) -> List[Dict]:
        """Executa ingestores em paralelo."""
        if max_workers is None:
            max_workers = min(len(ingestors), self.config.parallel_ingestors or 3)
        self.logger.info(f"[BRONZE][INFO] Modo de execução: Paralelo ({max_workers} workers).")
        
        resultados = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.executar_ingestor, nome, cls): nome for nome, cls in ingestors}
            for future in as_completed(futures):
                resultado = future.result()
                resultados.append(resultado)
        return resultados

    def imprimir_resumo(self, resultados: List[Dict], duracao_total: float) -> None:
        """Imprime resumo consolidado da execução."""
        total = len(resultados)
        sucessos = sum(1 for r in resultados if r['sucesso'])
        falhas = total - sucessos
        taxa_sucesso = (sucessos / total * 100) if total > 0 else 0

        self.logger.info("=" * 80)
        self.logger.info("📊 [BRONZE] RESUMO DA EXECUÇÃO")
        self.logger.info(f"   - Duração Total: {duracao_total:.1f}s | Ingestores: {total} (Sucessos: {sucessos}, Falhas: {falhas})")
        self.logger.info("-" * 80)

        for resultado in sorted(resultados, key=lambda x: x['nome']):
            status = "✅ SUCESSO" if resultado['sucesso'] else "❌ FALHOU"
            log_func = self.logger.info if resultado['sucesso'] else self.logger.error
            log_func(
                f"   - {resultado['nome']:20s} | {status:12s} | "
                f"Duração: {resultado['duracao']:5.1f}s | Tabela: {resultado['tabela']}"
            )
            if resultado['erro']:
                log_func(f"     ↳ Erro: {resultado['erro']}")
        
        if falhas > 0:
            self.logger.warning(f"\n⚠️  ATENÇÃO: {falhas} ingestor(es) falharam. Verifique os logs individuais para detalhes.")
        self.logger.info("=" * 80)

    def executar(self, ingestor_names: List[str] = None, paralelo: bool = False, max_workers: int = None) -> int:
        """Executa o pipeline de ingestores."""
        start_time = time.time()

        # Prioridade para calendário
        always_first = ['calendario']
        
        if ingestor_names:
            names = [n for n in ingestor_names if n in INGESTORS_REGISTRY]
        else:
            names = list(INGESTORS_REGISTRY.keys())
            
        # Reordenar para garantir que calendario rode primeiro
        ordered_names = [n for n in always_first if n in names] + [n for n in names if n not in always_first]
        
        ingestors = [(nome, INGESTORS_REGISTRY[nome]) for nome in ordered_names]

        if not ingestors:
            self.logger.error("[BRONZE][FALHA] Nenhum ingestor válido para executar.")
            return 1

        self.logger.info("=" * 80)
        self.logger.info("🏁 [BRONZE] INICIANDO EXECUÇÃO DE INGESTORES")
        self.logger.info(f"   - Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"   - Ingestores a executar: {', '.join([n for n, _ in ingestors])}")
        self.logger.info("=" * 80)

        # Se calendário estiver na lista, executá-lo isoladamente primeiro para garantir dependência
        # (embora aqui não haja chave estrangeira forte, é boa prática conforme pedido)
        final_results = []
        
        if 'calendario' in ordered_names:
            cal_idx = next(i for i, (n, _) in enumerate(ingestors) if n == 'calendario')
            cal_ingestor = ingestors.pop(cal_idx)
            self.logger.info("[BRONZE] Executando Calendário primeiro (Prioridade)...")
            final_results.append(self.executar_ingestor(*cal_ingestor))
            if not final_results[0]['sucesso']:
                 self.logger.error("[BRONZE] Falha na geração do calendário. Abortando pipeline?")
                 # Opcional: abortar. Mas vou seguir.

        # Restante
        if ingestors:
            results = self.executar_paralelo(ingestors, max_workers) if paralelo else self.executar_sequencial(ingestors)
            final_results.extend(results)
        
        duracao_total = time.time() - start_time
        self.imprimir_resumo(final_results, duracao_total)
        
        return 0 if all(r['sucesso'] for r in final_results) else 1


def parse_args():
    """Analisa os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(description='Executa ingestores CSV para a camada Bronze.')
    parser.add_argument('--scripts', '-s', nargs='+', choices=list(INGESTORS_REGISTRY.keys()), help='Nomes dos ingestores a executar.')
    parser.add_argument('--parallel', '-p', action='store_true', help='Executa ingestores em paralelo.')
    parser.add_argument('--workers', '-w', type=int, default=None, help='Número de workers para execução paralela.')
    parser.add_argument('--list', '-l', action='store_true', help='Lista os ingestores disponíveis e sai.')
    return parser.parse_args()


def main():
    """Função principal para execução via linha de comando."""
    args = parse_args()

    if args.list:
        print("\n📋 Ingestores Disponíveis:")
        for nome, ingestor_class in INGESTORS_REGISTRY.items():
            instancia = ingestor_class()
            print(f"  • {nome:20s} → {instancia.tabela_destino}")
        print()
        return 0

    orquestrador = OrquestradorBronze()
    try:
        return orquestrador.executar(ingestor_names=args.scripts, paralelo=args.parallel, max_workers=args.workers)
    except KeyboardInterrupt:
        orquestrador.logger.warning("\n[BRONZE][AVISO] Execução interrompida pelo usuário.")
        return 1
    except Exception as e:
        orquestrador.logger.error(f"\n[BRONZE][ERRO] Erro fatal no orquestrador: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())