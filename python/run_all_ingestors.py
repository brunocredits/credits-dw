#!/usr/bin/env python3
"""
Script: run_all_ingestors.py
Descrição: Executa todos os ingestores CSV com suporte a paralelização e métricas
Versão: 2.0

Uso:
    python run_all_ingestors.py                    # Executa todos sequencialmente
    python run_all_ingestors.py --parallel         # Executa em paralelo
    python run_all_ingestors.py --scripts faturamento usuarios  # Executa apenas alguns
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

from ingestors.csv.ingest_contas_base_oficial import IngestContasBaseOficial
from ingestors.csv.ingest_faturamento import IngestFaturamento
from ingestors.csv.ingest_usuarios import IngestUsuarios
from utils.logger import setup_logger
from utils.config import get_etl_config


# Mapeamento de nomes para classes de ingestores
INGESTORS_REGISTRY = {
    'contas': IngestContasBaseOficial,
    'faturamento': IngestFaturamento,
    'usuarios': IngestUsuarios
}


class IngestorOrchestrator:
    """Orquestrador de execução de ingestores com métricas e paralelização"""

    def __init__(self):
        self.logger = setup_logger('run_all_ingestors.py')
        self.config = get_etl_config()
        self.resultados: List[Dict] = []

    def executar_ingestor(self, nome: str, ingestor_class) -> Dict:
        """
        Executa um ingestor e retorna métricas de execução.

        Args:
            nome: Nome identificador do ingestor
            ingestor_class: Classe do ingestor

        Returns:
            Dict com métricas de execução
        """
        start_time = time.time()

        try:
            self.logger.info(f"▶️  Iniciando ingestor: {nome}")

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
                self.logger.success(f"✅ {nome} concluído em {duracao:.1f}s")
            else:
                self.logger.error(f"❌ {nome} falhou após {duracao:.1f}s")

            return resultado

        except Exception as e:
            duracao = time.time() - start_time
            self.logger.error(f"❌ {nome} falhou com exceção: {str(e)}")

            return {
                'nome': nome,
                'script': ingestor_class.__name__,
                'tabela': 'N/A',
                'sucesso': False,
                'duracao': duracao,
                'erro': str(e)
            }

    def executar_sequencial(self, ingestors: List[Tuple[str, type]]) -> List[Dict]:
        """
        Executa ingestores sequencialmente.

        Args:
            ingestors: Lista de tuplas (nome, classe_ingestor)

        Returns:
            Lista de resultados
        """
        self.logger.info("📋 Modo de execução: SEQUENCIAL")
        resultados = []

        for nome, ingestor_class in ingestors:
            resultado = self.executar_ingestor(nome, ingestor_class)
            resultados.append(resultado)

        return resultados

    def executar_paralelo(self, ingestors: List[Tuple[str, type]], max_workers: int = None) -> List[Dict]:
        """
        Executa ingestores em paralelo usando ThreadPoolExecutor.

        Args:
            ingestors: Lista de tuplas (nome, classe_ingestor)
            max_workers: Número máximo de workers (None = automático)

        Returns:
            Lista de resultados
        """
        if max_workers is None:
            max_workers = min(len(ingestors), self.config.parallel_ingestors or 3)

        self.logger.info(f"🚀 Modo de execução: PARALELO ({max_workers} workers)")
        resultados = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submeter todos os ingestores
            futures = {
                executor.submit(self.executar_ingestor, nome, ingestor_class): (nome, ingestor_class)
                for nome, ingestor_class in ingestors
            }

            # Coletar resultados conforme completam
            for future in as_completed(futures):
                resultado = future.result()
                resultados.append(resultado)

        return resultados

    def imprimir_resumo(self, resultados: List[Dict], duracao_total: float) -> None:
        """
        Imprime resumo consolidado da execução.

        Args:
            resultados: Lista de resultados dos ingestores
            duracao_total: Duração total da execução em segundos
        """
        total = len(resultados)
        sucessos = sum(1 for r in resultados if r['sucesso'])
        falhas = total - sucessos
        taxa_sucesso = (sucessos / total * 100) if total > 0 else 0

        self.logger.info("=" * 80)
        self.logger.info("📊 RESUMO DA EXECUÇÃO")
        self.logger.info("=" * 80)

        # Estatísticas gerais
        self.logger.info(f"\n📈 Estatísticas Gerais:")
        self.logger.info(f"   • Total de ingestores: {total}")
        self.logger.info(f"   • Sucessos: {sucessos} ({taxa_sucesso:.1f}%)")
        self.logger.info(f"   • Falhas: {falhas}")
        self.logger.info(f"   • Duração total: {duracao_total:.1f}s")

        # Detalhes por ingestor
        self.logger.info(f"\n📋 Detalhes por Ingestor:")
        for resultado in sorted(resultados, key=lambda x: x['nome']):
            status = "✅ SUCESSO" if resultado['sucesso'] else "❌ FALHOU"
            self.logger.info(
                f"   • {resultado['nome']:20s} | {status:12s} | "
                f"{resultado['duracao']:6.1f}s | {resultado['tabela']}"
            )

            if resultado['erro']:
                self.logger.error(f"     ↳ Erro: {resultado['erro']}")

        # Alertas
        if falhas > 0:
            self.logger.error(f"\n⚠️  ATENÇÃO: {falhas} ingestor(es) falharam!")
            self.logger.error(f"   Verifique os logs para mais detalhes.")

        self.logger.info("=" * 80)

    def executar(
        self,
        ingestor_names: List[str] = None,
        paralelo: bool = False,
        max_workers: int = None
    ) -> int:
        """
        Executa pipeline de ingestores com opções configuráveis.

        Args:
            ingestor_names: Lista de nomes de ingestores (None = todos)
            paralelo: Se deve executar em paralelo
            max_workers: Número de workers paralelos (None = automático)

        Returns:
            Código de saída (0 = sucesso, 1 = falhas parciais ou totais)
        """
        start_time = time.time()

        # Determinar quais ingestores executar
        if ingestor_names:
            ingestors = []
            for nome in ingestor_names:
                if nome in INGESTORS_REGISTRY:
                    ingestors.append((nome, INGESTORS_REGISTRY[nome]))
                else:
                    self.logger.warning(f"⚠️  Ingestor '{nome}' não encontrado. Ignorando.")
        else:
            ingestors = list(INGESTORS_REGISTRY.items())

        if not ingestors:
            self.logger.error("❌ Nenhum ingestor válido para executar")
            return 1

        # Header
        self.logger.info("=" * 80)
        self.logger.info("🏁 INICIANDO EXECUÇÃO DE INGESTORES CSV")
        self.logger.info(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"📦 Ingestores selecionados: {len(ingestors)}")
        self.logger.info("=" * 80)

        # Executar
        if paralelo:
            resultados = self.executar_paralelo(ingestors, max_workers)
        else:
            resultados = self.executar_sequencial(ingestors)

        # Resumo
        duracao_total = time.time() - start_time
        self.imprimir_resumo(resultados, duracao_total)

        # Retornar código de saída
        falhas = sum(1 for r in resultados if not r['sucesso'])
        return 0 if falhas == 0 else 1


def parse_args():
    """Parse argumentos da linha de comando"""
    parser = argparse.ArgumentParser(
        description='Executa ingestores CSV para o Data Warehouse',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  %(prog)s                              # Executa todos sequencialmente
  %(prog)s --parallel                    # Executa todos em paralelo
  %(prog)s --scripts faturamento usuarios  # Executa apenas alguns
  %(prog)s --list                        # Lista ingestores disponíveis
        """
    )

    parser.add_argument(
        '--scripts', '-s',
        nargs='+',
        choices=list(INGESTORS_REGISTRY.keys()),
        help='Ingestores específicos para executar'
    )

    parser.add_argument(
        '--parallel', '-p',
        action='store_true',
        help='Executa ingestores em paralelo'
    )

    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=None,
        help='Número de workers paralelos (padrão: 3)'
    )

    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='Lista ingestores disponíveis e sai'
    )

    return parser.parse_args()


def main():
    """Função principal"""
    args = parse_args()

    # Listar ingestores disponíveis
    if args.list:
        print("\n📋 Ingestores Disponíveis:")
        for nome, ingestor_class in INGESTORS_REGISTRY.items():
            ingestor = ingestor_class()
            print(f"  • {nome:20s} → {ingestor.tabela_destino}")
        print()
        return 0

    # Executar ingestores
    orchestrator = IngestorOrchestrator()

    try:
        exit_code = orchestrator.executar(
            ingestor_names=args.scripts,
            paralelo=args.parallel,
            max_workers=args.workers
        )
        return exit_code

    except KeyboardInterrupt:
        orchestrator.logger.warning("\n⚠️  Execução interrompida pelo usuário")
        return 1

    except Exception as e:
        orchestrator.logger.error(f"\n❌ Erro fatal na execução: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
