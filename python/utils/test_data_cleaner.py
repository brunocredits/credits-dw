"""
Script para limpar dados de teste do banco de dados.

Uso:
    # Limpar apenas tabelas Bronze
    python python/utils/test_data_cleaner.py --layer bronze

    # Limpar apenas tabelas Silver
    python python/utils/test_data_cleaner.py --layer silver

    # Limpar todas as tabelas (Bronze + Silver)
    python python/utils/test_data_cleaner.py --all

    # Limpar apenas histórico de execuções de teste
    python python/utils/test_data_cleaner.py --audit
"""

import argparse
import logging
from typing import List
import psycopg2
from psycopg2.extensions import connection as Connection

from utils.db_connection import get_db_connection


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestDataCleaner:
    """Classe para limpar dados de teste do banco de dados."""

    def __init__(self, conn: Connection):
        self.conn = conn

    def limpar_bronze(self) -> None:
        """Limpa todas as tabelas Bronze usando TRUNCATE."""
        tabelas_bronze = [
            'bronze.contas_base_oficial',
            'bronze.usuarios',
            'bronze.faturamento',
            'bronze.data'
        ]

        logger.info("Limpando tabelas Bronze...")

        with self.conn.cursor() as cur:
            for tabela in tabelas_bronze:
                try:
                    cur.execute(f"TRUNCATE TABLE {tabela} RESTART IDENTITY CASCADE;")
                    logger.info(f"  ✓ {tabela} - truncada")
                except Exception as e:
                    logger.error(f"  ✗ {tabela} - erro: {e}")
                    raise

        self.conn.commit()
        logger.info("Tabelas Bronze limpas com sucesso!")

    def limpar_silver(self) -> None:
        """Limpa todas as tabelas Silver respeitando ordem de FKs."""
        # Ordem: Facts primeiro, depois Dimensions (respeitando FKs)
        tabelas_silver = [
            'silver.fact_faturamento',  # Fact table primeiro
            'silver.dim_usuarios',      # Tem FK para si mesma
            'silver.dim_clientes',
            'silver.dim_canal',
            'silver.dim_tempo'
        ]

        logger.info("Limpando tabelas Silver...")

        with self.conn.cursor() as cur:
            for tabela in tabelas_silver:
                try:
                    cur.execute(f"TRUNCATE TABLE {tabela} RESTART IDENTITY CASCADE;")
                    logger.info(f"  ✓ {tabela} - truncada")
                except Exception as e:
                    logger.error(f"  ✗ {tabela} - erro: {e}")
                    raise

        self.conn.commit()
        logger.info("Tabelas Silver limpas com sucesso!")

    def limpar_audit_testes(self) -> None:
        """Remove registros de audit relacionados a execuções de teste."""
        logger.info("Limpando registros de audit de testes...")

        with self.conn.cursor() as cur:
            # Remove execuções com 'test_' no nome do script
            cur.execute("""
                DELETE FROM credits.historico_atualizacoes
                WHERE script_nome LIKE 'test_%'
                   OR script_nome LIKE '%_test.py'
                   OR script_nome LIKE 'pytest%';
            """)

            registros_removidos = cur.rowcount
            logger.info(f"  ✓ {registros_removidos} registros de audit removidos")

            # Remove registros do silver_control de testes
            cur.execute("""
                DELETE FROM credits.silver_control
                WHERE tabela_nome LIKE '%test%';
            """)

            registros_silver = cur.rowcount
            logger.info(f"  ✓ {registros_silver} registros de silver_control removidos")

        self.conn.commit()
        logger.info("Registros de audit de testes limpos com sucesso!")

    def limpar_tudo(self) -> None:
        """Limpa todos os dados (Bronze, Silver e Audit)."""
        logger.info("=" * 60)
        logger.info("LIMPEZA COMPLETA - Removendo todos os dados de teste")
        logger.info("=" * 60)

        self.limpar_silver()
        self.limpar_bronze()
        self.limpar_audit_testes()

        logger.info("=" * 60)
        logger.info("Limpeza completa finalizada!")
        logger.info("=" * 60)

    def verificar_contagens(self) -> None:
        """Verifica e exibe contagem de registros em todas as tabelas."""
        logger.info("\n" + "=" * 60)
        logger.info("CONTAGENS ATUAIS")
        logger.info("=" * 60)

        tabelas = [
            # Bronze
            'bronze.contas_base_oficial',
            'bronze.usuarios',
            'bronze.faturamento',
            'bronze.data',
            # Silver
            'silver.dim_tempo',
            'silver.dim_clientes',
            'silver.dim_usuarios',
            'silver.dim_canal',
            'silver.fact_faturamento',
            # Audit
            'credits.historico_atualizacoes',
            'credits.silver_control'
        ]

        with self.conn.cursor() as cur:
            for tabela in tabelas:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {tabela};")
                    count = cur.fetchone()[0]
                    logger.info(f"  {tabela:40} {count:>10} registros")
                except Exception as e:
                    logger.error(f"  {tabela:40} ERRO: {e}")

        logger.info("=" * 60 + "\n")


def main():
    """Função principal para executar limpeza via CLI."""
    parser = argparse.ArgumentParser(
        description='Limpar dados de teste do banco de dados Credits DW'
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--bronze',
        action='store_true',
        help='Limpar apenas tabelas Bronze'
    )
    group.add_argument(
        '--silver',
        action='store_true',
        help='Limpar apenas tabelas Silver'
    )
    group.add_argument(
        '--audit',
        action='store_true',
        help='Limpar apenas registros de audit de testes'
    )
    group.add_argument(
        '--all',
        action='store_true',
        help='Limpar todas as tabelas (Bronze + Silver + Audit)'
    )

    parser.add_argument(
        '--verify',
        action='store_true',
        help='Verificar contagens após limpeza'
    )

    args = parser.parse_args()

    # Conectar ao banco
    conn = get_db_connection()
    cleaner = TestDataCleaner(conn)

    try:
        # Executar limpeza conforme opção escolhida
        if args.bronze:
            cleaner.limpar_bronze()
        elif args.silver:
            cleaner.limpar_silver()
        elif args.audit:
            cleaner.limpar_audit_testes()
        elif args.all:
            cleaner.limpar_tudo()

        # Verificar contagens se solicitado
        if args.verify:
            cleaner.verificar_contagens()

    except Exception as e:
        logger.error(f"Erro durante limpeza: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("Conexão com banco encerrada.")


if __name__ == '__main__':
    main()
