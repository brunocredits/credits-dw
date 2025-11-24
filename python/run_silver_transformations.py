#!/usr/bin/env python3
"""Executa transformações Silver em ordem de dependências"""
import sys
import time
from typing import List, Tuple
from transformers.silver.transform_dim_clientes import TransformDimClientes
from transformers.silver.transform_dim_usuarios import TransformDimUsuarios
from transformers.silver.transform_fact_faturamento import TransformFactFaturamento
from utils.logger import setup_logger

def main() -> int:
    """Executa todas transformações Silver em ordem"""
    logger = setup_logger('run_silver_transformations.py')

    # Ordem de dependências: dimensões antes de fatos
    transformers: List[Tuple[str, object]] = [
        ('dim_clientes', TransformDimClientes()),
        ('dim_usuarios', TransformDimUsuarios()),
        ('fact_faturamento', TransformFactFaturamento())
    ]

    logger.info("=" * 80)
    logger.info("🏁 Iniciando Transformações Silver")
    logger.info(f"📦 {len(transformers)} transformações")
    logger.info("=" * 80)

    sucessos, falhas = 0, 0
    start_time = time.time()

    for nome, transformer in transformers:
        logger.info(f"\n▶️ Executando {nome}...")
        result = transformer.executar()

        if result == 0:
            sucessos += 1
            logger.success(f"✅ {nome} concluído")
        else:
            falhas += 1
            logger.error(f"❌ {nome} falhou")
            if nome.startswith('dim_'):
                logger.error("⚠️ Dimensão falhou - parando execução")
                break

    duracao = time.time() - start_time
    logger.info("\n" + "=" * 80)
    logger.info(f"📊 Resumo: {sucessos} sucessos, {falhas} falhas")
    logger.info(f"⏱️ Duração total: {duracao:.1f}s")
    logger.info("=" * 80)

    return 0 if falhas == 0 else 1

if __name__ == '__main__':
    sys.exit(main())
