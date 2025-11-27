#!/usr/bin/env python3
"""
Módulo: run_silver_transformers.py
Descrição: Orquestra a execução das transformações da camada Silver.
"""
import sys
import time
from typing import List, Tuple
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Importar as classes de transformação refatoradas
from transformers.silver.transform_dim_data import TransformDimData
from transformers.silver.transform_dim_cliente import TransformDimCliente
from transformers.silver.transform_dim_usuario import TransformDimUsuario
from transformers.silver.transform_fato_faturamento import TransformFatoFaturamento
from utils.logger import setup_logger


def main() -> int:
    """Executa todas as transformações Silver em ordem de dependência."""
    logger = setup_logger('run_silver_transformers.py')

    # Ordem de dependências: dimensões sempre antes dos fatos
    transformers: List[Tuple[str, object]] = [
        ('dim_data', TransformDimData()),
        ('dim_cliente', TransformDimCliente()),
        ('dim_usuario', TransformDimUsuario()),
        ('fato_faturamento', TransformFatoFaturamento())
    ]

    logger.info("=" * 80)
    logger.info("🏁 [SILVER] INICIANDO EXECUÇÃO DAS TRANSFORMAÇÕES")
    logger.info(f"   - Transformadores a executar: {[nome for nome, _ in transformers]}")
    logger.info("=" * 80)

    sucessos = 0
    start_time = time.time()

    for nome, transformer in transformers:
        result = transformer.executar()
        if result == 0:
            sucessos += 1
        else:
            # Interrompe a execução se uma transformação falhar
            logger.error(f"❌ [SILVER][FALHA GERAL] A execução foi abortada porque o transformador '{nome}' falhou.")
            break
    
    duracao_total = time.time() - start_time
    falhas = len(transformers) - sucessos

    logger.info("=" * 80)
    logger.info("📊 [SILVER] RESUMO DA EXECUÇÃO")
    logger.info(f"   - Duração Total: {duracao_total:.1f}s | Transformadores: {len(transformers)} (Sucessos: {sucessos}, Falhas: {falhas})")
    logger.info("=" * 80)
    
    return 0 if falhas == 0 else 1


if __name__ == '__main__':
    sys.exit(main())