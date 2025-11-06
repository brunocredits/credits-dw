"""
Módulo: logger.py
Descrição: Configuração de logging usando Loguru para scripts ETL
Versão: 2.0
"""

import sys
import os
from pathlib import Path
from loguru import logger


def setup_logger(name: str, log_dir: str = None, rotation: str = "100 MB", retention: str = "30 days") -> "logger":
    """
    Configura logger usando Loguru para scripts ETL com rotação automática.

    Args:
        name: Nome do logger (geralmente nome do script)
        log_dir: Diretório para salvar logs (padrão: /app/logs)
        rotation: Tamanho máximo do arquivo antes de rotacionar (padrão: 100 MB)
        retention: Tempo de retenção dos logs antigos (padrão: 30 dias)

    Returns:
        Logger Loguru configurado
    """
    # Remover configuração padrão do loguru
    logger.remove()

    # Determinar diretório de logs
    if log_dir is None:
        # Usa /app/logs se estiver no container, senão usa ./logs
        log_dir = Path('/app/logs') if Path('/app').exists() else Path(__file__).parent.parent.parent / 'logs'
    else:
        log_dir = Path(log_dir)

    log_dir.mkdir(parents=True, exist_ok=True)

    # Formato customizado para os logs
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )

    # Handler para console (apenas INFO+)
    logger.add(
        sys.stdout,
        format=log_format,
        level=os.getenv("LOG_LEVEL", "INFO"),
        colorize=True,
        backtrace=True,
        diagnose=True
    )

    # Handler para arquivo com rotação (DEBUG+)
    log_file = log_dir / f"{name}.log"
    logger.add(
        log_file,
        format=log_format,
        level="DEBUG",
        rotation=rotation,
        retention=retention,
        compression="zip",
        backtrace=True,
        diagnose=True,
        encoding="utf-8"
    )

    # Log de inicialização
    logger.info(f"Logger '{name}' configurado | Logs: {log_file}")

    return logger


def log_dataframe_info(df, nome: str = "DataFrame") -> None:
    """
    Loga informações detalhadas sobre um DataFrame.

    Args:
        df: DataFrame pandas
        nome: Nome descritivo do DataFrame
    """
    logger.info(f"📊 {nome} - Informações:")
    logger.info(f"   • Shape: {df.shape[0]:,} linhas x {df.shape[1]} colunas")
    logger.info(f"   • Colunas: {list(df.columns)}")
    logger.info(f"   • Memória: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    # Valores nulos
    nulls = df.isnull().sum()
    if nulls.sum() > 0:
        logger.warning(f"   • Valores nulos detectados:")
        for col, count in nulls[nulls > 0].items():
            logger.warning(f"     - {col}: {count:,} ({count/len(df)*100:.1f}%)")
    else:
        logger.success(f"   • Sem valores nulos ✓")

    # Tipos de dados
    logger.debug(f"   • Tipos de dados: {df.dtypes.to_dict()}")


def log_execution_summary(
    script_name: str,
    status: str,
    linhas_processadas: int = 0,
    linhas_inseridas: int = 0,
    duracao_segundos: float = 0
) -> None:
    """
    Loga resumo de execução de um script ETL.

    Args:
        script_name: Nome do script
        status: Status da execução (sucesso/erro)
        linhas_processadas: Total de linhas processadas
        linhas_inseridas: Total de linhas inseridas
        duracao_segundos: Duração da execução em segundos
    """
    logger.info("=" * 80)

    if status.lower() == 'sucesso':
        logger.success(f"✅ {script_name} - EXECUÇÃO CONCLUÍDA COM SUCESSO")
    else:
        logger.error(f"❌ {script_name} - EXECUÇÃO FALHOU")

    logger.info(f"   • Linhas processadas: {linhas_processadas:,}")
    logger.info(f"   • Linhas inseridas: {linhas_inseridas:,}")

    if duracao_segundos > 0:
        minutos, segundos = divmod(duracao_segundos, 60)
        if minutos > 0:
            logger.info(f"   • Duração: {int(minutos)}m {segundos:.1f}s")
        else:
            logger.info(f"   • Duração: {duracao_segundos:.2f}s")

        # Calcular throughput
        if linhas_processadas > 0 and duracao_segundos > 0:
            throughput = linhas_processadas / duracao_segundos
            logger.info(f"   • Throughput: {throughput:.0f} linhas/segundo")

    logger.info("=" * 80)
