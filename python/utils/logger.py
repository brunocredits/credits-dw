"""
Este módulo, `logger`, configura um sistema de logging robusto e flexível
para o projeto usando a biblioteca Loguru. Ele padroniza o formato dos logs,
habilita a rotação automática de arquivos e permite a configuração de
diferentes níveis de log para o console e para os arquivos.
"""

import sys
import os
from pathlib import Path
from loguru import logger

def setup_logger(name: str, log_dir: str = None, rotation: str = "100 MB", retention: str = "30 days") -> "logger":
    """
    Configura e retorna um logger Loguru para um script ou módulo específico.

    A configuração inclui:
    - Um handler para o console com nível de log 'INFO' (ou definido por variável de ambiente).
    - Um handler para arquivo com rotação, compressão e retenção automáticas.
    - Um formato de log padronizado e colorido para fácil leitura.

    Args:
        name (str): O nome do logger, geralmente o nome do script (`__name__`).
        log_dir (str, optional): Diretório para salvar os arquivos de log.
                                 Se não for fornecido, detecta o ambiente (local vs. Docker).
        rotation (str): O critério para rotacionar os arquivos de log (ex: "100 MB", "1 week").
        retention (str): O tempo para manter os arquivos de log antigos (ex: "30 days").

    Returns:
        logger: A instância do logger Loguru configurada.
    """
    # Remove a configuração padrão do Loguru para evitar duplicatas
    logger.remove()

    # Determina o diretório de logs, detectando o ambiente
    if log_dir is None:
        log_dir = Path('/app/logs') if Path('/app').exists() else Path(__file__).parent.parent.parent / 'logs'
    else:
        log_dir = Path(log_dir)

    log_dir.mkdir(parents=True, exist_ok=True)

    # Formato de log customizado para clareza e consistência
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )

    # Handler para o console (saída padrão)
    logger.add(
        sys.stdout,
        format=log_format,
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        colorize=True,
        backtrace=True,  # Mostra o stack trace completo em exceções
        diagnose=True    # Adiciona informações de diagnóstico em exceções
    )

    # Handler para o arquivo de log com rotação
    log_file = log_dir / f"{name}.log"
    logger.add(
        log_file,
        format=log_format,
        level="DEBUG",  # Nível mais detalhado para o arquivo
        rotation=rotation,
        retention=retention,
        compression="zip",  # Comprime os arquivos de log antigos
        backtrace=True,
        diagnose=True,
        encoding="utf-8"
    )

    logger.info(f"Logger '{name}' configurado. Logs serão salvos em: {log_file}")
    return logger

def log_dataframe_info(df, nome: str = "DataFrame", logger_obj=None) -> None:
    """
    Registra um resumo informativo sobre um DataFrame do pandas.

    Args:
        df (pd.DataFrame): O DataFrame a ser analisado.
        nome (str): Um nome descritivo para o DataFrame no log.
        logger_obj (logger, optional): A instância do logger a ser usada.
    """
    log = logger_obj or logger
    log.info(f"📊 Informações do DataFrame '{nome}':")
    log.info(f"   - Dimensões: {df.shape[0]:,} linhas x {df.shape[1]} colunas")
    log.info(f"   - Uso de Memória: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    nulls = df.isnull().sum()
    if nulls.sum() > 0:
        log.warning("   - Valores Nulos Encontrados:")
        for col, count in nulls[nulls > 0].items():
            log.warning(f"     - '{col}': {count:,} nulos ({count/len(df)*100:.1f}%)")
    else:
        log.success("   - Nenhum valor nulo encontrado.")
