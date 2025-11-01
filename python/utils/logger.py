"""
Módulo: logger.py
Descrição: Configuração de logging para scripts ETL
"""

import logging
import os
from datetime import datetime
from pathlib import Path


def setup_logger(name: str, log_dir: str = None) -> logging.Logger:
    """
    Configura logger para scripts ETL
    
    Args:
        name: Nome do logger (geralmente nome do script)
        log_dir: Diretório para salvar logs (padrão: ./logs)
        
    Returns:
        Logger configurado
    """
    # Criar diretório de logs se não existir
    if log_dir is None:
        log_dir = Path(__file__).parent.parent.parent / 'logs'
    else:
        log_dir = Path(log_dir)
    
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Nome do arquivo de log com timestamp
    log_filename = f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
    log_path = log_dir / log_filename
    
    # Configurar logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Remover handlers existentes para evitar duplicação
    if logger.handlers:
        logger.handlers.clear()
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para arquivo
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


def log_dataframe_info(logger: logging.Logger, df, nome: str = "DataFrame"):
    """
    Loga informações sobre um DataFrame
    
    Args:
        logger: Logger configurado
        df: DataFrame pandas
        nome: Nome descritivo do DataFrame
    """
    logger.info(f"{nome} - Shape: {df.shape}")
    logger.info(f"{nome} - Colunas: {list(df.columns)}")
    logger.info(f"{nome} - Tipos: {df.dtypes.to_dict()}")
    logger.info(f"{nome} - Valores nulos: {df.isnull().sum().to_dict()}")
    logger.info(f"{nome} - Memória: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
