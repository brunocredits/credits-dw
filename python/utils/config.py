"""
Módulo: config.py
Descrição: Configuração centralizada do projeto
Versão: 1.0
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Configurações de banco de dados"""
    host: str
    port: int
    database: str
    user: str
    password: str
    connect_timeout: int = 10
    pool_min_size: int = 1
    pool_max_size: int = 10

    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Cria configuração a partir de variáveis de ambiente"""
        required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        missing = [var for var in required_vars if not os.getenv(var)]

        if missing:
            raise ValueError(
                f"❌ Variáveis de ambiente obrigatórias não definidas: {', '.join(missing)}\n"
                f"   Configure o arquivo .env com base no .env.example"
            )

        return cls(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT', 5432)),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            connect_timeout=int(os.getenv('DB_CONNECT_TIMEOUT', 10)),
            pool_min_size=int(os.getenv('DB_POOL_MIN_SIZE', 1)),
            pool_max_size=int(os.getenv('DB_POOL_MAX_SIZE', 10))
        )


@dataclass
class CSVConfig:
    """Configurações padrão para ingestão de CSV"""
    separator: str = ';'
    encoding: str = 'utf-8-sig'
    decimal: str = ','
    thousands: str = '.'
    date_format: str = '%Y-%m-%d'
    chunk_size: int = 10000
    na_values: list = None

    def __post_init__(self):
        if self.na_values is None:
            self.na_values = ['', 'NULL', 'null', 'NA', 'N/A', '#N/A']

    @classmethod
    def from_env(cls) -> 'CSVConfig':
        """Cria configuração a partir de variáveis de ambiente"""
        return cls(
            separator=os.getenv('CSV_SEPARATOR', ';'),
            encoding=os.getenv('CSV_ENCODING', 'utf-8-sig'),
            chunk_size=int(os.getenv('CSV_CHUNK_SIZE', 10000))
        )


@dataclass
class PathsConfig:
    """Configurações de caminhos do projeto"""
    base_dir: Path
    data_input_dir: Path
    data_processed_dir: Path
    data_templates_dir: Path
    logs_dir: Path

    @classmethod
    def from_env(cls) -> 'PathsConfig':
        """Cria configuração de caminhos baseada no ambiente"""
        # Detecta se está no container Docker ou ambiente local
        if Path('/app').exists():
            base_dir = Path('/app')
        else:
            base_dir = Path(__file__).parent.parent.parent

        return cls(
            base_dir=base_dir,
            data_input_dir=base_dir / 'data' / 'input',
            data_processed_dir=base_dir / 'data' / 'processed',
            data_templates_dir=base_dir / 'data' / 'templates',
            logs_dir=base_dir / 'logs'
        )


@dataclass
class ETLConfig:
    """Configurações do processo ETL"""
    max_retries: int = 3
    retry_delay_seconds: int = 5
    batch_insert_size: int = 1000
    enable_profiling: bool = False
    parallel_ingestors: int = 1

    @classmethod
    def from_env(cls) -> 'ETLConfig':
        """Cria configuração a partir de variáveis de ambiente"""
        return cls(
            max_retries=int(os.getenv('ETL_MAX_RETRIES', 3)),
            retry_delay_seconds=int(os.getenv('ETL_RETRY_DELAY', 5)),
            batch_insert_size=int(os.getenv('ETL_BATCH_SIZE', 1000)),
            enable_profiling=os.getenv('ETL_PROFILING', 'false').lower() == 'true',
            parallel_ingestors=int(os.getenv('ETL_PARALLEL_INGESTORS', 1))
        )


class Config:
    """Configuração centralizada do projeto"""

    def __init__(self):
        self.database = DatabaseConfig.from_env()
        self.csv = CSVConfig.from_env()
        self.paths = PathsConfig.from_env()
        self.etl = ETLConfig.from_env()
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.environment = os.getenv('ENVIRONMENT', 'development')

    def __repr__(self) -> str:
        return (
            f"Config(\n"
            f"  environment={self.environment}\n"
            f"  database={self.database.host}:{self.database.port}/{self.database.database}\n"
            f"  log_level={self.log_level}\n"
            f")"
        )


# Instância global de configuração (singleton)
_config: Optional[Config] = None


def get_config() -> Config:
    """
    Retorna instância singleton da configuração.

    Returns:
        Config: Configuração do projeto

    Raises:
        ValueError: Se variáveis de ambiente obrigatórias não estiverem definidas
    """
    global _config

    if _config is None:
        _config = Config()

    return _config


# Atalhos para acesso rápido
def get_db_config() -> DatabaseConfig:
    """Retorna configuração de banco de dados"""
    return get_config().database


def get_csv_config() -> CSVConfig:
    """Retorna configuração de CSV"""
    return get_config().csv


def get_paths_config() -> PathsConfig:
    """Retorna configuração de caminhos"""
    return get_config().paths


def get_etl_config() -> ETLConfig:
    """Retorna configuração de ETL"""
    return get_config().etl
