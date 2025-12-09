"""
Este módulo, `config`, centraliza a gestão de todas as configurações do
projeto. Ele utiliza variáveis de ambiente para permitir uma configuração
flexível e segura, separando as configurações do código-fonte.

O módulo define data classes para agrupar configurações relacionadas (banco de
dados, CSV, caminhos, ETL) e fornece uma instância singleton para acesso
global e consistente.
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """
    Agrupa todas as configurações relacionadas à conexão com o banco de dados.
    """
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
        """
        Cria uma instância de `DatabaseConfig` a partir de variáveis de ambiente.

        Lança um `ValueError` se alguma variável obrigatória não estiver definida,
        orientando o usuário a configurar o arquivo `.env`.
        """
        required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        missing = [var for var in required_vars if not os.getenv(var)]

        if missing:
            raise ValueError(
                f"❌ Variáveis de ambiente de banco de dados não definidas: {', '.join(missing)}\n"
                f"   Por favor, configure o arquivo .env com base no .env.example."
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
    """
    Define as configurações padrão para a leitura e processamento de arquivos CSV.
    """
    separator: str = ';'
    encoding: str = 'utf-8-sig'
    decimal: str = ','
    thousands: str = '.'
    date_format: str = '%Y-%m-%d'
    chunk_size: int = 10000
    na_values: list = None

    def __post_init__(self):
        """Define valores padrão para `na_values` após a inicialização."""
        if self.na_values is None:
            self.na_values = ['', 'NULL', 'null', 'NA', 'N/A', '#N/A']

    @classmethod
    def from_env(cls) -> 'CSVConfig':
        """Cria uma instância de `CSVConfig` a partir de variáveis de ambiente."""
        return cls(
            separator=os.getenv('CSV_SEPARATOR', ';'),
            encoding=os.getenv('CSV_ENCODING', 'utf-8-sig'),
            chunk_size=int(os.getenv('CSV_CHUNK_SIZE', 10000))
        )


@dataclass
class PathsConfig:
    """
    Centraliza a definição dos principais caminhos de diretórios do projeto.
    """
    base_dir: Path
    data_input_dir: Path
    data_processed_dir: Path
    data_templates_dir: Path
    logs_dir: Path

    @classmethod
    def from_env(cls) -> 'PathsConfig':
        """
        Cria uma configuração de caminhos, detectando se o ambiente é local
        ou um container Docker para ajustar o diretório base.
        """
        # Detecta se está rodando dentro do container Docker
        if Path('/app').exists():
            base_dir = Path('/app')
        else:
            # Assume ambiente local e sobe na árvore de diretórios
            base_dir = Path(__file__).parent.parent.parent

        return cls(
            base_dir=base_dir,
            data_input_dir=base_dir / 'docker' / 'data' / 'input',
            data_processed_dir=base_dir / 'docker' / 'data' / 'processed',
            data_templates_dir=base_dir / 'docker' / 'data' / 'templates',
            logs_dir=base_dir / 'logs'
        )


@dataclass
class ETLConfig:
    """
    Configurações relacionadas ao comportamento do processo de ETL.
    """
    max_retries: int = 3
    retry_delay_seconds: int = 5
    batch_insert_size: int = 1000
    enable_profiling: bool = False
    parallel_ingestors: int = 1

    @classmethod
    def from_env(cls) -> 'ETLConfig':
        """Cria uma instância de `ETLConfig` a partir de variáveis de ambiente."""
        return cls(
            max_retries=int(os.getenv('ETL_MAX_RETRIES', 3)),
            retry_delay_seconds=int(os.getenv('ETL_RETRY_DELAY', 5)),
            batch_insert_size=int(os.getenv('ETL_BATCH_SIZE', 1000)),
            enable_profiling=os.getenv('ETL_PROFILING', 'false').lower() == 'true',
            parallel_ingestors=int(os.getenv('ETL_PARALLEL_INGESTORS', 1))
        )


class Config:
    """
    Classe principal que agrega todas as configurações do projeto.
    """

    def __init__(self):
        """Carrega todas as configurações a partir do ambiente."""
        self.database = DatabaseConfig.from_env()
        self.csv = CSVConfig.from_env()
        self.paths = PathsConfig.from_env()
        self.etl = ETLConfig.from_env()
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.environment = os.getenv('ENVIRONMENT', 'development')

    def __repr__(self) -> str:
        """Representação textual simplificada da configuração."""
        return (
            f"Config(environment={self.environment}, "
            f"db={self.database.host}:{self.database.port}/{self.database.database}, "
            f"log_level={self.log_level})"
        )


# Instância global (Singleton) para garantir que a configuração seja carregada uma única vez.
_config: Optional[Config] = None


def get_config() -> Config:
    """
    Função de acesso global à instância de configuração.

    Returns:
        Config: A instância única de configuração do projeto.

    Raises:
        ValueError: Se as variáveis de ambiente obrigatórias não estiverem definidas.
    """
    global _config
    if _config is None:
        _config = Config()
    return _config


# Funções de atalho para acesso rápido a seções específicas da configuração.
def get_db_config() -> DatabaseConfig:
    """Retorna apenas a configuração do banco de dados."""
    return get_config().database

def get_paths_config() -> PathsConfig:
    """Retorna apenas a configuração de caminhos."""
    return get_config().paths
