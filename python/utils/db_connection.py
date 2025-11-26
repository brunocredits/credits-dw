"""
Módulo: db_connection.py
Descrição: Gerenciamento de conexões com PostgreSQL usando context managers
Versão: 2.0
"""

import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor
from typing import Optional, Generator
from contextlib import contextmanager
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from utils.config import get_db_config


# Pool de conexões global
_connection_pool: Optional[SimpleConnectionPool] = None


def init_connection_pool() -> None:
    """
    Inicializa pool de conexões com configuração centralizada.

    Raises:
        ValueError: Se variáveis de ambiente não estiverem configuradas
        psycopg2.Error: Se houver erro na conexão com o banco
    """
    global _connection_pool

    if _connection_pool is not None:
        return  # Pool já inicializado

    db_config = get_db_config()

    try:
        _connection_pool = SimpleConnectionPool(
            minconn=db_config.pool_min_size,
            maxconn=db_config.pool_max_size,
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password,
            connect_timeout=db_config.connect_timeout
        )
    except psycopg2.Error as e:
        # Não usar f-string aqui para evitar problemas com chaves na mensagem
        raise ConnectionError("Falha ao inicializar pool de conexões: " + str(e)) from e


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(psycopg2.OperationalError)
)
def get_db_connection():
    """
    Retorna conexão com o banco de dados com retry automático.

    Returns:
        Conexão psycopg2

    Raises:
        ValueError: Se variáveis de ambiente obrigatórias não estiverem definidas
        psycopg2.Error: Se houver erro ao conectar após tentativas
    """
    db_config = get_db_config()

    conn = psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        database=db_config.database,
        user=db_config.user,
        password=db_config.password,
        connect_timeout=db_config.connect_timeout
    )

    # Configurações de sessão
    conn.autocommit = False

    return conn


@contextmanager
def get_connection() -> Generator:
    """
    Context manager para obter conexão do banco de dados.
    Garante que a conexão seja fechada após o uso.

    Yields:
        Conexão psycopg2

    Example:
        ```python
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
        ```
    """
    conn = None
    try:
        conn = get_db_connection()
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


@contextmanager
def get_cursor(conn, cursor_factory=None) -> Generator:
    """
    Context manager para obter cursor com commit/rollback automático.

    Args:
        conn: Conexão com o banco de dados
        cursor_factory: Factory para criar cursor customizado (ex: RealDictCursor)

    Yields:
        Cursor psycopg2

    Example:
        ```python
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                cursor.execute("SELECT * FROM table")
                results = cursor.fetchall()
        ```
    """
    cursor = conn.cursor(cursor_factory=cursor_factory) if cursor_factory else conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


@contextmanager
def get_dict_cursor(conn) -> Generator:
    """
    Context manager para obter cursor que retorna dicionários.

    Args:
        conn: Conexão com o banco de dados

    Yields:
        RealDictCursor psycopg2

    Example:
        ```python
        with get_connection() as conn:
            with get_dict_cursor(conn) as cursor:
                cursor.execute("SELECT * FROM table")
                rows = cursor.fetchall()  # Lista de dicionários
        ```
    """
    with get_cursor(conn, cursor_factory=RealDictCursor) as cursor:
        yield cursor


def get_pooled_connection():
    """
    Retorna conexão do pool.

    Returns:
        Conexão do pool

    Raises:
        RuntimeError: Se o pool não foi inicializado
    """
    if _connection_pool is None:
        init_connection_pool()

    return _connection_pool.getconn()


def return_pooled_connection(conn) -> None:
    """
    Devolve conexão ao pool.

    Args:
        conn: Conexão a ser devolvida
    """
    if _connection_pool:
        _connection_pool.putconn(conn)


@contextmanager
def get_pooled_connection_context() -> Generator:
    """
    Context manager para usar conexão do pool.

    Yields:
        Conexão do pool

    Example:
        ```python
        with get_pooled_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
        ```
    """
    conn = None
    try:
        conn = get_pooled_connection()
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            return_pooled_connection(conn)


def close_all_connections() -> None:
    """Fecha todas as conexões do pool"""
    global _connection_pool

    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None


def test_connection() -> bool:
    """
    Testa conexão com o banco de dados.

    Returns:
        bool: True se conexão bem-sucedida, False caso contrário
    """
    try:
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                cursor.execute("SELECT 1")
                return cursor.fetchone()[0] == 1
    except Exception:
        return False
