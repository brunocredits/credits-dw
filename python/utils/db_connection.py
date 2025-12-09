"""
Este módulo, `db_connection`, é responsável por gerenciar as conexões com o
banco de dados PostgreSQL. Ele abstrai a complexidade da criação de conexões,
oferecendo funções e gerenciadores de contexto (`contextmanager`) para obter
e liberar conexões e cursores de forma segura e eficiente.

Principais características:
- Conexão robusta com tentativas automáticas (`retry`) em caso de falha.
- Gerenciadores de contexto que garantem o fechamento de conexões e cursores.
- Suporte para cursores que retornam dicionários (`RealDictCursor`).
- Configuração centralizada através do módulo `config`.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Generator
from contextlib import contextmanager
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import get_db_config

@retry(
    stop=stop_after_attempt(3),  # Tenta no máximo 3 vezes
    wait=wait_exponential(multiplier=1, min=2, max=10),  # Espera exponencial entre tentativas
    retry=retry_if_exception_type(psycopg2.OperationalError)  # Só tenta novamente em erros operacionais
)
def get_db_connection():
    """
    Cria e retorna uma nova conexão com o banco de dados PostgreSQL.

    Utiliza a biblioteca `tenacity` para tentar reconectar automaticamente em
    caso de falhas operacionais (ex: instabilidade de rede). As configurações
    de conexão são obtidas centralizadamente de `get_db_config`.

    Returns:
        psycopg2.connection: Uma conexão ativa com o banco de dados.

    Raises:
        ConnectionError: Se a conexão falhar após todas as tentativas.
    """
    try:
        db_config = get_db_config()
        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password,
            connect_timeout=db_config.connect_timeout,
            sslmode='require'  # Exige SSL para conexões seguras
        )
        conn.autocommit = False  # Desabilita autocommit para controle transacional
        return conn
    except psycopg2.Error as e:
        raise ConnectionError(f"Falha ao conectar ao banco de dados após várias tentativas: {e}") from e

@contextmanager
def get_connection() -> Generator:
    """
    Um gerenciador de contexto para obter e gerenciar uma conexão com o banco.

    Garante que a conexão seja confirmada (`commit`) em caso de sucesso, revertida
    (`rollback`) em caso de erro, e sempre fechada ao final do bloco.

    Yields:
        psycopg2.connection: A conexão com o banco de dados.
    """
    conn = None
    try:
        conn = get_db_connection()
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

@contextmanager
def get_cursor(conn, cursor_factory=None) -> Generator:
    """
    Gerenciador de contexto para obter um cursor a partir de uma conexão.

    Garante que o cursor seja fechado ao final do bloco, liberando recursos.

    Args:
        conn: A conexão ativa com o banco de dados.
        cursor_factory (optional): Uma fábrica de cursores (ex: RealDictCursor).

    Yields:
        psycopg2.cursor: O cursor para execução de comandos.
    """
    cursor = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield cursor
    finally:
        cursor.close()

@contextmanager
def get_dict_cursor(conn) -> Generator:
    """
    Gerenciador de contexto que fornece um cursor que retorna resultados como dicionários.

    Args:
        conn: A conexão ativa com o banco de dados.

    Yields:
        psycopg2.extras.RealDictCursor: Um cursor que retorna linhas como dicionários.
    """
    with get_cursor(conn, cursor_factory=RealDictCursor) as cursor:
        yield cursor

def test_connection() -> bool:
    """
    Testa a conectividade com o banco de dados executando uma consulta simples.

    Returns:
        bool: `True` se a conexão for bem-sucedida, `False` caso contrário.
    """
    try:
        with get_connection() as conn:
            with get_cursor(conn) as cursor:
                cursor.execute("SELECT 1")
                return cursor.fetchone()[0] == 1
    except Exception:
        return False
