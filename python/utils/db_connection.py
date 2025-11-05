"""
Módulo: db_connection.py
Descrição: Gerenciamento de conexões com PostgreSQL
"""

import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from typing import Optional


# Pool de conexões (para uso em produção)
_connection_pool: Optional[SimpleConnectionPool] = None


def init_connection_pool(minconn: int = 1, maxconn: int = 10):
    """
    Inicializa pool de conexões
    
    Args:
        minconn: Número mínimo de conexões
        maxconn: Número máximo de conexões
    """
    global _connection_pool
    
    if _connection_pool is None:
        _connection_pool = SimpleConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            host=os.getenv('DB_HOST', 'host.docker.internal'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'creditsdw'),
            user=os.getenv('DB_USER', 'creditsdw'),
            password=os.getenv('DB_PASSWORD', '58230925AD@')
        )


def get_db_connection():
    """
    Retorna conexão com o banco de dados
    
    Returns:
        Conexão psycopg2
    """
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'host.docker.internal'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'creditsdw'),
        user=os.getenv('DB_USER', 'creditsdw'),
        password=os.getenv('DB_PASSWORD', '58230925AD@'),
        connect_timeout=10
    )
    
    # Configurações de sessão
    conn.autocommit = False
    
    return conn


def get_pooled_connection():
    """
    Retorna conexão do pool
    
    Returns:
        Conexão do pool
    """
    if _connection_pool is None:
        init_connection_pool()
    
    return _connection_pool.getconn()


def return_pooled_connection(conn):
    """
    Devolve conexão ao pool
    
    Args:
        conn: Conexão a ser devolvida
    """
    if _connection_pool:
        _connection_pool.putconn(conn)


def close_all_connections():
    """Fecha todas as conexões do pool"""
    global _connection_pool
    
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None