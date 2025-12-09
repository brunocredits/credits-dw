"""
Este módulo, `audit`, fornece um conjunto de funções para registrar e monitorar
as execuções dos pipelines de ETL. Ele é essencial para a observabilidade e
rastreabilidade do processo de ingestão de dados, permitindo registrar o início,
o fim, o status e as métricas de cada execução.
"""
from datetime import datetime
from typing import Optional, List, Dict
from contextlib import contextmanager
from uuid import uuid4
from .db_connection import get_cursor

def registrar_execucao(conn, script_nome: str, camada: str,
                       tabela_origem: Optional[str] = None,
                       tabela_destino: Optional[str] = None,
                       file_hash: Optional[str] = None) -> str:
    """
    Registra o início de uma execução de ETL na tabela de auditoria.

    Cria um novo registro com um UUID único, marcando o status inicial como
    'em_execucao'.

    Args:
        conn: A conexão com o banco de dados.
        script_nome (str): Nome do script ou processo em execução.
        camada (str): Camada do data warehouse (ex: 'bronze', 'silver').
        tabela_origem (str, optional): Tabela de origem dos dados.
        tabela_destino (str, optional): Tabela de destino dos dados.
        file_hash (str, optional): Hash do arquivo de origem, para controle de duplicatas.

    Returns:
        str: O ID (UUID) da execução registrada.
    """
    exec_id = str(uuid4())
    query = """
        INSERT INTO auditoria.historico_execucao
        (id, script_nome, camada, tabela_origem, tabela_destino, data_inicio, status, file_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_cursor(conn) as cur:
        cur.execute(query, (exec_id, script_nome, camada, tabela_origem, tabela_destino,
                           datetime.now(), 'em_execucao', file_hash))
        conn.commit()
        return exec_id

def finalizar_execucao(conn, execucao_id: str, status: str,
                       linhas_processadas: int = 0, linhas_inseridas: int = 0,
                       linhas_atualizadas: int = 0, linhas_erro: int = 0,
                       mensagem_erro: Optional[str] = None) -> None:
    """
    Atualiza o registro de uma execução de ETL com seu status final e métricas.

    Args:
        conn: A conexão com o banco de dados.
        execucao_id (str): O ID da execução a ser finalizada.
        status (str): O status final ('sucesso' ou 'erro').
        linhas_processadas (int): Total de linhas lidas da origem.
        linhas_inseridas (int): Total de linhas inseridas no destino.
        linhas_atualizadas (int): Total de linhas atualizadas no destino.
        linhas_erro (int): Total de linhas que resultaram em erro.
        mensagem_erro (str, optional): Mensagem de erro, caso o status seja 'erro'.
    """
    query = """
        UPDATE auditoria.historico_execucao
        SET data_fim = %s, status = %s, linhas_processadas = %s,
            linhas_inseridas = %s, linhas_atualizadas = %s,
            linhas_erro = %s, mensagem_erro = %s
        WHERE id = %s
    """
    with get_cursor(conn) as cur:
        cur.execute(query, (datetime.now(), status, linhas_processadas,
                           linhas_inseridas, linhas_atualizadas, linhas_erro,
                           mensagem_erro, execucao_id))
        conn.commit()

@contextmanager
def auditar_execucao(conn, script_nome: str, camada: str, tabela_destino: str = None):
    """
    Um context manager para registrar e finalizar execuções de ETL automaticamente.

    Este gerenciador de contexto simplifica a auditoria, garantindo que cada
    execução registrada seja finalizada com 'sucesso' ou 'erro', mesmo que
    exceções ocorram durante o processo.

    Exemplo de uso:
    ```
    with auditar_execucao(conn, 'meu_script', 'silver') as (exec_id, stats):
        # Seu código ETL aqui
        stats['linhas_inseridas'] = 100
    ```

    Args:
        conn: A conexão com o banco de dados.
        script_nome (str): Nome do script.
        camada (str): Camada do DW.
        tabela_destino (str, optional): Tabela de destino.

    Yields:
        tuple: Um ID de execução e um dicionário para coletar estatísticas.
    """
    execucao_id = registrar_execucao(conn, script_nome, camada, None, tabela_destino)
    stats = {
        'linhas_processadas': 0, 'linhas_inseridas': 0,
        'linhas_atualizadas': 0, 'linhas_erro': 0
    }

    try:
        yield execucao_id, stats
        finalizar_execucao(conn, execucao_id, 'sucesso', **stats)
    except Exception as e:
        finalizar_execucao(conn, execucao_id, 'erro', mensagem_erro=str(e), **stats)
        raise
