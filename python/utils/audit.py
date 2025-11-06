"""
Módulo: audit.py
Descrição: Funções de auditoria para registro de execuções ETL
Versão: 2.0
"""

from datetime import datetime
from typing import Optional, List, Dict
from contextlib import contextmanager

from utils.db_connection import get_cursor


def registrar_execucao(
    conn,
    script_nome: str,
    camada: str,
    tabela_origem: Optional[str] = None,
    tabela_destino: Optional[str] = None
) -> int:
    """
    Registra início de uma execução ETL.

    Args:
        conn: Conexão com banco
        script_nome: Nome do script executado
        camada: Camada do DW (bronze, silver, gold)
        tabela_origem: Tabela de origem (opcional)
        tabela_destino: Tabela de destino (opcional)

    Returns:
        ID da execução registrada

    Example:
        ```python
        with get_connection() as conn:
            execucao_id = registrar_execucao(
                conn, 'ingest_faturamento.py', 'bronze',
                tabela_destino='bronze.faturamento'
            )
        ```
    """
    query = """
        INSERT INTO credits.historico_atualizacoes (
            script_nome,
            camada,
            tabela_origem,
            tabela_destino,
            data_inicio,
            status
        ) VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """

    with get_cursor(conn) as cursor:
        cursor.execute(
            query,
            (script_nome, camada, tabela_origem, tabela_destino, datetime.now(), 'em_execucao')
        )
        execucao_id = cursor.fetchone()[0]
        conn.commit()

    return execucao_id


def finalizar_execucao(
    conn,
    execucao_id: int,
    status: str,
    linhas_processadas: int = 0,
    linhas_inseridas: int = 0,
    linhas_atualizadas: int = 0,
    linhas_erro: int = 0,
    mensagem_erro: Optional[str] = None
) -> None:
    """
    Finaliza registro de uma execução ETL.

    Args:
        conn: Conexão com banco
        execucao_id: ID da execução
        status: Status final (sucesso, erro, cancelado)
        linhas_processadas: Total de linhas processadas
        linhas_inseridas: Linhas inseridas
        linhas_atualizadas: Linhas atualizadas
        linhas_erro: Linhas com erro
        mensagem_erro: Mensagem de erro (se houver)

    Example:
        ```python
        finalizar_execucao(
            conn, execucao_id, 'sucesso',
            linhas_processadas=1000,
            linhas_inseridas=1000
        )
        ```
    """
    query = """
        UPDATE credits.historico_atualizacoes
        SET
            data_fim = %s,
            status = %s,
            linhas_processadas = %s,
            linhas_inseridas = %s,
            linhas_atualizadas = %s,
            linhas_erro = %s,
            mensagem_erro = %s
        WHERE id = %s
    """

    with get_cursor(conn) as cursor:
        cursor.execute(
            query,
            (
                datetime.now(),
                status,
                linhas_processadas,
                linhas_inseridas,
                linhas_atualizadas,
                linhas_erro,
                mensagem_erro,
                execucao_id
            )
        )
        conn.commit()


def obter_ultima_execucao(conn, script_nome: str) -> Optional[Dict]:
    """
    Obtém informações da última execução de um script.

    Args:
        conn: Conexão com banco
        script_nome: Nome do script

    Returns:
        Dicionário com informações da última execução ou None

    Example:
        ```python
        with get_connection() as conn:
            ultima_exec = obter_ultima_execucao(conn, 'ingest_faturamento.py')
            if ultima_exec and ultima_exec['status'] == 'sucesso':
                print("Última execução foi bem-sucedida")
        ```
    """
    query = """
        SELECT
            id,
            script_nome,
            camada,
            data_inicio,
            data_fim,
            status,
            linhas_processadas,
            linhas_inseridas,
            mensagem_erro
        FROM credits.historico_atualizacoes
        WHERE script_nome = %s
        ORDER BY data_inicio DESC
        LIMIT 1
    """

    with get_cursor(conn) as cursor:
        cursor.execute(query, (script_nome,))
        row = cursor.fetchone()

    if row:
        return {
            'id': row[0],
            'script_nome': row[1],
            'camada': row[2],
            'data_inicio': row[3],
            'data_fim': row[4],
            'status': row[5],
            'linhas_processadas': row[6],
            'linhas_inseridas': row[7],
            'mensagem_erro': row[8]
        }

    return None


def listar_execucoes_dia(conn, data: Optional[datetime] = None) -> List[Dict]:
    """
    Lista todas as execuções de um dia.

    Args:
        conn: Conexão com banco
        data: Data para consulta (padrão: hoje)

    Returns:
        Lista de execuções

    Example:
        ```python
        with get_connection() as conn:
            execucoes_hoje = listar_execucoes_dia(conn)
            for exec in execucoes_hoje:
                print(f"{exec['script_nome']}: {exec['status']}")
        ```
    """
    if data is None:
        data = datetime.now()

    query = """
        SELECT
            id,
            script_nome,
            camada,
            data_inicio,
            data_fim,
            status,
            linhas_processadas,
            linhas_inseridas
        FROM credits.historico_atualizacoes
        WHERE DATE(data_inicio) = %s
        ORDER BY data_inicio DESC
    """

    with get_cursor(conn) as cursor:
        cursor.execute(query, (data.date(),))
        rows = cursor.fetchall()

    execucoes = []
    for row in rows:
        execucoes.append({
            'id': row[0],
            'script_nome': row[1],
            'camada': row[2],
            'data_inicio': row[3],
            'data_fim': row[4],
            'status': row[5],
            'linhas_processadas': row[6],
            'linhas_inseridas': row[7]
        })

    return execucoes


def obter_execucoes_em_andamento(conn) -> List[Dict]:
    """
    Lista todas as execuções que estão em andamento.

    Args:
        conn: Conexão com banco

    Returns:
        Lista de execuções em andamento

    Example:
        ```python
        with get_connection() as conn:
            em_andamento = obter_execucoes_em_andamento(conn)
            if em_andamento:
                print(f"Há {len(em_andamento)} execução(ões) em andamento")
        ```
    """
    query = """
        SELECT
            id,
            script_nome,
            camada,
            tabela_destino,
            data_inicio,
            status
        FROM credits.historico_atualizacoes
        WHERE status = 'em_execucao'
        ORDER BY data_inicio DESC
    """

    with get_cursor(conn) as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    execucoes = []
    for row in rows:
        execucoes.append({
            'id': row[0],
            'script_nome': row[1],
            'camada': row[2],
            'tabela_destino': row[3],
            'data_inicio': row[4],
            'status': row[5]
        })

    return execucoes


def obter_estatisticas_script(conn, script_nome: str, dias: int = 30) -> Dict:
    """
    Obtém estatísticas de execução de um script nos últimos N dias.

    Args:
        conn: Conexão com banco
        script_nome: Nome do script
        dias: Número de dias para análise (padrão: 30)

    Returns:
        Dicionário com estatísticas

    Example:
        ```python
        with get_connection() as conn:
            stats = obter_estatisticas_script(conn, 'ingest_faturamento.py')
            print(f"Taxa de sucesso: {stats['taxa_sucesso']:.1f}%")
        ```
    """
    query = """
        SELECT
            COUNT(*) as total_execucoes,
            SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END) as total_sucesso,
            SUM(CASE WHEN status = 'erro' THEN 1 ELSE 0 END) as total_erro,
            AVG(EXTRACT(EPOCH FROM (data_fim - data_inicio))) as duracao_media_segundos,
            AVG(linhas_processadas) as media_linhas_processadas,
            MAX(data_inicio) as ultima_execucao
        FROM credits.historico_atualizacoes
        WHERE script_nome = %s
          AND data_inicio >= NOW() - INTERVAL '%s days'
          AND status IN ('sucesso', 'erro')
    """

    with get_cursor(conn) as cursor:
        cursor.execute(query, (script_nome, dias))
        row = cursor.fetchone()

    if row and row[0] > 0:
        total_execucoes = row[0] or 0
        total_sucesso = row[1] or 0
        total_erro = row[2] or 0
        taxa_sucesso = (total_sucesso / total_execucoes * 100) if total_execucoes > 0 else 0

        return {
            'total_execucoes': total_execucoes,
            'total_sucesso': total_sucesso,
            'total_erro': total_erro,
            'taxa_sucesso': taxa_sucesso,
            'duracao_media_segundos': float(row[3] or 0),
            'media_linhas_processadas': int(row[4] or 0),
            'ultima_execucao': row[5]
        }

    return {
        'total_execucoes': 0,
        'total_sucesso': 0,
        'total_erro': 0,
        'taxa_sucesso': 0,
        'duracao_media_segundos': 0,
        'media_linhas_processadas': 0,
        'ultima_execucao': None
    }


@contextmanager
def auditar_execucao(conn, script_nome: str, camada: str, tabela_destino: str = None):
    """
    Context manager para auditoria automática de execução.

    Args:
        conn: Conexão com banco
        script_nome: Nome do script
        camada: Camada do DW
        tabela_destino: Tabela de destino

    Yields:
        Tupla (execucao_id, stats_dict) para rastreamento

    Example:
        ```python
        with get_connection() as conn:
            with auditar_execucao(conn, 'meu_script.py', 'bronze', 'bronze.tabela') as (exec_id, stats):
                # Seu código ETL aqui
                stats['linhas_processadas'] = 1000
                stats['linhas_inseridas'] = 1000
        ```
    """
    execucao_id = registrar_execucao(
        conn=conn,
        script_nome=script_nome,
        camada=camada,
        tabela_destino=tabela_destino
    )

    stats = {
        'linhas_processadas': 0,
        'linhas_inseridas': 0,
        'linhas_atualizadas': 0,
        'linhas_erro': 0
    }

    try:
        yield execucao_id, stats
        finalizar_execucao(
            conn=conn,
            execucao_id=execucao_id,
            status='sucesso',
            **stats
        )
    except Exception as e:
        finalizar_execucao(
            conn=conn,
            execucao_id=execucao_id,
            status='erro',
            mensagem_erro=str(e),
            **stats
        )
        raise
