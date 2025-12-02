"""Auditoria de execuções ETL"""
from datetime import datetime
from typing import Optional, List, Dict
from contextlib import contextmanager
from uuid import uuid4
from .db_connection import get_connection, get_cursor
from .logger import setup_logger

def registrar_execucao(conn, script_nome: str, camada: str,
                       tabela_origem: Optional[str] = None,
                       tabela_destino: Optional[str] = None) -> str:
    """Registra início de execução ETL. Retorna UUID como string"""
    exec_id = str(uuid4())
    query = """
        INSERT INTO auditoria.historico_execucao
        (id, script_nome, camada, tabela_origem, tabela_destino, data_inicio, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with get_cursor(conn) as cur:
        cur.execute(query, (exec_id, script_nome, camada, tabela_origem, tabela_destino,
                           datetime.now(), 'em_execucao'))
        conn.commit()
        return exec_id

def finalizar_execucao(conn, execucao_id: str, status: str,
                       linhas_processadas: int = 0, linhas_inseridas: int = 0,
                       linhas_atualizadas: int = 0, linhas_erro: int = 0,
                       mensagem_erro: Optional[str] = None) -> None:
    """Finaliza registro de execução ETL"""
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

def obter_ultima_execucao(conn, script_nome: str) -> Optional[Dict]:
    """Obtém última execução de um script"""
    query = """
        SELECT id, script_nome, camada, data_inicio, data_fim, status,
               linhas_processadas, linhas_inseridas, mensagem_erro
        FROM auditoria.historico_execucao
        WHERE script_nome = %s
        ORDER BY data_inicio DESC LIMIT 1
    """
    with get_cursor(conn) as cur:
        cur.execute(query, (script_nome,))
        row = cur.fetchone()
        if row:
            return {
                'id': row[0], 'script_nome': row[1], 'camada': row[2],
                'data_inicio': row[3], 'data_fim': row[4], 'status': row[5],
                'linhas_processadas': row[6], 'linhas_inseridas': row[7],
                'mensagem_erro': row[8]
            }
    return None

def listar_execucoes_dia(conn, data: Optional[datetime] = None) -> List[Dict]:
    """Lista execuções de um dia"""
    if data is None:
        data = datetime.now()

    query = """
        SELECT id, script_nome, camada, data_inicio, data_fim, status,
               linhas_processadas, linhas_inseridas
        FROM auditoria.historico_execucao
        WHERE DATE(data_inicio) = %s
        ORDER BY data_inicio DESC
    """
    with get_cursor(conn) as cur:
        cur.execute(query, (data.date(),))
        return [
            {
                'id': r[0], 'script_nome': r[1], 'camada': r[2],
                'data_inicio': r[3], 'data_fim': r[4], 'status': r[5],
                'linhas_processadas': r[6], 'linhas_inseridas': r[7]
            }
            for r in cur.fetchall()
        ]

def obter_execucoes_em_andamento(conn) -> List[Dict]:
    """Lista execuções em andamento"""
    query = """
        SELECT id, script_nome, camada, tabela_destino, data_inicio, status
        FROM auditoria.historico_execucao
        WHERE status = 'em_execucao'
        ORDER BY data_inicio DESC
    """
    with get_cursor(conn) as cur:
        cur.execute(query)
        return [
            {
                'id': r[0], 'script_nome': r[1], 'camada': r[2],
                'tabela_destino': r[3], 'data_inicio': r[4], 'status': r[5]
            }
            for r in cur.fetchall()
        ]

def obter_estatisticas_script(conn, script_nome: str, dias: int = 30) -> Dict:
    """Estatísticas de execução de um script"""
    query = """
        SELECT COUNT(*) as total,
               SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END) as sucesso,
               SUM(CASE WHEN status = 'erro' THEN 1 ELSE 0 END) as erro,
               AVG(EXTRACT(EPOCH FROM (data_fim - data_inicio))) as duracao_media,
               AVG(linhas_processadas) as media_linhas,
               MAX(data_inicio) as ultima_exec
        FROM auditoria.historico_execucao
        WHERE script_nome = %s
          AND data_inicio >= NOW() - INTERVAL '1 day' * %s
          AND status IN ('sucesso', 'erro')
    """
    with get_cursor(conn) as cur:
        cur.execute(query, (script_nome, dias))
        row = cur.fetchone()

        if row and row[0]:
            total, sucesso, erro = row[0] or 0, row[1] or 0, row[2] or 0
            taxa = (sucesso / total * 100) if total > 0 else 0
            return {
                'total_execucoes': total, 'total_sucesso': sucesso,
                'total_erro': erro, 'taxa_sucesso': taxa,
                'duracao_media_segundos': float(row[3] or 0),
                'media_linhas_processadas': int(row[4] or 0),
                'ultima_execucao': row[5]
            }

    return {
        'total_execucoes': 0, 'total_sucesso': 0, 'total_erro': 0,
        'taxa_sucesso': 0, 'duracao_media_segundos': 0,
        'media_linhas_processadas': 0, 'ultima_execucao': None
    }

@contextmanager
def auditar_execucao(conn, script_nome: str, camada: str, tabela_destino: str = None):
    """Context manager para auditoria automática"""
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
