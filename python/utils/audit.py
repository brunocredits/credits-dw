"""
Módulo: audit.py
Descrição: Funções de auditoria para registro de execuções ETL
"""

from datetime import datetime
from typing import Optional


def registrar_execucao(
    conn,
    script_nome: str,
    camada: str,
    tabela_origem: Optional[str] = None,
    tabela_destino: Optional[str] = None
) -> int:
    """
    Registra início de uma execução ETL
    
    Args:
        conn: Conexão com banco
        script_nome: Nome do script executado
        camada: Camada do DW (bronze, silver, gold)
        tabela_origem: Tabela de origem (opcional)
        tabela_destino: Tabela de destino (opcional)
        
    Returns:
        ID da execução registrada
    """
    cursor = conn.cursor()
    
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
    
    cursor.execute(
        query,
        (script_nome, camada, tabela_origem, tabela_destino, datetime.now(), 'em_execucao')
    )
    
    execucao_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    
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
):
    """
    Finaliza registro de uma execução ETL
    
    Args:
        conn: Conexão com banco
        execucao_id: ID da execução
        status: Status final (sucesso, erro, cancelado)
        linhas_processadas: Total de linhas processadas
        linhas_inseridas: Linhas inseridas
        linhas_atualizadas: Linhas atualizadas
        linhas_erro: Linhas com erro
        mensagem_erro: Mensagem de erro (se houver)
    """
    cursor = conn.cursor()
    
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
    cursor.close()


def obter_ultima_execucao(conn, script_nome: str) -> Optional[dict]:
    """
    Obtém informações da última execução de um script
    
    Args:
        conn: Conexão com banco
        script_nome: Nome do script
        
    Returns:
        Dicionário com informações da última execução ou None
    """
    cursor = conn.cursor()
    
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
    
    cursor.execute(query, (script_nome,))
    row = cursor.fetchone()
    cursor.close()
    
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


def listar_execucoes_dia(conn, data: Optional[datetime] = None) -> list:
    """
    Lista todas as execuções de um dia
    
    Args:
        conn: Conexão com banco
        data: Data para consulta (padrão: hoje)
        
    Returns:
        Lista de execuções
    """
    if data is None:
        data = datetime.now()
    
    cursor = conn.cursor()
    
    query = """
        SELECT 
            id,
            script_nome,
            camada,
            data_inicio,
            data_fim,
            status,
            linhas_processadas
        FROM credits.historico_atualizacoes
        WHERE DATE(data_inicio) = %s
        ORDER BY data_inicio DESC
    """
    
    cursor.execute(query, (data.date(),))
    rows = cursor.fetchall()
    cursor.close()
    
    execucoes = []
    for row in rows:
        execucoes.append({
            'id': row[0],
            'script_nome': row[1],
            'camada': row[2],
            'data_inicio': row[3],
            'data_fim': row[4],
            'status': row[5],
            'linhas_processadas': row[6]
        })
    
    return execucoes
