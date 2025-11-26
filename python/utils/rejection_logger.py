"""
Módulo: rejection_logger.py
Descrição: Sistema de logging estruturado para registros rejeitados na camada Bronze
Versão: 1.0

Este módulo registra detalhes de registros rejeitados durante a ingestão,
facilitando auditoria e correção de dados.
"""

import json
from typing import Any, Dict, Optional, List
from datetime import datetime
from psycopg2 import sql

from utils.db_connection import get_cursor
from utils.logger import setup_logger

logger = setup_logger('rejection_logger')


class RejectionLogger:
    """
    Gerenciador de logs de rejeição para registros inválidos.

    Registra rejeições em lote no banco de dados para otimizar performance.
    """

    def __init__(self, conn, execucao_id: str, script_nome: str, tabela_destino: str):
        """
        Inicializa o logger de rejeições.

        Args:
            conn: Conexão com o banco de dados
            execucao_id: UUID da execução ETL (FK para credits.historico_atualizacoes)
            script_nome: Nome do script que está executando
            tabela_destino: Tabela Bronze de destino
        """
        self.conn = conn
        self.execucao_id = execucao_id
        self.script_nome = script_nome
        self.tabela_destino = tabela_destino
        self.rejeicoes = []  # Buffer para inserção em lote

    def registrar_rejeicao(
        self,
        numero_linha: Optional[int],
        campo_falha: str,
        motivo_rejeicao: str,
        valor_recebido: Any = None,
        registro_completo: Optional[Dict] = None,
        severidade: str = 'ERROR'
    ) -> None:
        """
        Registra uma rejeição no buffer para inserção posterior.

        Args:
            numero_linha: Número da linha no arquivo CSV original
            campo_falha: Nome do campo que falhou na validação
            motivo_rejeicao: Descrição clara do motivo da rejeição
            valor_recebido: Valor que causou a falha
            registro_completo: Registro completo em formato dict
            severidade: 'WARNING', 'ERROR' ou 'CRITICAL'
        """
        registro_json = None
        if registro_completo:
            try:
                registro_serializado = self._serializar_registro(registro_completo)
                registro_json = json.dumps(registro_serializado, ensure_ascii=False)
            except Exception as e:
                logger.warning(f"Erro ao serializar registro: {e}")
                registro_json = str(registro_completo)

        valor_str = None
        if valor_recebido is not None:
            valor_str = str(valor_recebido)[:500]

        rejeicao = {
            'execucao_id': self.execucao_id,
            'script_nome': self.script_nome,
            'tabela_destino': self.tabela_destino,
            'numero_linha': numero_linha,
            'campo_falha': campo_falha,
            'motivo_rejeicao': motivo_rejeicao,
            'valor_recebido': valor_str,
            'registro_completo': registro_json,
            'severidade': severidade
        }

        self.rejeicoes.append(rejeicao)

        log_msg = f"REJEIÇÃO | Linha {numero_linha} | Campo '{campo_falha}' | {motivo_rejeicao}"
        if valor_recebido is not None:
            log_msg += f" | Valor: {valor_str}"

        if severidade == 'CRITICAL':
            logger.critical(log_msg)
        elif severidade == 'ERROR':
            logger.error(log_msg)
        else:
            logger.warning(log_msg)

    def _serializar_registro(self, registro: Dict) -> Dict:
        """
        Serializa valores não-JSON compatíveis em um registro.

        Args:
            registro: Dicionário com valores potencialmente não serializáveis

        Returns:
            Dicionário serializado
        """
        import math

        serializado = {}
        for chave, valor in registro.items():
            if valor is None:
                serializado[chave] = None
            elif isinstance(valor, float) and (math.isnan(valor) or math.isinf(valor)):
                serializado[chave] = None
            elif isinstance(valor, (str, int, float, bool)):
                serializado[chave] = valor
            elif isinstance(valor, datetime):
                serializado[chave] = valor.isoformat()
            else:
                serializado[chave] = str(valor)
        return serializado

    def salvar_rejeicoes(self) -> int:
        """
        Salva todas as rejeições acumuladas no banco de dados em lote.

        Returns:
            Número de rejeições salvas
        """
        if not self.rejeicoes:
            return 0

        try:
            query = sql.SQL("""
                INSERT INTO credits.logs_rejeicao
                (execucao_id, script_nome, tabela_destino, numero_linha, campo_falha,
                 motivo_rejeicao, valor_recebido, registro_completo, severidade, data_rejeicao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)

            with get_cursor(self.conn) as cur:
                for rej in self.rejeicoes:
                    cur.execute(query, (
                        rej['execucao_id'],
                        rej['script_nome'],
                        rej['tabela_destino'],
                        rej['numero_linha'],
                        rej['campo_falha'],
                        rej['motivo_rejeicao'],
                        rej['valor_recebido'],
                        rej['registro_completo'],
                        rej['severidade'],
                        datetime.now()
                    ))

            total = len(self.rejeicoes)
            logger.info(f"✓ {total} rejeições salvas no banco de dados")
            self.rejeicoes = []  # Limpar buffer
            return total

        except Exception as e:
            logger.error(f"Erro ao salvar rejeições: {e}", exc_info=True)
            raise

    def get_total_rejeicoes(self) -> int:
        """
        Retorna o número total de rejeições acumuladas no buffer.

        Returns:
            Número de rejeições no buffer
        """
        return len(self.rejeicoes)

    def get_rejeicoes_por_campo(self) -> Dict[str, int]:
        """
        Retorna contagem de rejeições agrupadas por campo.

        Returns:
            Dicionário {campo: contagem}
        """
        contagem = {}
        for rej in self.rejeicoes:
            campo = rej['campo_falha']
            contagem[campo] = contagem.get(campo, 0) + 1
        return contagem

    def get_rejeicoes_por_severidade(self) -> Dict[str, int]:
        """
        Retorna contagem de rejeições agrupadas por severidade.

        Returns:
            Dicionário {severidade: contagem}
        """
        contagem = {}
        for rej in self.rejeicoes:
            sev = rej['severidade']
            contagem[sev] = contagem.get(sev, 0) + 1
        return contagem

    def imprimir_resumo(self) -> None:
        """Imprime um resumo das rejeições acumuladas."""
        total = self.get_total_rejeicoes()

        if total == 0:
            logger.success("Nenhuma rejeição encontrada")
            return

        logger.warning("=" * 80)
        logger.warning(f"RESUMO DE REJEIÇÕES: {total} registros rejeitados")
        logger.warning("=" * 80)

        por_campo = self.get_rejeicoes_por_campo()
        logger.warning("Rejeições por campo:")
        for campo, count in sorted(por_campo.items(), key=lambda x: x[1], reverse=True):
            logger.warning(f"  • {campo}: {count:,} rejeições")

        por_severidade = self.get_rejeicoes_por_severidade()
        logger.warning("Rejeições por severidade:")
        for sev, count in sorted(por_severidade.items()):
            logger.warning(f"  • {sev}: {count:,} rejeições")

        logger.warning("=" * 80)


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def consultar_rejeicoes_execucao(conn, execucao_id: str) -> List[Dict]:
    """
    Consulta todas as rejeições de uma execução específica.

    Args:
        conn: Conexão com o banco de dados
        execucao_id: UUID da execução ETL

    Returns:
        Lista de dicionários com informações das rejeições
    """
    query = """
        SELECT id, numero_linha, campo_falha, motivo_rejeicao,
               valor_recebido, severidade, data_rejeicao
        FROM credits.logs_rejeicao
        WHERE execucao_id = %s
        ORDER BY numero_linha, id
    """

    with get_cursor(conn) as cur:
        cur.execute(query, (execucao_id,))
        return [
            {
                'id': r[0],
                'numero_linha': r[1],
                'campo_falha': r[2],
                'motivo_rejeicao': r[3],
                'valor_recebido': r[4],
                'severidade': r[5],
                'data_rejeicao': r[6]
            }
            for r in cur.fetchall()
        ]


def consultar_rejeicoes_por_script(conn, script_nome: str, dias: int = 7) -> List[Dict]:
    """
    Consulta rejeições de um script nos últimos N dias.

    Args:
        conn: Conexão com o banco de dados
        script_nome: Nome do script
        dias: Número de dias para buscar (padrão: 7)

    Returns:
        Lista de dicionários com informações das rejeições
    """
    query = """
        SELECT campo_falha, motivo_rejeicao, COUNT(*) as total,
               MIN(data_rejeicao) as primeira_ocorrencia,
               MAX(data_rejeicao) as ultima_ocorrencia
        FROM credits.logs_rejeicao
        WHERE script_nome = %s
          AND data_rejeicao >= NOW() - INTERVAL '1 day' * %s
        GROUP BY campo_falha, motivo_rejeicao
        ORDER BY total DESC
    """

    with get_cursor(conn) as cur:
        cur.execute(query, (script_nome, dias))
        return [
            {
                'campo_falha': r[0],
                'motivo_rejeicao': r[1],
                'total': r[2],
                'primeira_ocorrencia': r[3],
                'ultima_ocorrencia': r[4]
            }
            for r in cur.fetchall()
        ]


def limpar_logs_antigos(conn, dias_retencao: int = 90) -> int:
    """
    Remove logs de rejeição mais antigos que o período de retenção.

    Args:
        conn: Conexão com o banco de dados
        dias_retencao: Número de dias para manter logs (padrão: 90)

    Returns:
        Número de registros removidos
    """
    query = """
        DELETE FROM credits.logs_rejeicao
        WHERE data_rejeicao < NOW() - INTERVAL '1 day' * %s
    """

    with get_cursor(conn) as cur:
        cur.execute(query, (dias_retencao,))
        count = cur.rowcount
        logger.info(f"✓ {count} logs de rejeição antigos removidos (> {dias_retencao} dias)")
        return count
