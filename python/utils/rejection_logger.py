"""
Módulo: rejection_logger.py
Descrição: Sistema de logging estruturado para registros rejeitados na camada Bronze.
"""

import json
import math
from typing import Any, Dict, Optional, List
from datetime import datetime
from psycopg2 import sql

from utils.db_connection import get_cursor
from utils.logger import setup_logger

logger = setup_logger('rejection_logger')


class RejectionLogger:
    """
    Gerenciador de logs de rejeição para registros inválidos.
    """

    def __init__(self, conn, execucao_fk: str, script_nome: str, tabela_destino: str):
        """
        Inicializa o logger de rejeições.

        Args:
            conn: Conexão com o banco de dados.
            execucao_fk: UUID da execução ETL (FK para auditoria.historico_execucao).
            script_nome: Nome do script que está executando.
            tabela_destino: Tabela Bronze de destino.
        """
        self.conn = conn
        self.execucao_fk = execucao_fk
        self.script_nome = script_nome
        self.tabela_destino = tabela_destino
        self.rejeicoes: List[Dict] = []  # Buffer para inserção em lote

    def registrar_rejeicao(
        self,
        numero_linha: Optional[int],
        campo_falha: str,
        motivo_rejeicao: str,
        valor_recebido: Any = None,
        registro_completo: Optional[Dict] = None,
        severidade: str = 'ERROR'
    ) -> None:
        """Registra uma rejeição no buffer para inserção posterior."""
        registro_json = None
        if registro_completo:
            try:
                registro_serializado = self._serializar_registro(registro_completo)
                registro_json = json.dumps(registro_serializado, ensure_ascii=False)
            except Exception as e:
                logger.warning(f"[AUDITORIA][AVISO] Erro ao serializar registro para log de rejeição: {e}")
                registro_json = str(registro_completo)

        valor_str = str(valor_recebido)[:500] if valor_recebido is not None else None

        rejeicao = {
            'execucao_fk': self.execucao_fk,
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

        log_msg = (
            f"[BRONZE][REJEICAO] Linha {numero_linha or 'N/A'}: "
            f"Campo '{campo_falha}' falhou na validação ({motivo_rejeicao}). "
            f"Valor: '{valor_str}'"
        )
        if severidade == 'CRITICAL':
            logger.critical(log_msg)
        else: # ERROR ou WARNING
            logger.warning(log_msg)

    def _serializar_registro(self, registro: Dict) -> Dict:
        """Serializa valores não-JSON compatíveis em um registro."""
        serializado = {}
        for chave, valor in registro.items():
            if isinstance(valor, float) and (math.isnan(valor) or math.isinf(valor)):
                serializado[chave] = None
            elif isinstance(valor, (str, int, float, bool, type(None))):
                serializado[chave] = valor
            elif isinstance(valor, datetime):
                serializado[chave] = valor.isoformat()
            else:
                serializado[chave] = str(valor)
        return serializado

    def salvar_rejeicoes(self) -> int:
        """Salva todas as rejeições acumuladas no banco de dados em lote."""
        if not self.rejeicoes:
            return 0

        try:
            # A query para execute_values deve ter um único %s
            query = sql.SQL("""
                INSERT INTO auditoria.log_rejeicao
                (execucao_fk, script_nome, tabela_destino, numero_linha, campo_falha,
                 motivo_rejeicao, valor_recebido, registro_completo, severidade, data_rejeicao)
                VALUES %s
            """)

            now = datetime.now()
            # Prepara a lista de tuplas com os dados
            dados_para_inserir = [
                (
                    rej['execucao_fk'], rej['script_nome'], rej['tabela_destino'],
                    rej['numero_linha'], rej['campo_falha'], rej['motivo_rejeicao'],
                    rej['valor_recebido'], rej['registro_completo'], rej['severidade'], now
                )
                for rej in self.rejeicoes
            ]

            with get_cursor(self.conn) as cur:
                # Importa e usa execute_values para inserção em lote
                from psycopg2.extras import execute_values
                execute_values(cur, query, dados_para_inserir, page_size=len(dados_para_inserir))

            total = len(self.rejeicoes)
            logger.info(f"[AUDITORIA][INFO] {total} rejeições salvas em auditoria.log_rejeicao.")
            self.rejeicoes = []  # Limpar buffer
            return total

        except Exception as e:
            logger.error(f"[AUDITORIA][ERRO] Falha ao salvar rejeições no banco: {e}", exc_info=True)
            raise

    def get_total_rejeicoes(self) -> int:
        """Retorna o número total de rejeições acumuladas no buffer."""
        return len(self.rejeicoes)

    def imprimir_resumo(self) -> None:
        """Imprime um resumo das rejeições acumuladas."""
        total = self.get_total_rejeicoes()
        if total == 0:
            return

        logger.warning("=" * 80)
        logger.warning(f"📊 [BRONZE] RESUMO DE REJEIÇÕES: {total} registros rejeitados")
        logger.warning("-" * 80)

        por_campo = sorted(self.get_rejeicoes_por_campo().items(), key=lambda x: x[1], reverse=True)
        logger.warning("   - Por Campo:")
        for campo, count in por_campo:
            logger.warning(f"     • {campo}: {count} rejeições")

        por_severidade = sorted(self.get_rejeicoes_por_severidade().items())
        logger.warning("   - Por Severidade:")
        for sev, count in por_severidade:
            logger.warning(f"     • {sev}: {count} rejeições")
        logger.warning("=" * 80)
        
    def get_rejeicoes_por_campo(self) -> Dict[str, int]:
        """Retorna contagem de rejeições agrupadas por campo."""
        contagem: Dict[str, int] = {}
        for rej in self.rejeicoes:
            campo = rej.get('campo_falha', 'DESCONHECIDO')
            contagem[campo] = contagem.get(campo, 0) + 1
        return contagem

    def get_rejeicoes_por_severidade(self) -> Dict[str, int]:
        """Retorna contagem de rejeições agrupadas por severidade."""
        contagem: Dict[str, int] = {}
        for rej in self.rejeicoes:
            sev = rej.get('severidade', 'DESCONHECIDA')
            contagem[sev] = contagem.get(sev, 0) + 1
        return contagem