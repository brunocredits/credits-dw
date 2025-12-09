"""
Este módulo, `rejection_logger`, fornece uma classe para gerenciar o registro
estruturado de dados que foram rejeitados durante o processo de ingestão na
camada Bronze. Ele permite acumular rejeições em um buffer e salvá-las em
lote no banco de dados para auditoria e análise posterior.
"""

import json
import math
from typing import Any, Dict, Optional, List
from datetime import datetime
from psycopg2 import sql
from psycopg2.extras import execute_values

from .db_connection import get_cursor
from .logger import setup_logger

# Logger específico para este módulo
logger = setup_logger('rejection_logger')


class RejectionLogger:
    """
    Gerencia o logging de registros rejeitados, armazenando-os em um buffer
    e inserindo-os em lote na tabela `auditoria.log_rejeicao`.
    """

    def __init__(self, conn, execucao_fk: str, script_nome: str, tabela_destino: str):
        """
        Inicializa o logger de rejeições para uma execução específica.

        Args:
            conn: A conexão com o banco de dados.
            execucao_fk (str): O UUID da execução do ETL.
            script_nome (str): O nome do script que está gerando as rejeições.
            tabela_destino (str): A tabela de destino onde a inserção falhou.
        """
        self.conn = conn
        self.execucao_fk = execucao_fk
        self.script_nome = script_nome
        self.tabela_destino = tabela_destino
        self.rejeicoes: List[Dict] = []  # Buffer para acumular rejeições

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
        Adiciona um registro de rejeição ao buffer para inserção posterior.

        Args:
            numero_linha (int, optional): O número da linha no arquivo de origem.
            campo_falha (str): O nome do campo que causou a falha.
            motivo_rejeicao (str): A descrição do motivo da rejeição.
            valor_recebido (Any, optional): O valor específico que falhou na validação.
            registro_completo (Dict, optional): O registro completo como um dicionário.
            severidade (str): A severidade da rejeição ('ERROR', 'WARNING', 'CRITICAL').
        """
        registro_json = self._serializar_registro_para_json(registro_completo)
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

        log_msg = (f"[REJEIÇÃO] Linha {numero_linha or 'N/A'}: Campo '{campo_falha}' "
                   f"falhou: {motivo_rejeicao}. Valor: '{valor_str}'")
        logger.warning(log_msg)

    def _serializar_registro_para_json(self, registro: Optional[Dict]) -> Optional[str]:
        """Converte um dicionário de registro em uma string JSON, tratando tipos de dados incompatíveis."""
        if not registro:
            return None
        try:
            # Converte valores não serilizáveis (como NaN, Inf) para strings ou None
            serializado = {
                k: str(v) if isinstance(v, (datetime, float)) and not math.isfinite(v) else v
                for k, v in registro.items()
            }
            return json.dumps(serializado, ensure_ascii=False, default=str)
        except Exception as e:
            logger.warning(f"Erro ao serializar registro para JSON: {e}")
            return str(registro)

    def salvar_rejeicoes(self) -> int:
        """
        Insere todas as rejeições acumuladas no buffer no banco de dados.

        Utiliza `psycopg2.extras.execute_values` para uma inserção em lote eficiente.

        Returns:
            int: O número de registros de rejeição que foram salvos.
        """
        if not self.rejeicoes:
            return 0

        try:
            query = sql.SQL("""
                INSERT INTO auditoria.log_rejeicao (
                    execucao_fk, script_nome, tabela_destino, numero_linha, campo_falha,
                    motivo_rejeicao, valor_recebido, registro_completo, severidade, data_rejeicao
                ) VALUES %s
            """)
            now = datetime.now()
            dados_para_inserir = [
                (
                    r['execucao_fk'], r['script_nome'], r['tabela_destino'], r['numero_linha'],
                    r['campo_falha'], r['motivo_rejeicao'], r['valor_recebido'],
                    r['registro_completo'], r['severidade'], now
                ) for r in self.rejeicoes
            ]

            with get_cursor(self.conn) as cur:
                execute_values(cur, query, dados_para_inserir, page_size=len(dados_para_inserir))

            total = len(self.rejeicoes)
            logger.info(f"{total} rejeições salvas com sucesso na tabela de auditoria.")
            self.rejeicoes.clear()  # Limpa o buffer após a inserção
            return total

        except Exception as e:
            logger.error(f"Falha crítica ao salvar rejeições no banco de dados: {e}", exc_info=True)
            raise