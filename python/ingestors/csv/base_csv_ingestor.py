#!/usr/bin/env python3
"""
Módulo: base_csv_ingestor.py
Descrição: Classe base para ingestão CSV na camada Bronze com validação rigorosa
Versão: 2.0

IMPORTANTE: A camada Bronze agora implementa validação rigorosa.
Apenas dados VÁLIDOS são inseridos no banco de dados.
Dados inválidos são REJEITADOS e registrados em credits.logs_rejeicao.
"""

import os
import sys
import pandas as pd
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from abc import ABC, abstractmethod
from psycopg2.extras import execute_values
from psycopg2 import sql

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.db_connection import get_db_connection, get_cursor
from utils.logger import setup_logger, log_execution_summary
from utils.audit import registrar_execucao, finalizar_execucao
from utils.config import get_paths_config, get_csv_config, get_etl_config
from utils.validators import validar_campo
from utils.rejection_logger import RejectionLogger


# ============================================================================
# CONFIGURAÇÃO DE SEGURANÇA
# ============================================================================

# Whitelist de tabelas permitidas (segurança contra SQL injection)
TABELAS_PERMITIDAS = {
    'bronze.contas_base_oficial',
    'bronze.usuarios',
    'bronze.faturamento',
    'bronze.data'
}


# ============================================================================
# CLASSE BASE PARA INGESTÃO CSV
# ============================================================================

class BaseCSVIngestor(ABC):
    """
    Classe base para ingestores CSV com validação rigorosa (Template Method pattern).

    A camada Bronze agora implementa validação de qualidade ANTES da inserção.
    Registros inválidos são rejeitados e apenas dados válidos entram no banco.

    Fluxo de execução:
    1. Validar arquivo CSV existe e é acessível
    2. Conectar ao banco de dados
    3. Registrar início da execução (auditoria)
    4. Ler CSV linha por linha
    5. Validar cada linha rigorosamente
    6. Rejeitar linhas inválidas (log estruturado)
    7. Transformar apenas linhas válidas para formato Bronze
    8. Inserir dados válidos (TRUNCATE/RELOAD)
    9. Salvar logs de rejeição no banco
    10. Arquivar arquivo processado
    11. Finalizar execução (auditoria)
    """

    def __init__(
        self,
        script_name: str,
        tabela_destino: str,
        arquivo_nome: str,
        input_subdir: str = ''
    ):
        """
        Inicializa o ingestor CSV.

        Args:
            script_name: Nome do script para logs (ex: 'ingest_faturamento.py')
            tabela_destino: Tabela Bronze de destino (ex: 'bronze.faturamento')
            arquivo_nome: Nome do arquivo CSV a ser processado
            input_subdir: Subdiretório dentro de /app/data/input/ (ex: 'onedrive')

        Raises:
            ValueError: Se tabela_destino não estiver na whitelist
        """
        # Validação de segurança: apenas tabelas permitidas
        if tabela_destino not in TABELAS_PERMITIDAS:
            raise ValueError(
                f"Tabela não permitida: {tabela_destino}. "
                f"Tabelas permitidas: {TABELAS_PERMITIDAS}"
            )

        self.script_name = script_name
        self.tabela_destino = tabela_destino
        self.arquivo_nome = arquivo_nome

        # Carregar configurações centralizadas
        self.paths_config = get_paths_config()
        self.csv_config = get_csv_config()
        self.etl_config = get_etl_config()

        # Configurar caminhos de arquivos
        self.arquivo_path = self.paths_config.data_input_dir / input_subdir / arquivo_nome
        self.processed_dir = self.paths_config.data_processed_dir
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        # Configurar logger
        self.logger = setup_logger(script_name)
        self.start_time = None

        # Rejection logger será inicializado após conexão com banco
        self.rejection_logger: Optional[RejectionLogger] = None

    # ========================================================================
    # MÉTODOS ABSTRATOS (devem ser implementados pelas classes filhas)
    # ========================================================================

    @abstractmethod
    def get_column_mapping(self) -> Dict[str, str]:
        """
        Retorna mapeamento de colunas CSV -> Bronze.

        Returns:
            Dict com mapeamento {coluna_csv: coluna_bronze}

        Example:
            {'Data': 'data', 'Receita': 'receita', 'Moeda': 'moeda'}
        """
        pass

    @abstractmethod
    def get_bronze_columns(self) -> List[str]:
        """
        Retorna lista ordenada de colunas da tabela Bronze.

        Returns:
            Lista com nomes das colunas Bronze na ordem correta

        Example:
            ['data', 'receita', 'moeda', 'cnpj_cliente', 'email_usuario']
        """
        pass

    @abstractmethod
    def get_validation_rules(self) -> Dict[str, dict]:
        """
        Retorna regras de validação para cada campo Bronze.

        Returns:
            Dict com regras {nome_campo: dict_regras}

        Example:
            {
                'data': {'obrigatorio': True, 'tipo': 'data'},
                'receita': {'obrigatorio': True, 'tipo': 'decimal', 'positivo': True},
                'email_usuario': {'obrigatorio': True, 'tipo': 'email'}
            }
        """
        pass

    # ========================================================================
    # MÉTODOS OPCIONAIS (podem ser sobrescritos se necessário)
    # ========================================================================

    def get_date_columns(self) -> List[str]:
        """
        Retorna colunas que contêm datas (auto-detecta por prefixo data_ ou dt_).

        Returns:
            Lista de nomes de colunas de data
        """
        return [
            c for c in self.get_bronze_columns()
            if c.startswith(('data_', 'dt_'))
        ]

    def transform_custom(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformações customizadas antes da validação (opcional).

        Use este método para transformações específicas que não são
        cobertas pela lógica padrão (ex: cálculos, concatenações, etc).

        Args:
            df: DataFrame com colunas Bronze

        Returns:
            DataFrame transformado
        """
        return df

    # ========================================================================
    # VALIDAÇÃO E LEITURA DE ARQUIVO
    # ========================================================================

    def validar_arquivo(self) -> bool:
        """
        Valida se arquivo existe e tem permissões adequadas.

        Returns:
            True se arquivo é válido

        Raises:
            FileNotFoundError: Se arquivo não existe
            PermissionError: Se não há permissão de leitura
        """
        if not self.arquivo_path.exists():
            raise FileNotFoundError(
                f"Arquivo não encontrado: {self.arquivo_path}"
            )

        if not os.access(self.arquivo_path, os.R_OK):
            raise PermissionError(
                f"Sem permissão de leitura: {self.arquivo_path}"
            )

        size_mb = self.arquivo_path.stat().st_size / 1024 / 1024
        self.logger.info(f"✓ Arquivo válido: {self.arquivo_nome} ({size_mb:.2f} MB)")
        return True

    def ler_csv(self) -> pd.DataFrame:
        """
        Lê arquivo CSV com configurações centralizadas.

        Returns:
            DataFrame com dados do CSV (todas colunas como string)

        Raises:
            pd.errors.EmptyDataError: Se arquivo está vazio
            pd.errors.ParserError: Se há erro ao parsear CSV
        """
        try:
            self.logger.info(f"📖 Lendo arquivo: {self.arquivo_nome}")

            df = pd.read_csv(
                self.arquivo_path,
                encoding=self.csv_config.encoding,
                sep=self.csv_config.separator,
                dtype=str,  # Ler tudo como string inicialmente
                na_values=self.csv_config.na_values,
                keep_default_na=False  # Não converter strings vazias em NaN
            )

            self.logger.success(f"✓ {len(df):,} linhas lidas do CSV")
            return df

        except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
            self.logger.error(f"❌ Erro ao ler CSV: {e}")
            raise

    def validar_colunas_csv(self, df: pd.DataFrame) -> bool:
        """
        Valida se todas as colunas esperadas estão presentes no CSV.

        Args:
            df: DataFrame lido do CSV

        Returns:
            True se todas colunas estão presentes

        Raises:
            ValueError: Se alguma coluna obrigatória está faltando
        """
        esperadas = set(self.get_column_mapping().keys())
        presentes = set(df.columns)
        faltando = esperadas - presentes

        if faltando:
            raise ValueError(
                f"Colunas faltando no CSV: {faltando}. "
                f"Esperadas: {esperadas}"
            )

        self.logger.success("✓ Todas colunas esperadas presentes no CSV")
        return True

    # ========================================================================
    # VALIDAÇÃO RIGOROSA DE DADOS
    # ========================================================================

    def validar_e_filtrar_dados(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """
        Valida cada linha do DataFrame e filtra apenas dados válidos.

        Este é o CORAÇÃO da validação rigorosa da Bronze.
        Cada linha é validada campo por campo usando as regras definidas.
        Linhas inválidas são rejeitadas e registradas.

        Args:
            df: DataFrame com colunas Bronze já mapeadas

        Returns:
            Tupla (df_valido, num_rejeitados)
            - df_valido: DataFrame apenas com linhas válidas
            - num_rejeitados: Número de linhas rejeitadas
        """
        self.logger.info("🔍 Validando dados rigorosamente...")

        regras = self.get_validation_rules()
        indices_validos = []
        total_linhas = len(df)

        # Validar cada linha individualmente
        for idx, row in df.iterrows():
            # idx é o número da linha no DataFrame (0-based)
            # Adicionar +2 para obter linha do CSV original (+1 por 0-based, +1 por header)
            numero_linha_csv = idx + 2

            linha_valida = True
            registro_dict = row.to_dict()

            # Validar cada campo desta linha
            for campo, regra in regras.items():
                valor = row.get(campo)

                # Aplicar validação
                valido, mensagem_erro = validar_campo(valor, campo, regra)

                if not valido:
                    # Registrar rejeição
                    self.rejection_logger.registrar_rejeicao(
                        numero_linha=numero_linha_csv,
                        campo_falha=campo,
                        motivo_rejeicao=mensagem_erro,
                        valor_recebido=valor,
                        registro_completo=registro_dict,
                        severidade='ERROR'
                    )
                    linha_valida = False
                    break  # Parar validação desta linha (já é inválida)

            if linha_valida:
                indices_validos.append(idx)

        # Filtrar apenas linhas válidas
        df_valido = df.loc[indices_validos].copy()
        num_rejeitados = total_linhas - len(df_valido)

        if num_rejeitados > 0:
            percentual = (num_rejeitados / total_linhas) * 100
            self.logger.warning(
                f"⚠️  {num_rejeitados:,} linhas rejeitadas ({percentual:.1f}%)"
            )
            self.rejection_logger.imprimir_resumo()
        else:
            self.logger.success("✓ Todas as linhas são válidas!")

        self.logger.success(
            f"✓ {len(df_valido):,} linhas válidas prontas para Bronze"
        )

        return df_valido, num_rejeitados

    # ========================================================================
    # TRANSFORMAÇÃO PARA BRONZE
    # ========================================================================

    def transformar_para_bronze(self, df: pd.DataFrame) -> Tuple[List, List[str]]:
        """
        Transforma DataFrame validado para formato Bronze.

        Args:
            df: DataFrame com dados já validados

        Returns:
            Tupla (registros, colunas)
            - registros: Lista de listas com valores
            - colunas: Lista com nomes das colunas
        """
        self.logger.info("🔄 Transformando para formato Bronze...")

        # Renomear colunas CSV -> Bronze
        df_bronze = df.rename(columns=self.get_column_mapping())

        # Adicionar colunas faltantes (preencher com None)
        colunas_bronze = self.get_bronze_columns()
        for col in colunas_bronze:
            if col not in df_bronze.columns:
                df_bronze[col] = None
                self.logger.debug(f"Coluna '{col}' preenchida com NULL")

        # Formatar datas para padrão Bronze (YYYY-MM-DD)
        df_bronze = self._formatar_datas(df_bronze)

        # Aplicar transformações customizadas (se houver)
        df_bronze = self.transform_custom(df_bronze)

        # Limpar dados: remover linhas completamente vazias
        df_bronze = df_bronze.dropna(how='all')

        # Converter pandas NA/NaT para None (compatível com PostgreSQL)
        df_bronze = df_bronze.replace({pd.NA: None, pd.NaT: None})
        df_bronze = df_bronze.where(pd.notna(df_bronze), None)

        # Converter para lista de listas (formato para inserção)
        registros = df_bronze[colunas_bronze].values.tolist()

        self.logger.success(f"✓ {len(registros):,} registros prontos para inserção")
        return registros, colunas_bronze

    def _formatar_datas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Formata colunas de data para padrão Bronze (YYYY-MM-DD).

        Args:
            df: DataFrame com colunas Bronze

        Returns:
            DataFrame com datas formatadas
        """
        colunas_data = self.get_date_columns()

        if not colunas_data:
            return df  # Nenhuma coluna de data

        self.logger.info("📅 Formatando colunas de data...")

        for col in colunas_data:
            if col in df.columns:
                # Converter para datetime
                df[col] = pd.to_datetime(df[col], errors='coerce')

                # Manter valores válidos, None para inválidos
                df[col] = df[col].where(df[col].notna(), None)

                # Formatar como string YYYY-MM-DD
                df[col] = df[col].apply(
                    lambda x: x.strftime(self.csv_config.date_format)
                    if pd.notna(x) else None
                )

                self.logger.debug(f"✓ Coluna '{col}' formatada como data")

        return df

    # ========================================================================
    # INSERÇÃO NO BANCO DE DADOS
    # ========================================================================

    def inserir_bronze(self, conn, registros: List, colunas: List[str]) -> int:
        """
        Insere dados na Bronze usando estratégia TRUNCATE/RELOAD.

        IMPORTANTE: Bronze usa TRUNCATE/RELOAD, não incremental.
        Todos os dados existentes são removidos antes da inserção.

        Args:
            conn: Conexão com o banco de dados
            registros: Lista de listas com valores a inserir
            colunas: Lista com nomes das colunas

        Returns:
            Número de linhas inseridas
        """
        if not registros:
            self.logger.warning("⚠️  Nenhum registro válido para inserir")
            return 0

        self.logger.info(f"💾 Inserindo {len(registros):,} registros na Bronze...")

        with get_cursor(conn) as cur:
            # Dividir tabela_destino em schema e tabela
            schema, tabela = self.tabela_destino.split('.')

            # TRUNCATE: remover dados existentes (estratégia Bronze)
            truncate_query = sql.SQL("TRUNCATE TABLE {}.{} CASCADE").format(
                sql.Identifier(schema),
                sql.Identifier(tabela)
            )
            self.logger.info(f"🗑️  Truncando tabela {self.tabela_destino}")
            cur.execute(truncate_query)

            # INSERT: inserir novos dados em lote (batch insert otimizado)
            colunas_sql = sql.SQL(', ').join(map(sql.Identifier, colunas))
            insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                sql.Identifier(schema),
                sql.Identifier(tabela),
                colunas_sql
            )

            batch_size = self.etl_config.batch_insert_size
            total_inserido = 0

            # Inserir em lotes para otimizar performance
            for i in range(0, len(registros), batch_size):
                batch = registros[i:i + batch_size]
                execute_values(cur, insert_query, batch, page_size=batch_size)
                total_inserido += len(batch)

                # Log de progresso
                if i + batch_size < len(registros):
                    self.logger.info(f"   • {total_inserido:,} / {len(registros):,}")

        self.logger.success(f"✓ {total_inserido:,} registros inseridos na Bronze")
        return total_inserido

    # ========================================================================
    # ARQUIVAMENTO DE ARQUIVO PROCESSADO
    # ========================================================================

    def mover_para_processed(self) -> None:
        """
        Move arquivo processado para diretório de arquivamento com timestamp.

        Arquivos processados são movidos para data/processed/ com prefixo de data/hora
        para manter histórico de processamentos.
        """
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            arquivo_destino = self.processed_dir / f"{timestamp}_{self.arquivo_nome}"

            shutil.move(str(self.arquivo_path), str(arquivo_destino))

            self.logger.success(f"✓ Arquivo arquivado: {arquivo_destino.name}")

        except Exception as e:
            self.logger.warning(
                f"⚠️  Não foi possível arquivar arquivo: {e}"
            )
            # Não falhar a execução por erro no arquivamento

    # ========================================================================
    # PIPELINE DE EXECUÇÃO PRINCIPAL
    # ========================================================================

    def executar(self) -> int:
        """
        Executa pipeline completo de ingestão CSV -> Bronze.

        Fluxo de execução:
        1. Validar arquivo
        2. Conectar ao banco
        3. Registrar início da execução (auditoria)
        4. Ler CSV
        5. Validar estrutura (colunas)
        6. Validar dados rigorosamente (linha por linha)
        7. Transformar dados válidos para formato Bronze
        8. Inserir na Bronze (TRUNCATE/RELOAD)
        9. Salvar logs de rejeição
        10. Commit transação
        11. Arquivar arquivo processado
        12. Finalizar execução (auditoria)

        Returns:
            0 se sucesso, 1 se erro
        """
        conn = None
        execucao_id = None
        self.start_time = time.time()

        try:
            # ================================================================
            # FASE 1: PREPARAÇÃO
            # ================================================================
            self.logger.info("=" * 80)
            self.logger.info(f"🚀 INICIANDO: {self.script_name}")
            self.logger.info(f"🎯 DESTINO: {self.tabela_destino}")
            self.logger.info(f"📁 ARQUIVO: {self.arquivo_nome}")
            self.logger.info("=" * 80)

            # Validar arquivo existe e é acessível
            self.validar_arquivo()

            # Conectar ao banco de dados
            conn = get_db_connection()
            self.logger.success("✓ Conectado ao banco de dados")

            # Registrar início da execução (auditoria)
            execucao_id = registrar_execucao(
                conn=conn,
                script_nome=self.script_name,
                camada='bronze',
                tabela_origem=None,
                tabela_destino=self.tabela_destino
            )
            self.logger.info(f"✓ Execução registrada: {execucao_id}")

            # Inicializar rejection logger
            self.rejection_logger = RejectionLogger(
                conn=conn,
                execucao_id=execucao_id,
                script_nome=self.script_name,
                tabela_destino=self.tabela_destino
            )

            # ================================================================
            # FASE 2: LEITURA E VALIDAÇÃO
            # ================================================================
            # Ler CSV
            df_raw = self.ler_csv()

            # Validar estrutura (colunas presentes)
            self.validar_colunas_csv(df_raw)

            # Renomear colunas para Bronze (preparar para validação)
            df_bronze = df_raw.rename(columns=self.get_column_mapping())

            # Validar dados rigorosamente (linha por linha)
            df_valido, num_rejeitados = self.validar_e_filtrar_dados(df_bronze)

            # Se não há dados válidos, falhar execução
            if len(df_valido) == 0:
                raise ValueError(
                    "Nenhum registro válido encontrado. "
                    "Todos os registros foram rejeitados. "
                    "Verifique os logs de rejeição."
                )

            # ================================================================
            # FASE 3: TRANSFORMAÇÃO E INSERÇÃO
            # ================================================================
            # Transformar para formato Bronze final
            registros, colunas = self.transformar_para_bronze(df_valido)

            # Inserir na Bronze
            linhas_inseridas = self.inserir_bronze(conn, registros, colunas)

            # Salvar logs de rejeição no banco
            if num_rejeitados > 0:
                self.rejection_logger.salvar_rejeicoes()

            # Commit transação (sucesso!)
            conn.commit()
            self.logger.success("✓ Transação confirmada (COMMIT)")

            # ================================================================
            # FASE 4: FINALIZAÇÃO
            # ================================================================
            # Arquivar arquivo processado
            self.mover_para_processed()

            # Calcular métricas
            duracao = time.time() - self.start_time
            total_processado = len(df_raw)

            # Finalizar execução (auditoria)
            finalizar_execucao(
                conn=conn,
                execucao_id=execucao_id,
                status='sucesso',
                linhas_processadas=total_processado,
                linhas_inseridas=linhas_inseridas,
                linhas_erro=num_rejeitados
            )

            # Log de resumo
            log_execution_summary(
                script_name=self.script_name,
                status='sucesso',
                linhas_processadas=total_processado,
                linhas_inseridas=linhas_inseridas,
                duracao_segundos=duracao
            )

            return 0  # Sucesso

        except Exception as e:
            # ================================================================
            # TRATAMENTO DE ERRO
            # ================================================================
            erro_msg = str(e).replace('{', '{{').replace('}', '}}')
            self.logger.error(f"❌ ERRO: {erro_msg}", exc_info=True)

            duracao = time.time() - self.start_time if self.start_time else 0

            if conn:
                # Rollback transação
                conn.rollback()
                self.logger.warning("⚠️  Transação revertida (ROLLBACK)")

                # Finalizar execução com erro (auditoria)
                if execucao_id:
                    finalizar_execucao(
                        conn=conn,
                        execucao_id=execucao_id,
                        status='erro',
                        mensagem_erro=str(e)
                    )

            # Log de resumo de erro
            log_execution_summary(
                script_name=self.script_name,
                status='erro',
                duracao_segundos=duracao
            )

            return 1  # Erro

        finally:
            # ================================================================
            # LIMPEZA
            # ================================================================
            if conn:
                conn.close()
                self.logger.info("✓ Conexão com banco encerrada")
