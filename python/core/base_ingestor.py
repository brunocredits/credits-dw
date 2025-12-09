"""
Este módulo define o `BaseIngestor`, uma classe abstrata para ingestão de dados
para a camada Bronze do data warehouse. Ele fornece uma estrutura robusta para
leitura, validação, limpeza e carga de dados em massa usando o comando `COPY`
do PostgreSQL, garantindo alta performance.

Principais funcionalidades:
- Detecção de arquivos duplicados através de hash MD5.
- Validação de cabeçalhos contra templates pré-definidos.
- Limpeza de dados numéricos e de data.
- Registro de auditoria detalhado para cada execução.
- Log de linhas rejeitadas com motivos claros.
- Movimentação automática de arquivos processados.
"""

import pandas as pd
import sys
import io
import re
import time
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod

# Adiciona o diretório raiz do projeto ao sys.path para importações relativas
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from python.utils.db_connection import get_db_connection, get_cursor
from python.utils.audit import registrar_execucao, finalizar_execucao
from python.core.data_cleaner import DataCleaner
from python.core.file_handler import FileHandler
from python.core.validator import Validator

# Definição dos diretórios padrão
INPUT_DIR = Path("docker/data/input")
PROCESSED_DIR = Path("docker/data/processed")
TEMPLATE_DIR = Path("docker/data/templates")
LOG_DIR = Path("logs")

# Cria os diretórios se não existirem
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

class BaseIngestor(ABC):
    """
    Classe base abstrata para ingestores da camada Bronze.
    
    Esta classe implementa o fluxo principal de ingestão, incluindo:
    - Leitura de arquivos (CSV, Excel).
    - Validação de estrutura e dados.
    - Carga otimizada no banco de dados via `COPY`.
    - Auditoria e logging.
    
    As subclasses devem implementar `get_column_mapping`.
    """

    def __init__(self, name, target_table, mandatory_cols):
        """
        Inicializa o ingestor.

        Args:
            name (str): Nome do ingestor (ex: "faturamento").
            target_table (str): Tabela de destino no banco (ex: "bronze.faturamento").
            mandatory_cols (list): Lista de colunas obrigatórias.
        """
        self.name = name
        self.target_table = target_table
        self.mandatory_cols = mandatory_cols
        
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = LOG_DIR / f"{self.timestamp}_{name}.log"
        
        self.file_handler = FileHandler(PROCESSED_DIR)
        self.validator = Validator(TEMPLATE_DIR)

    @abstractmethod
    def get_column_mapping(self):
        """
        Método abstrato que deve ser implementado pelas subclasses.
        
        Retorna um dicionário que mapeia os nomes das colunas do arquivo de origem
        para os nomes das colunas da tabela de destino no banco de dados.
        """
        pass

    def check_duplicate(self, conn, file_hash):
        """
        Verifica no log de auditoria se um arquivo com o mesmo hash já foi 
        processado com sucesso.

        Args:
            conn: Conexão com o banco de dados.
            file_hash (str): Hash MD5 do arquivo.

        Returns:
            bool: True se o arquivo for duplicado, False caso contrário.
        """
        with get_cursor(conn) as cur:
            cur.execute("""
                SELECT 1 FROM auditoria.historico_execucao 
                WHERE file_hash = %s AND status = 'sucesso'
                LIMIT 1
            """, (file_hash,))
            return cur.fetchone() is not None

    def run(self, file_pattern):
        """
        Orquestra a execução do pipeline de ingestão para um padrão de arquivo.

        Args:
            file_pattern (str): Padrão de nome de arquivo (glob) a ser procurado 
                                no diretório de entrada.
        """
        files = list(INPUT_DIR.glob(file_pattern))
        if not files:
            print(f"[{self.name}] ⚠️  Nenhum arquivo encontrado para o padrão: {file_pattern}")
            return

        conn = get_db_connection()
        
        for file_path in files:
            print(f"[{self.name}] 📂 Processando: {file_path.name}")
            is_duplicate = self.process_file(conn, file_path)
            
            try:
                dest = self.file_handler.move_to_processed(file_path, is_duplicate=is_duplicate)
                print(f"   📂 Arquivo movido para: {dest.relative_to(PROCESSED_DIR)}")
            except Exception as e:
                print(f"   ⚠️  Erro ao mover o arquivo: {e}")

        conn.close()

    def process_file(self, conn, file_path):
        """
        Processa um único arquivo, desde a leitura até a carga no banco.

        Args:
            conn: Conexão com o banco de dados.
            file_path (Path): Caminho do arquivo a ser processado.

        Returns:
            bool: True se o arquivo for uma duplicata, False caso contrário.
        """
        start_time = time.time()
        
        file_hash = self.file_handler.calculate_hash(file_path)
        if self.check_duplicate(conn, file_hash):
            print(f"   ⚠️  Arquivo duplicado detectado (Hash: {file_hash}). O arquivo não será reprocessado.")
            return True

        try:
            sep = self.file_handler.detect_separator(file_path)
            if file_path.suffix == '.csv':
                try:
                    df = pd.read_csv(file_path, sep=sep, encoding='utf-8', dtype=str, 
                                    engine='c', on_bad_lines='skip')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, sep=sep, encoding='latin-1', dtype=str, 
                                    engine='c', on_bad_lines='skip')
            else:
                df = pd.read_excel(file_path, dtype=str)
        except Exception as e:
            print(f"   ❌ Erro fatal na leitura do arquivo: {e}")
            return False

        try:
            self.validator.validate_headers(self.name, df.columns)
        except ValueError as e:
            print(f"   ❌ {e}")
            exec_id = registrar_execucao(conn, f"ingest_{self.name}", "bronze", 
                                        file_path.name, self.target_table, file_hash)
            finalizar_execucao(conn, exec_id, "erro", 0, 0, 0, 0, str(e))
            return False

        mapping = self.get_column_mapping()
        df = df.rename(columns=mapping)
        df = df.loc[:, ~df.columns.duplicated()]

        # Garante que todas as colunas obrigatórias existam no DataFrame
        # Se uma coluna obrigatória não existir, ela é criada com valores nulos
        for col in self.mandatory_cols:
            if col not in df.columns: 
                df[col] = None
            
        # Estratégia de validação: separar warnings (campos obrigatórios vazios) de errors (tipos inválidos)
        # - valid_df: linhas que serão inseridas no banco (podem ter campos vazios)
        # - error_df: linhas rejeitadas por erros de conversão de tipo (numérico/data)
        # - warning_log_entries: registros de campos obrigatórios vazios (não bloqueiam ingestão)
        valid_df = df.copy()
        error_df = pd.DataFrame(columns=df.columns)  # Exclusivo para erros do DataCleaner

        warning_log_entries = []
        # Cria máscara booleana para identificar linhas com campos obrigatórios vazios
        rejected_by_mandatory_mask = pd.Series(False, index=df.index)
        # Identifica todas as linhas que possuem pelo menos um campo obrigatório vazio
        for col in self.mandatory_cols:
            missing_mask = df[col].isna() | (df[col].astype(str).str.strip() == '')
            rejected_by_mandatory_mask |= missing_mask  # OR lógico para acumular violações
        
        # Para cada linha com campos obrigatórios faltantes, cria um warning log
        # Importante: estas linhas NÃO são rejeitadas, apenas avisadas (WARN vs ERROR)
        for idx in df[rejected_by_mandatory_mask].index:
            row_data = df.loc[idx].to_dict()
            # Identifica exatamente quais campos obrigatórios estão vazios nesta linha
            missing_cols_in_row = [c for c in self.mandatory_cols 
                                   if pd.isna(row_data.get(c)) or str(row_data.get(c)).strip() == '']
            
            if missing_cols_in_row:
                warning_log_entries.append({
                    'script_nome': f"ingest_{self.name}",
                    'tabela_destino': self.target_table,
                    'numero_linha': idx + 2,  # +2: +1 para header, +1 para indexação começar em 1
                    'campo_falha': ', '.join(missing_cols_in_row),
                    'motivo_rejeicao': f"Campos obrigatórios vazios: {', '.join(missing_cols_in_row)}",
                    'valor_recebido': None,
                    'registro_completo': str(row_data),
                    'severidade': 'WARN',  # WARN = não bloqueia ingestão, apenas registra
                    'execucao_fk': None  # Será preenchido com exec_id após registro da execução
                })


        with get_cursor(conn) as cur:
            cur.execute(f"SELECT * FROM {self.target_table} LIMIT 0")
            db_cols_info = {desc[0]: desc[1] for desc in cur.description}
            
            db_cols = [desc[0] for desc in cur.description 
                       if desc[0] not in ('id', 'data_carga', 'source_filename')]
            numeric_cols = [name for name, oid in db_cols_info.items() if oid in (1700, 700, 701)]
            date_cols = [name for name, oid in db_cols_info.items() if oid in (1082, 1114, 1184)]

        # Garante que o DataFrame tenha todas as colunas do banco
        for col in db_cols:
            if col not in valid_df.columns: 
                valid_df[col] = None

        # === LIMPEZA E VALIDAÇÃO DE TIPOS DE DADOS ===
        # Esta seção é crítica: converte formatos brasileiros para padrão SQL
        # e REJEITA linhas com valores inválidos (diferente de warnings acima)
        
        # Processamento de colunas numéricas: 1.000,50 → 1000.50
        for col in numeric_cols:
            if col in valid_df.columns:
                original = valid_df[col].copy()
                cleaned = DataCleaner.clean_numeric(valid_df[col])  # Remove pontos, troca vírgula por ponto
                
                # Identifica valores que falharam na conversão (ex: texto em campo numérico)
                failed = DataCleaner.identify_errors(original, cleaned)
                if failed.any():
                    print(f"   ⚠️  {failed.sum()} valores numéricos inválidos encontrados na coluna '{col}'")
                    
                    # Move linhas com erro para error_df (serão logadas com severidade ERROR)
                    rejected_indices = valid_df[failed].index
                    new_errors = valid_df.loc[rejected_indices].copy()
                    new_errors['_custom_error'] = f"Valor numérico inválido em '{col}': " + original[failed].astype(str)
                    
                    error_df = pd.concat([error_df, new_errors]) if not error_df.empty else new_errors
                    valid_df = valid_df.drop(rejected_indices)  # Remove do DataFrame válido
                    cleaned = cleaned.drop(rejected_indices)
                
                valid_df[col] = cleaned  # Substitui coluna original pela versão limpa

        # Processamento de colunas de data: DD/MM/YYYY → YYYY-MM-DD (ISO format)
        for col in date_cols:
            if col in valid_df.columns:
                original = valid_df[col].copy()
                cleaned = DataCleaner.clean_date(valid_df[col])  # Converte para datetime
                
                # Identifica datas inválidas (ex: "32/13/2023" ou texto em campo de data)
                failed = DataCleaner.identify_errors(original, cleaned)
                if failed.any():
                    print(f"   ⚠️  {failed.sum()} datas inválidas encontradas na coluna '{col}'")
                    
                    # Move linhas com erro de data para error_df
                    rejected_indices = valid_df[failed].index
                    new_errors = valid_df.loc[rejected_indices].copy()
                    new_errors['_custom_error'] = f"Data inválida em '{col}': " + original[failed].astype(str)
                    
                    error_df = pd.concat([error_df, new_errors]) if not error_df.empty else new_errors
                    valid_df = valid_df.drop(rejected_indices)
                    cleaned = cleaned.drop(rejected_indices)

                # Formata datas para string ISO (PostgreSQL aceita diretamente)
                valid_df[col] = cleaned.dt.strftime('%Y-%m-%d')
                valid_df.loc[cleaned.isna(), col] = None  # Mantém NaT como NULL

        valid_df = valid_df[db_cols].copy()
        valid_df['source_filename'] = file_path.name
        
        
        exec_id = registrar_execucao(conn, f"ingest_{self.name}", "bronze", 
                                     file_path.name, self.target_table, file_hash)
        
        # Assign exec_id to warning_log_entries
        for entry in warning_log_entries:
            entry['execucao_fk'] = exec_id

        inserted_count = 0
        total_logged_entries = 0 # To count both warnings and errors

        try:
            # Limpa dados antigos do mesmo arquivo para evitar duplicatas
            with get_cursor(conn) as cur:
                cur.execute(f"DELETE FROM {self.target_table} WHERE source_filename = %s", 
                           (file_path.name,))
            
            if not valid_df.empty:
                inserted_count = self.copy_to_db(conn, valid_df, self.target_table, 
                                                db_cols + ['source_filename'])

            # Prepare and insert DataCleaner errors
            data_cleaner_error_entries = self._prepare_data_cleaner_error_entries(error_df, file_path.name, exec_id)
            total_logged_entries += self.insert_log_entries(conn, data_cleaner_error_entries, exec_id)

            # Insert mandatory column warnings
            total_logged_entries += self.insert_log_entries(conn, warning_log_entries, exec_id)

            duration = time.time() - start_time
            finalizar_execucao(conn, exec_id, "sucesso", len(df), inserted_count, 0, total_logged_entries)
            print(f"   ✓ Inseridos: {inserted_count}/{len(df)} | ⚠️/❌ Logs: {total_logged_entries} | ⏱️ {duration:.1f}s")
            
        except Exception as e:
            conn.rollback()
            duration = time.time() - start_time
            finalizar_execucao(conn, exec_id, "erro", len(df), inserted_count, 0, total_logged_entries, str(e))
            print(f"   ❌ Erro crítico durante a carga no banco: {e}")
            raise

        return False

    def copy_to_db(self, conn, df, table, columns):
        """
        Realiza a carga em massa de um DataFrame para uma tabela no PostgreSQL
        usando o comando `COPY FROM STDIN`.

        Args:
            conn: Conexão com o banco de dados.
            df (pd.DataFrame): DataFrame com os dados a serem inseridos.
            table (str): Nome da tabela de destino.
            columns (list): Lista de colunas do DataFrame a serem inseridas.

        Returns:
            int: Número de linhas inseridas.
        """
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        with get_cursor(conn) as cur:
            try:
                # Garante que o estilo de data seja compatível com o formato do DataFrame
                cur.execute("SET datestyle = 'ISO, DMY';")
                
                cols_str = ",".join([f'"{c}"' for c in columns])
                sql = f"COPY {table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
                
                cur.copy_expert(sql, buffer)
                conn.commit()
                return len(df)
            except Exception as e:
                conn.rollback()
                print(f"   ❌ Erro durante a operação de COPY: {e}")
                return 0

    def insert_log_entries(self, conn, log_entries, exec_id):
        """
        Registra as entradas de log (warnings ou errors) em uma tabela de auditoria.

        Args:
            conn: Conexão com o banco de dados.
            log_entries (list): Lista de dicionários, cada um representando uma entrada de log.
            exec_id (UUID): ID da execução atual.

        Returns:
            int: Número de entradas de log inseridas.
        """
        if not log_entries:
            return 0
        
        # Fill exec_id for all entries
        for entry in log_entries:
            entry['execucao_fk'] = exec_id
            
        # Convert list of dicts to list of tuples for execute_values
        columns = ['execucao_fk', 'script_nome', 'tabela_destino', 'numero_linha', 
                   'campo_falha', 'motivo_rejeicao', 'valor_recebido', 'registro_completo', 'severidade']
        
        values = [[entry[col] for col in columns] for entry in log_entries]

        with get_cursor(conn) as cur:
            from psycopg2.extras import execute_values
            sql = f"""
                INSERT INTO auditoria.log_rejeicao 
                ({', '.join(columns)})
                VALUES %s
            """
            execute_values(cur, sql, values, page_size=1000)
            conn.commit()
        
        return len(log_entries)

    def _prepare_data_cleaner_error_entries(self, error_df, filename, exec_id):
        """
        Prepara as entradas de log para linhas que continham erros do DataCleaner.

        Args:
            error_df (pd.DataFrame): DataFrame contendo as linhas com erro.
            filename (str): Nome do arquivo de origem.
            exec_id (UUID): ID da execução atual.

        Returns:
            list: Lista de dicionários, cada um representando uma entrada de log de erro.
        """
        error_data = []
        
        for idx, row in error_df.iterrows():
            motivo = 'Erro de limpeza de dados'
            campo_falha = 'data_cleaning'
            if '_custom_error' in row and pd.notna(row['_custom_error']):
                motivo = row['_custom_error']
                # Try to extract the failing field from the custom error message
                match = re.search(r"'(.*?)'", motivo)
                if match:
                    campo_falha = match.group(1)

            error_data.append({
                'execucao_fk': exec_id,
                'script_nome': f"ingest_{self.name}",
                'tabela_destino': self.target_table,
                'numero_linha': idx + 2, # +2 for 0-indexed and header
                'campo_falha': campo_falha,
                'motivo_rejeicao': motivo,
                'valor_recebido': None,
                'registro_completo': str(row.to_dict()),
                'severidade': 'ERROR'
            })
        
        return error_data

