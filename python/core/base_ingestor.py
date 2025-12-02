"""
BaseIngestor - Classe base para ingestão de dados na camada Bronze
Implementa padrão RAW-FIRST com validação e limpeza de dados
Otimizado para PostgreSQL COPY (bulk insert de alta performance)
"""

import pandas as pd
import sys
import io
import time
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod

# Adiciona raiz do projeto ao path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from python.utils.db_connection import get_db_connection, get_cursor
from python.utils.audit import registrar_execucao, finalizar_execucao
from python.core.data_cleaner import DataCleaner
from python.core.file_handler import FileHandler
from python.core.validator import Validator

# Configuração de diretórios
INPUT_DIR = Path("docker/data/input")
PROCESSED_DIR = Path("docker/data/processed")
TEMPLATE_DIR = Path("docker/data/templates")
LOG_DIR = Path("logs")

# Cria diretórios se não existirem
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

class BaseIngestor(ABC):
    """
    Ingestor base otimizado para carga em massa via PostgreSQL COPY
    Arquitetura modular seguindo princípios SOLID
    """

    def __init__(self, name, target_table, mandatory_cols):
        """Inicializa ingestor com configurações básicas"""
        self.name = name
        self.target_table = target_table
        self.mandatory_cols = mandatory_cols
        
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = LOG_DIR / f"{self.timestamp}_{name}.log"
        
        # Componentes modulares (SRP)
        self.file_handler = FileHandler(PROCESSED_DIR)
        self.validator = Validator(TEMPLATE_DIR)

    @abstractmethod
    def get_column_mapping(self):
        """Mapeia headers CSV para colunas do banco (implementado por subclasses)"""
        pass

    def check_duplicate(self, conn, file_hash):
        """Verifica se arquivo já foi processado com sucesso (por hash MD5)"""
        with get_cursor(conn) as cur:
            cur.execute("""
                SELECT 1 FROM auditoria.historico_execucao 
                WHERE file_hash = %s AND status = 'sucesso'
                LIMIT 1
            """, (file_hash,))
            return cur.fetchone() is not None

    def run(self, file_pattern):
        """
        Executa pipeline de ingestão para arquivos que correspondem ao pattern
        Exemplo: run("faturamento*.csv")
        """
        files = list(INPUT_DIR.glob(file_pattern))
        if not files:
            print(f"[{self.name}] ⚠️  Nenhum arquivo encontrado: {file_pattern}")
            return

        conn = get_db_connection()
        
        for file_path in files:
            print(f"[{self.name}] 📂 {file_path.name}")
            is_duplicate = self.process_file(conn, file_path)
            
            # Move arquivo para pasta processados (ou duplicates)
            try:
                dest = self.file_handler.move_to_processed(file_path, is_duplicate=is_duplicate)
                print(f"   📂 Arquivado em: {dest.relative_to(PROCESSED_DIR)}")
            except Exception as e:
                print(f"   ⚠️  Erro ao mover arquivo: {e}")

        conn.close()

    def process_file(self, conn, file_path):
        """
        Processa um arquivo individual
        Retorna True se duplicado, False caso contrário
        """
        start_time = time.time()
        
        # 1. Verifica duplicidade por hash
        file_hash = self.file_handler.calculate_hash(file_path)
        if self.check_duplicate(conn, file_hash):
            print(f"   ⚠️  Arquivo duplicado detectado (Hash: {file_hash}).")
            return True

        # 2. Lê arquivo CSV/Excel
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
            print(f"   ❌ Erro na leitura: {e}")
            return False

        # 3. Valida headers contra template
        try:
            self.validator.validate_headers(self.name, df.columns)
        except ValueError as e:
            print(f"   ❌ {e}")
            exec_id = registrar_execucao(conn, f"ingest_{self.name}", "bronze", 
                                        None, self.target_table, file_hash)
            finalizar_execucao(conn, exec_id, "erro", 0, 0, 0, 0, str(e))
            return False

        # 4. Aplica mapeamento de colunas e remove duplicatas
        mapping = self.get_column_mapping()
        df = df.rename(columns=mapping)
        df = df.loc[:, ~df.columns.duplicated()]

        # 5. Valida campos obrigatórios
        for col in self.mandatory_cols:
            if col not in df.columns: 
                df[col] = None
            
        valid_mask = pd.Series(True, index=df.index)
        for col in self.mandatory_cols:
            valid_mask &= df[col].notna() & (df[col].astype(str).str.strip() != '')
            
        valid_df = df[valid_mask].copy()
        error_df = df[~valid_mask].copy()

        # 6. Obtém metadados do banco (colunas, tipos)
        with get_cursor(conn) as cur:
            cur.execute(f"SELECT * FROM {self.target_table} LIMIT 0")
            db_cols = [desc[0] for desc in cur.description 
                      if desc[0] not in ('id', 'data_carga', 'source_filename')]
            numeric_cols = [desc[0] for desc in cur.description if desc[1] in (1700, 700, 701)]
            date_cols = [desc[0] for desc in cur.description if desc[1] in (1082, 1114, 1184)]

        # Garante que todas as colunas do banco existem no DataFrame
        for col in db_cols:
            if col not in valid_df.columns: 
                valid_df[col] = None

        # 7. Limpa e valida colunas numéricas
        for col in numeric_cols:
            if col in valid_df.columns:
                original = valid_df[col].copy()
                cleaned = DataCleaner.clean_numeric(valid_df[col])
                
                # Identifica valores que falharam na conversão
                failed = DataCleaner.identify_errors(valid_df[col], cleaned)
                if failed.any():
                    print(f"   ⚠️  {failed.sum()} valores numéricos inválidos em '{col}'")
                    
                    # Move linhas com erro para error_df
                    rejected_indices = valid_df[failed].index
                    new_errors = valid_df.loc[rejected_indices].copy()
                    new_errors['_custom_error'] = f"Valor numérico inválido em '{col}': " + original[failed].astype(str)
                    
                    error_df = pd.concat([error_df, new_errors]) if not error_df.empty else new_errors
                    valid_df = valid_df[~failed]
                    cleaned = cleaned[~failed]
                
                valid_df[col] = cleaned

        # 8. Limpa e valida colunas de data
        for col in date_cols:
            if col in valid_df.columns:
                original = valid_df[col].copy()
                cleaned = DataCleaner.clean_date(valid_df[col])
                
                # Identifica datas inválidas
                failed = DataCleaner.identify_errors(valid_df[col], cleaned)
                if failed.any():
                    print(f"   ⚠️  {failed.sum()} datas inválidas em '{col}'")
                    
                    # Move linhas com erro para error_df
                    rejected_indices = valid_df[failed].index
                    new_errors = valid_df.loc[rejected_indices].copy()
                    new_errors['_custom_error'] = f"Data inválida em '{col}': " + original[failed].astype(str)
                    
                    error_df = pd.concat([error_df, new_errors]) if not error_df.empty else new_errors
                    valid_df = valid_df[~failed]
                    cleaned = cleaned[~failed]

                # Formata datas para ISO (YYYY-MM-DD)
                valid_df[col] = cleaned.dt.strftime('%Y-%m-%d')
                valid_df.loc[cleaned.isna(), col] = None

        # 9. Prepara DataFrame para carga (ordena colunas + metadata)
        valid_df = valid_df[db_cols].copy()
        valid_df['source_filename'] = file_path.name
        
        # 10. Registra execução na auditoria
        exec_id = registrar_execucao(conn, f"ingest_{self.name}", "bronze", 
                                     None, self.target_table, file_hash)
        
        inserted_count = 0
        error_count = 0
        
        try:
            # Idempotência: remove dados anteriores do mesmo arquivo
            with get_cursor(conn) as cur:
                cur.execute(f"DELETE FROM {self.target_table} WHERE source_filename = %s", 
                           (file_path.name,))
            
            # Carga em massa via COPY (alta performance)
            if not valid_df.empty:
                inserted_count = self.copy_to_db(conn, valid_df, self.target_table, 
                                                db_cols + ['source_filename'])

            # Registra erros na auditoria
            if not error_df.empty:
                error_count = self.insert_errors(conn, error_df, file_path.name, exec_id)

            # Finaliza execução com sucesso
            duration = time.time() - start_time
            finalizar_execucao(conn, exec_id, "sucesso", len(df), inserted_count, 0, error_count)
            print(f"   ✓ {inserted_count}/{len(df)} | ✗ {error_count} | {duration:.1f}s")
            
        except Exception as e:
            # Registra falha na auditoria
            duration = time.time() - start_time
            finalizar_execucao(conn, exec_id, "erro", len(df), inserted_count, 0, error_count, str(e))
            print(f"   ❌ Erro crítico: {e}")
            raise

        return False

    def copy_to_db(self, conn, df, table, columns):
        """
        Executa carga em massa via PostgreSQL COPY
        Método mais rápido para inserção de grandes volumes
        """
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        with get_cursor(conn) as cur:
            try:
                # Configura formato de data para DD/MM/YYYY
                cur.execute("SET datestyle = 'ISO, DMY';")
                
                # Monta comando COPY com colunas especificadas
                cols_str = ",".join([f'"{c}"' for c in columns])
                sql = f"COPY {table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
                
                cur.copy_expert(sql, buffer)
                conn.commit()
                return len(df)
            except Exception as e:
                conn.rollback()
                print(f"   ❌ Erro no COPY: {e}")
                return 0

    def insert_errors(self, conn, error_df, filename, exec_id):
        """Registra linhas rejeitadas na tabela de auditoria"""
        error_data = []
        
        for idx, row in error_df.iterrows():
            # Prioriza mensagem de erro customizada (limpeza de dados)
            if '_custom_error' in row and pd.notna(row['_custom_error']):
                motivo = row['_custom_error']
                campo_falha = 'data_cleaning'
            else:
                # Erro de campo obrigatório
                missing = [c for c in self.mandatory_cols 
                          if pd.isna(row.get(c)) or str(row.get(c)).strip() == '']
                campo_falha = ', '.join(missing) if missing else 'unknown'
                motivo = f"Campos obrigatórios vazios: {campo_falha}"
            
            error_data.append((
                exec_id, f"ingest_{self.name}", self.target_table, idx + 2,
                campo_falha, motivo, None, str(row.to_dict()), 'ERROR'
            ))
        
        # Insere erros em batch (otimizado)
        if error_data:
            with get_cursor(conn) as cur:
                from psycopg2.extras import execute_values
                sql = """
                    INSERT INTO auditoria.log_rejeicao 
                    (execucao_fk, script_nome, tabela_destino, numero_linha, 
                     campo_falha, motivo_rejeicao, valor_recebido, registro_completo, severidade)
                    VALUES %s
                """
                execute_values(cur, sql, error_data, page_size=1000)
                conn.commit()
        
        return len(error_data)
