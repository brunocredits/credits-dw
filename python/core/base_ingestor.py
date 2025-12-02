import pandas as pd
import sys
import io
import time
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from python.utils.db_connection import get_db_connection, get_cursor
from python.utils.audit import registrar_execucao, finalizar_execucao

# Config
INPUT_DIR = Path("docker/data/input")
PROCESSED_DIR = Path("docker/data/processed")
LOG_DIR = Path("logs")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

class BaseIngestor(ABC):
    """
    Base Ingestor optimized for PostgreSQL COPY command (Bulk Insert).
    RAW-FIRST approach: preserves original data in Bronze layer.
    """

    def __init__(self, name, target_table, mandatory_cols):
        self.name = name
        self.target_table = target_table
        self.error_table = f"bronze.erro_{target_table.split('.')[1]}"
        self.mandatory_cols = mandatory_cols
        
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = LOG_DIR / f"{self.timestamp}_{name}.log"

    @abstractmethod
    def get_column_mapping(self):
        """
        Maps CSV headers to database columns.
        Only needed when CSV header != DB column name.
        """
        pass

    def detect_separator(self, file_path):
        """Auto-detect CSV separator."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline()
        except UnicodeDecodeError:
            with open(file_path, 'r', encoding='latin-1') as f:
                first_line = f.readline()
        
        if ';' in first_line:
            return ';'
        elif ',' in first_line:
            return ','
        elif '\t' in first_line:
            return '\t'
        return ','  # default

    def run(self, file_pattern):
        files = list(INPUT_DIR.glob(file_pattern))
        if not files:
            print(f"[{self.name}] ⚠️  Nenhum arquivo encontrado: {file_pattern}")
            return

        conn = get_db_connection()
        
        for file_path in files:
            print(f"[{self.name}] 📂 {file_path.name}")
            self.process_file(conn, file_path)
            
            # Move to processed
            try:
                import shutil
                dest = PROCESSED_DIR / f"{self.timestamp}_{file_path.name}"
                shutil.move(str(file_path), str(dest))
            except Exception as e:
                print(f"   ⚠️  Erro ao mover arquivo: {e}")

        conn.close()

    def process_file(self, conn, file_path):
        start_time = time.time()
        
        # 1. Auto-detect separator and read file
        try:
            sep = self.detect_separator(file_path)
            if file_path.suffix == '.csv':
                # Try UTF-8 first, fallback to latin-1
                try:
                    df = pd.read_csv(
                        file_path, 
                        sep=sep, 
                        encoding='utf-8', 
                        dtype=str, 
                        engine='c', 
                        on_bad_lines='skip'
                    )
                except UnicodeDecodeError:
                    df = pd.read_csv(
                        file_path, 
                        sep=sep, 
                        encoding='latin-1', 
                        dtype=str, 
                        engine='c', 
                        on_bad_lines='skip'
                    )
            else:
                df = pd.read_excel(file_path, dtype=str)
        except Exception as e:
            print(f"   ❌ Erro na leitura: {e}")
            return

        # 2. Apply column mapping (CSV headers -> DB columns)
        mapping = self.get_column_mapping()
        df = df.rename(columns=mapping)
        
        # Remove duplicate columns (keep first)
        df = df.loc[:, ~df.columns.duplicated()]

        # 3. Validation (Mandatory Columns)
        # Fill missing mandatory columns with None to detect them
        for col in self.mandatory_cols:
            if col not in df.columns:
                df[col] = None

        # Boolean mask: All mandatory cols must be not null and not empty
        valid_mask = pd.Series(True, index=df.index)
        for col in self.mandatory_cols:
            valid_mask &= df[col].notna() & (df[col].astype(str).str.strip() != '')
            
        valid_df = df[valid_mask].copy()
        error_df = df[~valid_mask].copy()

        # 4. Prepare DB Columns
        with get_cursor(conn) as cur:
            cur.execute(f"SELECT * FROM {self.target_table} LIMIT 0")
            # Exclude auto-generated columns
            db_cols = [desc[0] for desc in cur.description 
                      if desc[0] not in ('id', 'data_carga', 'source_filename')]

        # Ensure all DB cols exist in DataFrame (fill missing with None)
        for col in db_cols:
            if col not in valid_df.columns:
                valid_df[col] = None
        
        # Order DataFrame to match DB
        valid_df = valid_df[db_cols]
        
        # Add Metadata
        valid_df['source_filename'] = file_path.name
        
        # 5. COPY to Database (High Performance)
        exec_id = registrar_execucao(conn, f"ingest_{self.name}", "bronze", None, self.target_table)
        
        inserted_count = 0
        error_count = 0
        duration = 0
        
        try:
            if not valid_df.empty:
                inserted_count = self.copy_to_db(conn, valid_df, self.target_table, db_cols + ['source_filename'])

            # 6. Handle Errors (Batch Insert)
            if not error_df.empty:
                error_count = self.insert_errors(conn, error_df, file_path.name)

            # 7. Logging & Metrics (Minimal for Docker)
            duration = time.time() - start_time
            finalizar_execucao(conn, exec_id, "sucesso", len(df), inserted_count, error_count)
            
        except Exception as e:
            duration = time.time() - start_time
            finalizar_execucao(conn, exec_id, "erro", len(df), inserted_count, error_count, str(e))
            print(f"   ❌ Erro crítico: {e}")
            raise
        
        # Console: minimal output
        print(f"   ✓ {inserted_count}/{len(df)} | ✗ {error_count} | {duration:.1f}s")

    def copy_to_db(self, conn, df, table, columns):
        """
        Uses COPY command with copy_expert for better control.
        """
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        with get_cursor(conn) as cur:
            try:
                cols_str = ",".join([f'"{c}"' for c in columns])
                sql = f"COPY {table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
                
                cur.copy_expert(sql, buffer)
                conn.commit()
                return len(df)
            except Exception as e:
                conn.rollback()
                print(f"   ❌ Erro no COPY: {e}")
                return 0

    def insert_errors(self, conn, error_df, filename):
        """
        Inserts errors into the error table.
        """
        error_data = []
        for _, row in error_df.iterrows():
            missing = [c for c in self.mandatory_cols 
                      if pd.isna(row.get(c)) or str(row.get(c)).strip() == '']
            reason = f"Campos obrigatórios vazios: {', '.join(missing)}"
            error_data.append((str(row.to_dict()), reason, filename))
        
        if error_data:
            with get_cursor(conn) as cur:
                from psycopg2.extras import execute_values
                sql = f"INSERT INTO {self.error_table} (linha_original, motivo_descarte, source_filename) VALUES %s"
                execute_values(cur, sql, error_data, page_size=1000)
                conn.commit()
        
        return len(error_data)
