import pandas as pd
import unicodedata
import re
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
    Removes overhead of row-by-row processing.
    """

    def __init__(self, name, target_table, mandatory_cols, date_cols=None, money_cols=None):
        self.name = name
        self.target_table = target_table
        self.error_table = f"bronze.erro_{target_table.split('.')[1]}"
        self.mandatory_cols = mandatory_cols
        self.date_cols = date_cols or []
        self.money_cols = money_cols or []
        
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = LOG_DIR / f"{self.timestamp}_{name}.log"

    @abstractmethod
    def get_column_mapping(self):
        pass

    def normalize_header(self, col):
        if not isinstance(col, str): return str(col)
        text = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('ASCII').lower()
        text = re.sub(r'[^a-z0-9]+', '_', text).strip('_')
        if text == 'set': return 'set_'
        if text == 'out': return 'out_'
        return text

    def clean_dates(self, df):
        for col in self.date_cols:
            if col in df.columns:
                # Coerce errors to NaT, then format to YYYY-MM-DD
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
        return df

    def clean_money(self, df):
        for col in self.money_cols:
            if col in df.columns:
                # Vectorized string cleanup
                df[col] = df[col].astype(str).str.replace('R$', '', regex=False)\
                                             .str.replace(' ', '')\
                                             .str.replace('.', '')\
                                             .str.replace(',', '.')
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def run(self, file_pattern):
        files = list(INPUT_DIR.glob(file_pattern))
        if not files:
            print(f"[{self.name}] Nenhum arquivo encontrado para padrão: {file_pattern}")
            return

        conn = get_db_connection()
        
        for file_path in files:
            print(f"[{self.name}] Processando: {file_path.name}...")
            self.process_file(conn, file_path)
            
            # Move to processed
            try:
                dest = PROCESSED_DIR / f"{self.timestamp}_{file_path.name}"
                file_path.rename(dest)
            except Exception as e:
                print(f"Erro ao mover arquivo: {e}")

        conn.close()

    def process_file(self, conn, file_path):
        start_time = time.time()
        
        # 1. Read File (Optimized)
        try:
            if file_path.suffix == '.csv':
                # Engine 'c' is faster
                df = pd.read_csv(file_path, sep=';', encoding='utf-8', dtype=str, engine='c', on_bad_lines='skip')
            else:
                df = pd.read_excel(file_path, dtype=str)
        except Exception as e:
            print(f"Erro leitura critico: {e}")
            return

        # 2. Normalize Headers
        df.columns = [self.normalize_header(c) for c in df.columns]
        
        # Deduplicate columns
        seen = {}
        new_cols = []
        for c in df.columns:
            if c in seen:
                seen[c] += 1
                new_cols.append(f"{c}_{seen[c]}")
            else:
                seen[c] = 0
                new_cols.append(c)
        df.columns = new_cols

        # 3. Rename Mappings
        df = df.rename(columns=self.get_column_mapping())
        
        # Keep only unique columns
        df = df.loc[:, ~df.columns.duplicated()]

        # 4. Data Cleaning (Vectorized)
        df = self.clean_dates(df)
        df = self.clean_money(df)

        # 5. Validation (Mandatory Cols)
        # Fill missing mandatory columns with None/NaN to detect them
        for col in self.mandatory_cols:
            if col not in df.columns:
                df[col] = None

        # Boolean mask for valid rows: All mandatory cols must be not null and not empty string
        valid_mask = pd.Series(True, index=df.index)
        for col in self.mandatory_cols:
            valid_mask &= df[col].notna() & (df[col].astype(str).str.strip() != '')
            
        valid_df = df[valid_mask].copy()
        error_df = df[~valid_mask].copy()

        # 6. Prepare DB Columns
        with get_cursor(conn) as cur:
            cur.execute(f"SELECT * FROM {self.target_table} LIMIT 0")
            # Exclude auto-generated or metadata columns from schema check
            db_cols = [desc[0] for desc in cur.description if desc[0] not in ('id', 'data_carga', 'source_filename')]

        # Ensure all DB cols exist in DataFrame (fill missing with None)
        for col in db_cols:
            if col not in valid_df.columns:
                valid_df[col] = None
        
        # Order DataFrame matches DB
        valid_df = valid_df[db_cols]
        
        # Add Metadata
        valid_df['source_filename'] = file_path.name
        
        # 7. COPY to Database (High Performance)
        exec_id = registrar_execucao(conn, f"ingest_{self.name}", "bronze", None, self.target_table)
        
        inserted_count = 0
        if not valid_df.empty:
            inserted_count = self.copy_to_db(conn, valid_df, self.target_table, db_cols + ['source_filename'])

        # 8. Handle Errors (Batch Insert)
        error_count = 0
        if not error_df.empty:
            error_count = self.insert_errors(conn, error_df, file_path.name)

        # 9. Logging & Metrics
        duration = time.time() - start_time
        finalizar_execucao(conn, exec_id, "sucesso", len(df), inserted_count, error_count)
        
        with open(self.log_file, "a") as f:
            f.write(f"Arquivo: {file_path.name} | Total: {len(df)} | Valid: {inserted_count} | Erros: {error_count} | Tempo: {duration:.2f}s\n")
            if not error_df.empty:
                 f.write(f"Exemplos de erros (Missing {self.mandatory_cols}):\n")
                 f.write(error_df.head(5).to_string())
        
        print(f"   -> Sucesso: {inserted_count} | Erros: {error_count} | Tempo: {duration:.2f}s")

    def copy_to_db(self, conn, df, table, columns):
        """
        Uses COPY command with copy_expert for better control.
        """
        # Prepare buffer
        buffer = io.StringIO()
        # Write CSV to buffer
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        with get_cursor(conn) as cur:
            try:
                # Construct SQL manually to ensure correct quoting if needed
                # If table has quotes, use as is, otherwise let it be
                cols_str = ",".join([f'"{c}"' for c in columns])
                sql = f"COPY {table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
                
                cur.copy_expert(sql, buffer)
                conn.commit()
                return len(df)
            except Exception as e:
                conn.rollback()
                print(f"Erro no COPY: {e}")
                return 0

    def insert_errors(self, conn, error_df, filename):
        """
        Inserts errors into the error table.
        """
        error_data = []
        for _, row in error_df.iterrows():
            # Determine reason
            missing = [c for c in self.mandatory_cols if pd.isna(row.get(c)) or str(row.get(c)).strip() == '']
            reason = f"Faltando obrigatórios: {', '.join(missing)}"
            error_data.append((str(row.to_dict()), reason, filename))
        
        if error_data:
            with get_cursor(conn) as cur:
                from psycopg2.extras import execute_values
                sql = f"INSERT INTO {self.error_table} (linha_original, motivo_descarte, source_filename) VALUES %s"
                execute_values(cur, sql, error_data, page_size=1000)
                conn.commit()
        
        return len(error_data)
