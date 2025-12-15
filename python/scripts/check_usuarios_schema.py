import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from python.utils.db_connection import get_connection, get_cursor

def check_usuarios_schema():
    """Check the current schema of bronze.usuarios table"""
    try:
        with get_connection() as conn:
            with get_cursor(conn) as cur:
                # Get column information
                cur.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'bronze' 
                    AND table_name = 'usuarios'
                    ORDER BY ordinal_position;
                """)
                
                print("=" * 80)
                print("BRONZE.USUARIOS TABLE SCHEMA")
                print("=" * 80)
                print(f"{'Column Name':<30} {'Data Type':<20} {'Nullable':<10} {'Default':<20}")
                print("-" * 80)
                
                for row in cur.fetchall():
                    col_name, data_type, is_nullable, col_default = row
                    default_str = str(col_default)[:18] if col_default else ''
                    print(f"{col_name:<30} {data_type:<20} {is_nullable:<10} {default_str:<20}")
                
                print("=" * 80)
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_usuarios_schema()
