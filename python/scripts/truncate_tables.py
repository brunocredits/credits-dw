
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from python.utils.db_connection import get_db_connection

def truncate_tables():
    conn = get_db_connection()
    with conn.cursor() as cur:
        sql_path = Path(__file__).resolve().parent / 'truncate_tables.sql'
        with open(sql_path, 'r') as f:
            cur.execute(f.read())
    conn.commit()
    conn.close()

if __name__ == "__main__":
    truncate_tables()
