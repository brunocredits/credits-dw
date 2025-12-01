#!/usr/bin/env python3
import sys
from pathlib import Path
import psycopg2
from psycopg2 import sql

sys.path.insert(0, str(Path(__file__).parent))
from utils.db_connection import get_db_connection, get_cursor

def check_tables():
    try:
        conn = get_db_connection()
        with get_cursor(conn) as cur:
            cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema IN ('bronze', 'auditoria')
                ORDER BY table_schema, table_name;
            """)
            tables = cur.fetchall()
            
            print(f"{'SCHEMA':<15} | {'TABLE':<30} | {'ROWS':<10}")
            print("-" * 60)
            
            for schema, table in tables:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                ))
                count = cur.fetchone()[0]
                print(f"{schema:<15} | {table:<30} | {count:<10}")
                
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        if conn: conn.close()

if __name__ == '__main__':
    check_tables()
