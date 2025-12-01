#!/usr/bin/env python3
import sys
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(0, str(Path(__file__).parent))
from utils.db_connection import get_db_connection

def check_audit():
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, script_nome, status, data_inicio, data_fim, linhas_processadas 
                FROM auditoria.historico_execucao 
                ORDER BY data_inicio DESC 
                LIMIT 10;
            """)
            rows = cur.fetchall()
            
            print(f"{'SCRIPT':<25} | {'STATUS':<15} | {'INICIO':<20} | {'FIM':<20}")
            print("-" * 90)
            
            for row in rows:
                start = row['data_inicio'].strftime('%Y-%m-%d %H:%M:%S') if row['data_inicio'] else 'N/A'
                end = row['data_fim'].strftime('%Y-%m-%d %H:%M:%S') if row['data_fim'] else 'Em Execução'
                print(f"{row['script_nome']:<25} | {row['status']:<15} | {start:<20} | {end:<20}")
                
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        if conn: conn.close()

if __name__ == '__main__':
    check_audit()
