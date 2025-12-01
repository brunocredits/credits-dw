#!/usr/bin/env python3
import sys
from pathlib import Path
import psycopg2

sys.path.insert(0, str(Path(__file__).parent))
from utils.db_connection import get_db_connection, get_cursor

def fix_audit_status():
    try:
        conn = get_db_connection()
        with get_cursor(conn) as cur:
            # Fix recent successful runs
            cur.execute("""
                UPDATE auditoria.historico_execucao 
                SET status = 'sucesso', data_fim = CURRENT_TIMESTAMP 
                WHERE status = 'em_execucao' AND data_inicio > '2025-12-01 15:50:00';
            """)
            updated_success = cur.rowcount
            
            # Fix older stuck runs
            cur.execute("""
                UPDATE auditoria.historico_execucao 
                SET status = 'interrompido', data_fim = CURRENT_TIMESTAMP 
                WHERE status = 'em_execucao' AND data_inicio <= '2025-12-01 15:50:00';
            """)
            updated_stuck = cur.rowcount
            
            print(f"Atualizados: {updated_success} para 'sucesso', {updated_stuck} para 'interrompido'.")
            
        conn.commit()
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        if conn: conn.close()

if __name__ == '__main__':
    fix_audit_status()
