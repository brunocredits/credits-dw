"""
Script para truncar (limpar) todas as tabelas da camada Bronze.

Este script:
- Remove todos os dados das tabelas Bronze (exceto dim_data)
- Limpa as tabelas de auditoria
- Reseta os contadores de ID (IDENTITY)
- Mantém a estrutura das tabelas intacta
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from python.utils.db_connection import get_connection, get_cursor

# SQL para truncar tabelas Bronze
TRUNCATE_SQL = """
BEGIN;

-- 1. Limpar tabelas de auditoria primeiro (por causa das FKs)
TRUNCATE TABLE auditoria.log_rejeicao CASCADE;
TRUNCATE TABLE auditoria.historico_execucao RESTART IDENTITY CASCADE;

-- 2. Limpar tabelas Bronze (dados de ingestão)
TRUNCATE TABLE bronze.base_oficial RESTART IDENTITY CASCADE;
TRUNCATE TABLE bronze.faturamento RESTART IDENTITY CASCADE;
TRUNCATE TABLE bronze.usuarios RESTART IDENTITY CASCADE;

-- 3. NÃO truncar dim_data (tabela de dimensão permanente)

COMMIT;
"""

def truncate_tables():
    """
    Executa o truncate de todas as tabelas Bronze e de auditoria.
    """
    print("🧹 Iniciando limpeza das tabelas Bronze...")
    
    try:
        with get_connection() as conn:
            with get_cursor(conn) as cur:
                # Executa o SQL de truncate
                cur.execute(TRUNCATE_SQL)
                
        print("✅ Tabelas truncadas com sucesso!")
        print("   - bronze.base_oficial")
        print("   - bronze.faturamento")
        print("   - bronze.usuarios")
        print("   - auditoria.historico_execucao")
        print("   - auditoria.log_rejeicao")
        print("   ⚠️  dim_data mantida (não foi truncada)")
        
    except Exception as e:
        print(f"❌ Erro ao truncar tabelas: {e}")
        raise

if __name__ == "__main__":
    truncate_tables()
