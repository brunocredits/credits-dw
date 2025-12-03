"""
Script para documentar a estrutura final da camada bronze.
"""

import sys
from pathlib import Path
from dotenv import load_dotenv

# Carregar variáveis de ambiente
project_root = Path(__file__).resolve().parents[2]
load_dotenv(project_root / '.env')

sys.path.insert(0, str(project_root))

from python.utils.db_connection import get_connection, get_dict_cursor


def document_bronze_structure():
    """Documenta a estrutura final da camada bronze."""
    
    print('=' * 80)
    print('ESTRUTURA FINAL DA CAMADA BRONZE')
    print('=' * 80)
    print()
    
    with get_connection() as conn:
        with get_dict_cursor(conn) as cursor:
            # Listar tabelas
            cursor.execute('''
                SELECT 
                    table_name,
                    (SELECT COUNT(*) FROM information_schema.columns 
                     WHERE table_schema = 'bronze' AND table_name = t.table_name) as num_columns
                FROM information_schema.tables t
                WHERE table_schema = 'bronze' AND table_type = 'BASE TABLE'
                ORDER BY table_name
            ''')
            tables = cursor.fetchall()
            
            print('📊 TABELAS:')
            print('-' * 80)
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) as total FROM bronze.{table['table_name']}")
                count = cursor.fetchone()['total']
                print(f"  ✓ {table['table_name']:<20} | {table['num_columns']:>2} colunas | {count:>8,} registros")
            
            print()
            
            # Listar índices
            cursor.execute('''
                SELECT 
                    schemaname,
                    relname as tablename,
                    indexrelname as indexname,
                    pg_size_pretty(pg_relation_size(indexrelid)) as size
                FROM pg_stat_user_indexes
                WHERE schemaname = 'bronze'
                ORDER BY relname, indexrelname
            ''')
            indexes = cursor.fetchall()
            
            print('🔍 ÍNDICES:')
            print('-' * 80)
            current_table = None
            total_indexes = 0
            for idx in indexes:
                if idx['tablename'] != current_table:
                    if current_table is not None:
                        print()
                    current_table = idx['tablename']
                    print(f"  {idx['tablename']}:")
                print(f"    ✓ {idx['indexname']:<45} {idx['size']:>8}")
                total_indexes += 1
            
            print()
            print(f"  Total: {total_indexes} índices")
            print()
            
            # Listar views
            cursor.execute('''
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = 'bronze'
                ORDER BY table_name
            ''')
            views = cursor.fetchall()
            
            print('📋 VIEWS:')
            print('-' * 80)
            if views:
                for view in views:
                    print(f"  ✓ {view['table_name']}")
            else:
                print('  (nenhuma view)')
            
            print()
            print('=' * 80)
            print('DOCUMENTAÇÃO CONCLUÍDA')
            print('=' * 80)


if __name__ == '__main__':
    try:
        document_bronze_structure()
    except Exception as e:
        print(f'\n❌ Erro: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
