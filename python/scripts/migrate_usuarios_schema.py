"""
Migration script to fix bronze.usuarios table schema - Alternative approach.

This script updates the data types of access columns from numeric to text (VARCHAR)
to properly store email addresses instead of numeric values.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from python.utils.db_connection import get_connection, get_cursor

def migrate_usuarios_schema():
    """
    Apply schema migration to bronze.usuarios table
    """
    print("=" * 80)
    print("MIGRATING BRONZE.USUARIOS SCHEMA")
    print("=" * 80)
    print("\nChanges to apply:")
    print("  - acesso_vendedor: numeric → character varying")
    print("  - acesso_gerente: numeric → character varying")
    print("  - acesso_indireto: numeric → character varying")
    print("\n" + "=" * 80)
    
    try:
        with get_connection() as conn:
            conn.autocommit = False
            with get_cursor(conn) as cur:
                # Execute each ALTER statement separately
                print("\n1. Altering acesso_vendedor...")
                cur.execute("""
                    ALTER TABLE bronze.usuarios 
                    ALTER COLUMN acesso_vendedor TYPE character varying 
                    USING acesso_vendedor::text;
                """)
                
                print("2. Altering acesso_gerente...")
                cur.execute("""
                    ALTER TABLE bronze.usuarios 
                    ALTER COLUMN acesso_gerente TYPE character varying 
                    USING acesso_gerente::text;
                """)
                
                print("3. Altering acesso_indireto...")
                cur.execute("""
                    ALTER TABLE bronze.usuarios 
                    ALTER COLUMN acesso_indireto TYPE character varying 
                    USING acesso_indireto::text;
                """)
                
                conn.commit()
                
        print("\n✅ Migration completed successfully!")
        print("\nThe bronze.usuarios table now accepts text values (email addresses)")
        print("for acesso_vendedor, acesso_gerente, and acesso_indireto columns.")
        print("=" * 80)
        
        # Verify the changes
        print("\nVerifying schema changes...")
        with get_connection() as conn:
            with get_cursor(conn) as cur:
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'bronze' 
                    AND table_name = 'usuarios'
                    AND column_name IN ('acesso_vendedor', 'acesso_gerente', 'acesso_indireto')
                    ORDER BY column_name;
                """)
                
                print("\nUpdated column types:")
                for row in cur.fetchall():
                    print(f"  - {row[0]}: {row[1]}")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        print("\nPossible solutions:")
        print("1. Connect with a database user that has ALTER TABLE permissions")
        print("2. Ask your DBA to run the migration SQL")
        print("3. Use a superuser account")
        raise

if __name__ == "__main__":
    migrate_usuarios_schema()
