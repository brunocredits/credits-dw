import psycopg2
from python.utils.db_connection import get_db_connection

def setup_roles_and_users():
    conn = get_db_connection()
    conn.autocommit = True
    
    users = [
        'bruno_cavalcante', 
        'maria_rodrigues', 
        'joao_viveiros', 
        'crislaine_cardoso'
    ]
    default_pass = 'Credits@2025' # Senha padrão solicitada

    try:
        with conn.cursor() as cur:
            print("--- Configurando Roles e Permissões ---")

            # 1. Criar Role de Grupo (dw_developer)
            print("1. Configurando grupo 'dw_developer'...")
            try:
                cur.execute("CREATE ROLE dw_developer NOLOGIN;")
                print("   -> Role 'dw_developer' criada.")
            except psycopg2.errors.DuplicateObject:
                print("   -> Role 'dw_developer' já existe.")

            # 2. Conceder Permissões ao Grupo
            print("2. Concedendo permissões no schema 'bronze' e 'auditoria'...")
            
            # Schema Bronze
            cur.execute("GRANT USAGE ON SCHEMA bronze TO dw_developer;")
            cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bronze TO dw_developer;")
            cur.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO dw_developer;")
            
            # Schema Auditoria (Permitir ver logs e inserir se necessário)
            cur.execute("GRANT USAGE ON SCHEMA auditoria TO dw_developer;")
            cur.execute("GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA auditoria TO dw_developer;")
            cur.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA auditoria TO dw_developer;")

            # Default Privileges (Para garantir acesso a futuras tabelas)
            cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO dw_developer;")

            # 3. Criar Usuários e Adicionar ao Grupo
            print("3. Criando/Atualizando usuários...")
            for user in users:
                try:
                    cur.execute(f"CREATE USER {user} WITH PASSWORD '{default_pass}';")
                    print(f"   -> Usuário '{user}' criado.")
                except psycopg2.errors.DuplicateObject:
                    print(f"   -> Usuário '{user}' já existe. Atualizando senha.")
                    cur.execute(f"ALTER USER {user} WITH PASSWORD '{default_pass}';")
                
                # Adicionar ao grupo
                cur.execute(f"GRANT dw_developer TO {user};")
                print(f"   -> '{user}' adicionado ao grupo 'dw_developer'.")
                
                # Garantir permissão de conexão
                cur.execute(f"GRANT CONNECT ON DATABASE creditsdw TO {user};")

            print("\n✅ Configuração de acessos concluída com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao configurar acessos: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    setup_roles_and_users()
