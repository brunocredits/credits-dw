from python.utils.db_connection import get_db_connection

def check_users():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            print("--- Database Roles ---")
            cur.execute("SELECT rolname FROM pg_roles WHERE rolname NOT LIKE 'pg_%' AND rolname NOT LIKE 'azure_%'")
            rows = cur.fetchall()
            for row in rows:
                print(row[0])
    except Exception as e:
        print(f"Error checking users: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_users()
