import psycopg2
import os

DB_NAME = os.environ.get('PG_DB', 'botdb')
DB_USER = os.environ.get('PG_USER', 'postgres')
DB_PASS = os.environ.get('PG_PASS', 'user')
DB_HOST = os.environ.get('PG_HOST', 'localhost')
DB_PORT = os.environ.get('PG_PORT', '5432')
TARGET_DB_USER = 'botuser'
TARGET_DB_NAME = 'botdb'

def drop_db_and_user():
    conn = psycopg2.connect(dbname='postgres', user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    conn.autocommit = True
    cur = conn.cursor()
    # Отключаем пользователей от базы
    cur.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s", (TARGET_DB_NAME,))
    # Удаляем базу, если есть
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname=%s", (TARGET_DB_NAME,))
    if cur.fetchone():
        cur.execute(f"DROP DATABASE {TARGET_DB_NAME}")
        print(f"Database {TARGET_DB_NAME} dropped.")
    # Удаляем пользователя, если есть
    cur.execute(f"SELECT 1 FROM pg_roles WHERE rolname=%s", (TARGET_DB_USER,))
    if cur.fetchone():
        cur.execute(f"DROP USER {TARGET_DB_USER}")
        print(f"User {TARGET_DB_USER} dropped.")
    cur.close()
    conn.close()
    print('All data, user, and db dropped.')

if __name__ == '__main__':
    drop_db_and_user() 