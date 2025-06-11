import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import asyncio
from src.db import Database
from src.config import Config

DB_NAME = os.environ.get('PG_DB', 'botdb')
DB_USER = os.environ.get('PG_USER', 'botuser')
DB_PASS = os.environ.get('PG_PASS', 'botpass')
DB_HOST = os.environ.get('PG_HOST', 'localhost')
DB_PORT = os.environ.get('PG_PORT', '5432')

SQL_FILE = 'init_local_pg.sql'

def create_db_and_user():
    conn = psycopg2.connect(dbname='postgres', user='postgres', host=DB_HOST, port=DB_PORT)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_roles WHERE rolname=%s", (DB_USER,))
    if not cur.fetchone():
        cur.execute(f"CREATE USER {DB_USER} WITH PASSWORD %s", (DB_PASS,))
        print(f"User {DB_USER} created.")
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname=%s", (DB_NAME,))
    if not cur.fetchone():
        cur.execute(f"CREATE DATABASE {DB_NAME} OWNER {DB_USER}")
        print(f"Database {DB_NAME} created.")
    cur.close()
    conn.close()

def run_sql_file():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()
    with open(SQL_FILE, 'r', encoding='utf-8') as f:
        sql = f.read()
    # psycopg2 не поддерживает выполнение нескольких команд сразу, разбиваем по ';'
    for statement in sql.split(';'):
        stmt = statement.strip()
        if stmt:
            cur.execute(stmt)
    conn.commit()
    cur.close()
    conn.close()
    print("Schema initialized from SQL file.")

async def init_db():
    # Загрузка конфигурации
    config = Config.from_yaml('config.yaml')
    # Инициализация и подключение к БД
    db = Database(config)
    await db.connect()
    await db.create_tables()  # Создаем таблицы с новыми полями

if __name__ == '__main__':
    create_db_and_user()
    run_sql_file()
    print("Done.")
    asyncio.run(init_db()) 