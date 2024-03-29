import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import OperationalError
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# Параметры подключения к базе данных
conn_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': '127.0.0.1',
    'port': '5432'
}

def create_conn():
        conn = None
        try:
            conn = psycopg2.connect(**conn_params)
        except OperationalError as e:
            pass
        return conn
    

def create_tables():
    """Создаем структуру таблиц."""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS tail_dictionary (
            id SERIAL PRIMARY KEY,
            type VARCHAR(50), 
            class VARCHAR(50), 
            service VARCHAR(50), 
            description_rus VARCHAR(255),
            description_eng VARCHAR(255), 
            location VARCHAR(3)
        )
        """,
    )
    conn = None
    try:
        conn = create_conn()
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_tails_dict(tails_dict):
    conn = create_conn()
    cur = conn.cursor()
    for row in tails_dict.itertuples(index=False):
        values = []
        for value in row:
            if isinstance(value, list):
                value = [v if not pd.isna(v) else None for v in value]
            else:
                value = None if pd.isna(value) else value
            values.append(value)
        
        insert_query = "INSERT INTO tail_dictionary (" + ", ".join(tails_dict.columns) + ") VALUES (" + ", ".join(["%s"] * len(tails_dict.columns)) + ")"
        try:
            cur.execute(insert_query, values)
            conn.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")
            conn.rollback()


create_tables()
tails_dict = pd.read_excel('tails_dictionary.xlsx')
insert_tails_dict(tails_dict)