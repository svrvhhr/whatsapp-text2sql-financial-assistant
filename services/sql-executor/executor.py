import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

def execute_sql(sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        if cur.description:
            return cur.fetchall()
        conn.commit()
        return "OK"

if __name__ == "__main__":
    print("SQL Executor ready")
