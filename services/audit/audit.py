import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

def log_action(user_id: int, role: str, query: str, success: bool):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS audit_log (id SERIAL PRIMARY KEY, utilisateur_id INT, role VARCHAR(50), query TEXT, success BOOLEAN, date TIMESTAMP DEFAULT NOW());"
        )
        cur.execute(
            "INSERT INTO audit_log (utilisateur_id, role, query, success) VALUES (%s,%s,%s,%s);",
            (user_id, role, query, success)
        )
        conn.commit()

if __name__ == "__main__":
    print("Audit Service ready")
