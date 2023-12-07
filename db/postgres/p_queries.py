import os
import logging
from dotenv import load_dotenv
import psycopg2

load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def try_connection():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER,
                            password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    logging.info("SUCCESSFUL CONNECTION")
    conn.close()


if __name__ == "__main__":
    try_connection()
