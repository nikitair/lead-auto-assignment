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


def postgres_connector(func):
    def inner(*args, **kwargs):
        conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER,
                                password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
        logging.info("POSTGRES CONNECTED")
        try:
            return func(conn, *args, **kwargs)
        except Exception as ex:
            logging.error(ex)
        finally:
            conn.close()
            logging.info("POSTGRES DISCONNECTED")

    return inner


@postgres_connector
def postgres_demo_query(connector):
    curr = connector.cursor()
    curr.execute("SELECT * FROM demo")
    data = curr.fetchall()
    curr.close()
    logging.info(data)
    return data


if __name__ == "__main__":
    postgres_demo_query()
