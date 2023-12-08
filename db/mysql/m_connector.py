import os
import logging
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s - %(levelname)s - %(message)s')


def mysql_connector(func):
    def inner(*args, **kwargs):
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        logging.info("MYSQL CONNECTED")
        try:
            return func(connection, *args, **kwargs)
        except Exception as ex:
            logging.error(ex)
        finally:
            connection.close()
            logging.info("MYSQL DISCONNECTED")

    return inner


@mysql_connector
def mysql_demo_query(connector):
    curr = connector.cursor()
    curr.execute("SELECT * FROM main.demo")
    data = curr.fetchall()
    curr.close()
    logging.debug(data)
    return data


if __name__ == "__main__":
    mysql_demo_query()
