import os
import pretty_errors
from logging_config import logger as logging
from dotenv import load_dotenv
import mysql.connector
from sshtunnel import SSHTunnelForwarder

# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

SSH_MODE = int(os.getenv("SSH_MODE"))
SSH_PKEY = os.getenv("SSH_PKEY")

MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

SSH_USERNAME = os.getenv("SSH_MYSQL_USERNAME")
SSH_SERVER_ADDRESS=os.getenv("SSH_MYSQL_SERVER_ADDRESS")
SSH_SERVER_PORT = int(os.getenv("SSH_MYSQL_SERVER_PORT"))
LOCAL_PORT = os.getenv("MYSQL_LOCAL_PORT")


def mysql_connector(func):
    """
    decorator that connects to MySQL DB and executes inputted query handler; handles exceptions
    """
    def inner(*args, **kwargs):
        logging.info(f"CONNECTING TO MYSQL WITH SSH MODE - {SSH_MODE}")
        if SSH_MODE == 1:
            # starting SSH tunnelling
            server = SSHTunnelForwarder(
                (SSH_SERVER_ADDRESS, SSH_SERVER_PORT),
                ssh_username=SSH_USERNAME,
                ssh_pkey=SSH_PKEY,
                remote_bind_address=(MYSQL_HOST, int(MYSQL_PORT)),
                local_bind_address=("localhost", int(LOCAL_PORT))
            )
            server.start()
            logging.info("MYSQL SSH TUNNEL STARTED")

            connection = mysql.connector.connect(
                host="localhost",
                port=LOCAL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DB
            )
        else:
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
            logging.error(f"!!! MYSQL ERROR -- {ex}")
        finally:
            connection.close()
            logging.info("MYSQL DISCONNECTED")
            if SSH_MODE == 1:
                server.stop()
                logging.info("MYSQL SSH TUNNEL DISCONNECTED")
    return inner


# mysql_connector demo use case
@mysql_connector
def mysql_demo_query(connector):
    curr = connector.cursor()
    curr.execute("SELECT * FROM tbl_customers LIMIT 1")
    data = curr.fetchall()
    curr.close()
    logging.debug(data)
    return data


if __name__ == "__main__":
    mysql_demo_query()
