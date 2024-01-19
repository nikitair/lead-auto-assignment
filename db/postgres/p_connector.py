import os
from logging_config import logger as  logging
import pretty_errors
from dotenv import load_dotenv
import psycopg2
from sshtunnel import SSHTunnelForwarder

load_dotenv()

SSH_MODE = int(os.getenv("SSH_MODE"))
SSH_PKEY = os.getenv("SSH_PKEY")

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

SSH_USERNAME = os.getenv("SSH_POSTGRES_USERNAME")
SSH_SERVER_ADDRESS=os.getenv("SSH_POSTGRES_SERVER_ADDRESS")
SSH_SERVER_PORT = int(os.getenv("SSH_POSTGRES_SERVER_PORT"))
LOCAL_PORT = os.getenv("POSTGRES_LOCAL_PORT")


def postgres_connector(func):
    """
    decorator that connects to Postgres DB and executes inputted query handler; handles exceptions
    """
    def inner(*args, **kwargs):

        if SSH_MODE == 1:
            # starting SSH tunnel
            server = SSHTunnelForwarder(
                (SSH_SERVER_ADDRESS, SSH_SERVER_PORT),
                ssh_username=SSH_USERNAME,
                ssh_pkey=SSH_PKEY,
                remote_bind_address=(POSTGRES_HOST, int(POSTGRES_PORT)),
                local_bind_address=("localhost", int(LOCAL_PORT))
            )
            server.start()
            logging.info("POSTGRES SSH TUNNEL STARTED")

            conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER,
                                    password=POSTGRES_PASSWORD, host="localhost", port=LOCAL_PORT)
        else:
             conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER,
                                    password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)   
        

        logging.info(f"POSTGRES CONNECTED WITH SSH MODE {SSH_MODE}")
        try:
            return func(conn, *args, **kwargs)
        except Exception as ex:
            logging.error(f"!!! POSTGRES ERROR -- {ex}")
        finally:
            conn.close()
            logging.info("POSTGRES DISCONNECTED")
            if SSH_MODE == 1:
                server.stop()
                logging.info("POSTGRES SSH TUNNEL DISCONNECTED")

    return inner


# demo postgres_connector decorator use case
@postgres_connector
def postgres_demo_query(connector):
    curr = connector.cursor()
    curr.execute("SELECT * FROM demo")
    data = curr.fetchall()
    curr.close()
    logging.debug(data)
    return data


if __name__ == "__main__":
    ...
